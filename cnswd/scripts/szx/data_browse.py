"""

深证信数据搜索

"""
import math
import os
import time

import logbook
import pandas as pd
from cnswd.constants import MARKET_START
from cnswd.data_proxy import DataProxy
from cnswd.sql.base import get_engine, get_session, session_scope
from cnswd.sql.szx import (IPO, Classification, ClassificationBom, Quote,
                           StockInfo)
from cnswd.utils import ensure_list, loop_codes
from cnswd.websource import DataBrowser
from numpy.random import shuffle
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from ..runner import TryToCompleted
from ..utils import create_tables
from .base import (DATE_MAPS, MODEL_MAPS, get_all_stock, get_bank_stock,
                   get_quote_start_date, get_start_date, has_data)
from .sql import write_to_sql

logger = logbook.Logger('深证信')
db_dir_name = 'szx'
START_CHECK = (pd.Timestamp('today') - pd.Timedelta(days=30)).date()
B_2007_LEVELS = ('8.3.4', '8.3.5', '8.3.6')
DELAYS = {'3.1': 7}
START_MAPS = {'3.1': MARKET_START.tz_localize(None)}


def default_ordered_levels():
    """默认项目排序"""
    y = ['6.1']
    s_e = ['2.1', '2.2', '2.3', '2.4', '3.1',
           '4.1'] + ['7.1', '7.2', '7.3', '7.4']
    y_q = ['2.5', '5.1'] + [x for x in MODEL_MAPS.keys() if x.startswith('8')
                            and (x not in B_2007_LEVELS)]
    return y + s_e + y_q


def delete_data_of(class_):
    """删除表数据"""
    session = get_session(db_dir_name)
    num = session.query(class_).delete(False)
    logger.notice(f"删除 表:{class_.__tablename__} {num}行")
    session.commit()
    session.close()


def batch_get_data(codes, data_item, start=None, end=None):
    """分批获取项目数据"""
    with DataBrowser() as api:
        return api.get_data(data_item, codes, start, end)


def _insert_info(session, class_, s, cols, code):
    logger.info(f"插入表：{class_.__tablename__} 股票代码：{code}")
    obj = class_()
    obj.股票代码 = code
    for col in cols:
        v = s[col]
        if pd.isna(v) or pd.isnull(v):
            continue
        setattr(obj, col, v)
    session.add(obj)
    session.commit()


def _update_info(session, obj, s, cols, code):
    logger.info(f"更新表：{obj.__tablename__} 股票代码：{code}")
    for col in cols:
        v = s[col]
        if pd.isna(v) or pd.isnull(v):
            continue
        setattr(obj, col, v)
    session.commit()


def _update_or_insert_info(df, level):
    if df.empty:
        return
    engine = get_engine(db_dir_name)
    session = get_session(db_dir_name)
    df = write_to_sql(engine, df, level, save=False)
    cols = df.columns
    class_ = MODEL_MAPS[level]
    for code, s in df.iterrows():
        obj = session.query(class_).filter(class_.股票代码 == code).one_or_none()
        if obj is None:
            _insert_info(session, class_, s, cols, code)
        else:
            if level != '7.5':
                _update_info(session, obj, s, cols, code)
    session.close()


def update_stock_info(rewrite=False):
    """刷新股票基本信息(含IPO)"""
    if rewrite:
        delete_data_of(StockInfo)
    done = {}
    levels = ('1.1', '7.5')
    for _ in range(3):
        all_ok = all([done.get(l, False) for l in levels])
        if all_ok:
            break
        with DataBrowser(True) as api:
            for level in levels:
                ok = done.get(level, False)
                if ok:
                    continue
                try:
                    df = api.query(level)
                    _update_or_insert_info(df, level)
                    done[level] = True
                except Exception as e:
                    api.logger.notice(f"{e!r}")
            time.sleep(1)


def _get_q_start_date(level, delay):
    """财务报告类数据开始刷新时间"""
    session = get_session(db_dir_name)
    class_ = MODEL_MAPS[level]
    t_end_date = session.query(func.max(class_.报告年度)).scalar()
    if t_end_date is None:
        return pd.Timestamp('2006-01-01')
    else:
        # 可能存在近期发布的数据
        return pd.Timestamp('today') - pd.Timedelta(days=delay)


def _f_part_insert(df, level):
    session = get_session(db_dir_name)
    class_ = MODEL_MAPS[level]
    table = class_.__tablename__
    cols = df.columns
    for code, s in df.iterrows():
        obj = class_()
        d = s['报告年度'].strftime(r'%Y-%m-%d')
        obj.股票代码 = code
        for col in cols:
            v = s[col]
            if pd.isna(v) or pd.isnull(v):
                continue
            setattr(obj, col, v)
        session.add(obj)
        try:
            session.commit()
            logger.info(f"插入表：{table} 股票代码：{code} 报告年度：{d}")
        except IntegrityError:
            session.rollback()
    session.close()


def refresh_bank_data():
    """刷新金融业专项财务数据"""
    engine = get_engine(db_dir_name)
    delay = 135
    # 注意，尽管使用全部代码，但银行专项只有金融业股票才有数据
    with DataBrowser(True) as api:
        for level in B_2007_LEVELS:
            start = _get_q_start_date(level, delay)
            df = api.get_data(level, start=start)
            df = write_to_sql(engine, df, level, save=False)
            if not df.empty:
                _f_part_insert(df, level)


def delete_incomplete_quotes():
    """删除不完整的日线数据"""
    session = get_session(db_dir_name)
    start = session.query(
        func.min(Quote.交易日期)
    ).filter(
        Quote.股票代码 == '000001',
    ).filter(
        func.date(Quote.交易日期) >= START_CHECK,
    ).filter(
        Quote.本日融资余额.is_(None),
    ).one_or_none()
    table = Quote.__tablename__
    # 如果存在，证明数据不完整，删除改日之后的数据
    # 限定融资融券类的股票
    if start[0] is not None:
        margin_codes = session.query(
            Quote.股票代码.distinct()
        ).filter(
            Quote.本日融资余额.isnot(None)
        ).all()
        for code in margin_codes:
            rows = session.query(Quote).filter(func.date(Quote.交易日期) >= start[0].date(),
                                               Quote.股票代码 == code[0]).delete(False)
            session.commit()
            logger.notice(f'删除 {table} 代码：{code[0]} 不完整数据 {rows} 行')
    session.close()


def _select_rows_from(level, df, start):
    """从原始数据中选择自start开始的行"""
    if df.empty:
        return df
    try:
        date_col = DATE_MAPS[level][2]
    except IndexError:
        date_col = DATE_MAPS[level][0]
    cond = df[date_col] >= start.strftime(r'%Y-%m-%d')
    return df.loc[cond, :]


def _insert(df, level):
    session = get_session(db_dir_name)
    class_ = MODEL_MAPS[level]
    table = class_.__tablename__
    cols = df.columns
    d_name = DATE_MAPS[level]
    for code, s in df.iterrows():
        obj = class_()
        d = s[d_name].strftime(r'%Y-%m-%d')
        obj.股票代码 = code
        for col in cols:
            v = s[col]
            if pd.isna(v) or pd.isnull(v):
                continue
            setattr(obj, col, v)
        session.add(obj)
        try:
            session.commit()
            logger.info(f"插入表：{table} 股票代码：{code} 日期：{d}")
        except IntegrityError:
            session.rollback()
    session.close()


def _get_d_start_date(level):
    """日线数据开始刷新时间"""
    session = get_session(db_dir_name)
    class_ = MODEL_MAPS[level]
    delay = DELAYS[level]
    if level in ('3.1',):
        expr = class_.交易日期
    t_end_date = session.query(func.max(expr)).scalar()
    if t_end_date is None:
        return START_MAPS[level]
    else:
        # 自前溯delay天开始
        return t_end_date - pd.Timedelta(days=delay)


def _refresh_stock_data(level):
    """刷新股票数据"""
    engine = get_engine(db_dir_name)
    with DataBrowser(True) as api:
        # 使暂停上市、退市股票开始日期无效
        start = _get_d_start_date(level)
        df = api.get_data(level, start=start)
        if level == '4.1' and (not df.empty):
            # 去掉研究员为空的记录(年份久远的数据可能存在，不影响)
            df = df.loc[df['研究员名称'].str.len() > 0, :]
        df = write_to_sql(engine, df, level, save=False)
        _insert(df, level)


def valid_level(levels):
    """验证输入项目是否有效"""
    v_ls = [l for l in MODEL_MAPS.keys() if l not in (
            B_2007_LEVELS + ('1.1', '7.5'))]
    for l in levels:
        if l not in v_ls:
            raise ValueError(f"刷新szx股票信息，{l}不是有效项目")


def refresh_stock_data(levels):
    """刷新股票数据"""
    if levels is None:
        # 默认除金融项目、股票基本资料、IPO外的所有项目
        levels = default_ordered_levels()
    else:
        levels = ensure_list(levels)
    valid_level(levels)
    if '3.1' in levels:
        delete_incomplete_quotes()
    for level in levels:
        _refresh_stock_data(level)


def daily_refresh():
    """每日刷新数据"""
    update_stock_info(False)
    refresh_stock_data('3.1', 30)


def weekly_refresh():
    """每周刷新数据"""
    update_stock_classify()
    levels = ['2.1', '2.2', '2.3', '2.4', '4.1',
              '5.1', '7.1', '7.2', '7.3', '7.4']
    refresh_stock_data(levels, 30)


def monthly_refresh():
    """每月刷新数据"""
    update_stock_info(True)
    update_stock_classify()


def quarterly_refresh():
    """每季刷新数据"""
    levels = ['2.5', '5.1', '8.1.1', '8.1.2', '8.2.1',
              '8.2.2', '8.2.3', '8.3.1', '8.3.2', '8.3.3',
              '8.4.1', '8.4.2']
    refresh_stock_data(levels, 30)


def update_stock_classify():
    """更新股票分类信息"""
    table = Classification.__tablename__
    api = DataBrowser(True)
    levels = []
    for nth in (1, 2, 3, 4, 5, 6):
        levels.extend(api.get_levels_for(nth))
    # CDR当前不可用
    levels = [x for x in levels if x not in (
        '6.6', '6.9') and api._is_end_level(x)]
    classify_proxy = DataProxy(api.get_classify_stock, freq='D')
    done = []
    dfs = []
    for i in range(10):
        to_do = set(levels) - set(done)
        if len(to_do) == 0:
            break
        api.logger.notice(f'第{i+1}次尝试，剩余分类层级数量:{len(to_do)}')
        for level in sorted(to_do):
            try:
                df = classify_proxy.read(level=level)
                dfs.append(df)
                done.append(level)
            except Exception:
                pass
        time.sleep(1)

    api.driver.quit()
    # 完整下载后，才删除旧数据
    delete_data_of(Classification)

    for df in dfs:
        if not df.empty:
            df.to_sql(table, get_engine(db_dir_name),
                      if_exists='append', index=False)
            api.logger.info(
                f'表：{table} 添加{len(df):>4}行')


def update_classify_bom():
    table = ClassificationBom.__tablename__
    engine = get_engine(db_dir_name)
    with DataBrowser() as api:
        bom = api.classify_bom
        bom.to_sql(table, engine, if_exists='replace')
        api.logger.info(f'表：{table} 更新 {len(bom):>4}行')


def init_szx():
    """初始化深证信数据库"""
    create_tables(db_dir_name, False)
    update_stock_info(True)
    update_stock_classify()
    refresh_bank_data()
    # 除股票基本资料、IPO及金融行业外的数据项目
    levels = [x for x in DATE_MAPS.keys() if x not in B_2007_LEVELS]
    refresh_stock_data(levels, times=30)
