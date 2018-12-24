"""

深证信数据搜索

备注
    考虑到后台任务可能在某个时点重叠，使用本机cpu一半作为最大数量
"""

import math
import os
import time

import logbook
import pandas as pd
from numpy.random import shuffle
from sqlalchemy import func

from cnswd.sql.base import get_engine, get_session, session_scope
from cnswd.sql.szx import (IPO, Classification, ClassificationBom, Quote,
                           StockInfo)
from cnswd.utils import ensure_list, loop_codes
from cnswd.websource import DataBrowser

from ..runner import TryToCompleted
from ..utils import create_tables
from .base import (DATE_MAPS, MODEL_MAPS, get_all_stock, get_bank_stock,
                   get_quote_start_date, get_start_date, has_data)
from .sql import write_to_sql

logger = logbook.Logger('深证信')
db_dir_name = 'szx'
START_CHECK = (pd.Timestamp('today') - pd.Timedelta(days=30)).date()
B_2007_LEVELS = ('8.3.4', '8.3.5', '8.3.6')


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


def _batch_insert_stock_info(codes):
    levels = ('1.1', '7.5')
    engine = get_engine(db_dir_name)
    session = get_session(db_dir_name)
    with DataBrowser(True) as api:
        for code in codes:
            for level in levels:
                # IPO信息不会发生变动
                if level == '7.5':
                    if has_data(session, code, level):
                        continue
                df = api.get_data(level, code)
                write_to_sql(engine, df, level)


def update_stock_info(rewrite=False):
    """刷新股票基本信息(含IPO)"""
    with DataBrowser(True) as api:
        codes = api.get_stock_code()
    if rewrite:
        delete_data_of(StockInfo)
    runner = TryToCompleted(_batch_insert_stock_info, codes)
    runner.run()


def batch_refresh_bank_data(codes):
    """刷新金融业专项财务数据"""
    session = get_session(db_dir_name)
    engine = get_engine(db_dir_name)
    with DataBrowser(True) as api:
        for code in codes:
            for level in B_2007_LEVELS:
                start = get_start_date(session, level, code)
                if start > pd.Timestamp('today').date():
                    continue
                df = api.get_data(level, code, start)
                if not df.empty:
                    write_to_sql(engine, df, level)
    session.close()


def refresh_bank_data(times=5):
    """刷新金融业专项财务数据"""
    session = get_session(db_dir_name)
    codes = get_bank_stock(session)
    session.close()
    runner = TryToCompleted(batch_refresh_bank_data, codes, retry_times=times)
    runner.run()


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


def batch_refresh_stock_data(codes, level, dates):
    """分批刷新股票数据"""
    engine = get_engine(db_dir_name)
    # 不可混淆api内部尝试次数。内部尝试次数默认为3次
    # 整体尝试次数可以设置很大的数。一旦记录的再次尝试代码为空，即退出循环
    with DataBrowser(True) as api:
        for code in codes:
            # 使暂停上市、退市股票开始日期无效
            if level == '3.1':
                start = dates.at[code, '开始日期'].date()
            else:
                with session_scope(db_dir_name) as session:
                    start = get_start_date(session, level, code)
            if start > pd.Timestamp('today').date():
                continue
            df = api.get_data(level, code, start)
            # 选择自开始日期起的行
            df = _select_rows_from(level, df, start)
            if level == '4.1' and (not df.empty):
                # 去掉研究员为空的记录(年份久远的数据可能存在，不影响)
                df = df.loc[df['研究员名称'].str.len() > 0, :]
            write_to_sql(engine, df, level)


def valid_level(levels):
    """验证输入项目是否有效"""
    v_ls = [l for l in MODEL_MAPS.keys() if l not in (
            B_2007_LEVELS + ('1.1', '7.5'))]
    for l in levels:
        if l not in v_ls:
            raise ValueError(f"刷新szx股票信息，{l}不是有效项目")


def refresh_stock_data(levels, times):
    """刷新股票数据"""
    if levels is None:
        # 默认除金融项目、股票基本资料、IPO外的所有项目
        levels = default_ordered_levels()
    else:
        levels = ensure_list(levels)
    valid_level(levels)
    if '3.1' in levels:
        delete_incomplete_quotes()
    with session_scope(db_dir_name) as session:
        all_codes = get_all_stock(session)
        # 尽量平衡各cpu负载，提高并行效率
        shuffle(all_codes)
    for level in levels:
        if level == '3.1':
            dates = get_quote_start_date()
        else:
            dates = None
        kws = {'level': level, 'dates':dates}
        runner = TryToCompleted(batch_refresh_stock_data,
                                all_codes, kws, retry_times=times)
        runner.run()


def daily_refresh():
    """每日刷新数据"""
    update_stock_info(False)
    refresh_stock_data('3.1', 30)


def weekly_refresh():
    """每周刷新数据"""
    update_stock_classify(rewrite=False, times=1)
    levels = ['2.1', '2.2', '2.3', '2.4', '4.1',
              '5.1', '7.1', '7.2', '7.3', '7.4']
    refresh_stock_data(levels, 30)


def monthly_refresh():
    """每月刷新数据"""
    update_stock_info(True)
    update_stock_classify(rewrite=True, times=10)


def quarterly_refresh():
    """每季刷新数据"""
    levels = ['2.5', '5.1', '8.1.1', '8.1.2', '8.2.1',
              '8.2.2', '8.2.3', '8.3.1', '8.3.2', '8.3.3',
              '8.4.1', '8.4.2']
    refresh_stock_data(levels, 30)


def batch_stock_classify(levels, times):
    """分批获取层级分类股票列表"""
    # 以下情形判断为完成状态
    # 1. 如果数据库已有层级数据
    # 2. 上次提取无异常，尽管无数据
    progress = {}  # 记录level提取状态，防止重复提取
    table = Classification.__tablename__
    with DataBrowser(True) as api:
        for i in range(times):
            with session_scope(db_dir_name) as sess:
                db_levels = sess.query(Classification.分类层级).distinct()
            to_do = set(levels) - set([x[0] for x in db_levels])
            if len(to_do) == 0:
                break
            api.logger.notice(f'第{i+1}次尝试，提取{len(to_do)}个分类层级......')
            for level in to_do:
                if progress.get(level):
                    api.logger.info(f'分类：{level} 数据已完成，跳过')
                    continue
                try:
                    df = api.get_classify_stock(level)
                    if not df.empty:
                        df.to_sql(table, get_engine(db_dir_name),
                                  if_exists='append', index=False)
                        api.logger.info(
                            f'表：{table} 分类：{level} 已添加{len(df):>4}行')
                    else:
                        api.logger.info(f'分类：{level} 无数据')
                    progress[level] = True
                except Exception as e:
                    progress[level] = False
                    api.logger.error(f'{level} {e!r}')
            time.sleep(1)
    # 报告尚未完成的分类层级
    for k, v in progress.items():
        if not v:
            print(f'{k} 尚未完成')
    time.sleep(1)


def update_stock_classify(rewrite, times):
    """更新股票分类信息
    
    Arguments:
        rewrite {bool} -- 是否重写数据。如重写，首先删除表中数据
        times {int} -- 重试次数
    """
    if rewrite:
        delete_data_of(Classification)
    with DataBrowser() as api:
        # 防止冲突，仅分六组
        p_levels = []
        for nth in (1, 2, 3, 4, 5, 6):
            levels = api.get_levels_for(nth)
            if nth == 6:
                # CDR当前不可用
                levels.remove('6.6')
                levels.remove('6.9')
            p_levels.append(levels)
    runner = TryToCompleted(batch_stock_classify, levels, retry_times=times)
    runner.run()


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
    update_stock_classify(rewrite=True, times=10)
    refresh_bank_data()
    # 除股票基本资料、IPO及金融行业外的数据项目
    levels = [x for x in DATE_MAPS.keys() if x not in B_2007_LEVELS]
    refresh_stock_data(levels, times=30)
