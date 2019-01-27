"""

深证信数据搜索刷新脚本

备注：
    1. 需要确定网站数据更新时段。此时段容易出出错 18-19?
"""
import time

import logbook
import pandas as pd
from selenium.common.exceptions import (ElementNotInteractableException,
                                        NoSuchElementException,
                                        TimeoutException)
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from cnswd.sql.base import get_engine, get_session
from cnswd.sql.szx import Classification, ClassificationBom
from cnswd.utils import ensure_list, loop_codes, loop_period_by
from cnswd.websource.szx.data_browse import LEVEL_MAPS, DataBrowser

from .base import DATE_MAPS, MODEL_MAPS
from .utils import fixed_data


db_dir_name = 'dataBrowse'
logger = logbook.Logger('数据搜索')


def delete_data_of(class_, code=None):
    """删除表数据"""
    session = get_session(db_dir_name)
    if code is None:
        num = session.query(class_).delete(False)
    else:
        num = session.query(class_).filter(class_.证券简称 == code).delete(False)
    logger.notice(f"删除 表:{class_.__tablename__} {num}行")
    session.commit()
    session.close()


def _get_start_date(level, code, offset=1):
    """开始刷新时间"""
    session = get_session(db_dir_name)
    class_ = MODEL_MAPS[level]
    expr = getattr(class_, DATE_MAPS[level][0])
    cond = class_.证券代码 == code
    t_end_date = session.query(func.max(expr)).filter(cond).scalar()
    session.close()
    if t_end_date is None:
        return pd.Timestamp(DATE_MAPS[level][2])
    else:
        # 调整天数
        return t_end_date + pd.Timedelta(days=offset)


def _need_repalce(level, e, freq):
    today = pd.Timestamp('today')
    limit = 90 if freq == 'Q' else 365 
    if (today - e).days <= limit:
        return True
    else:
        return False


def _save_to_sql(level, df, e, code):
    """对于部分项目，由于每日可能新增行，采用重写方式完成保存"""
    class_ = MODEL_MAPS[level]
    expr = getattr(class_, DATE_MAPS[level][0])
    table_name = class_.__tablename__
    engine = get_engine(db_dir_name)
    freq = DATE_MAPS[level][1]
    # 首先删除旧数据
    if freq in ('Q', 'Y') and _need_repalce(level, e, freq):
        session = get_session(db_dir_name)
        num = session.query(class_).filter(expr == e, class_.证券代码 == code).delete(False)
        logger.notice(f"删除 表:{table_name} {num}行")
        session.commit()
        session.close()
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    logger.notice(f"表：{table_name}, 添加 {len(df)} 条记录")


def _loop_by(api, level, code):
    start = _get_start_date(level, code)
    if level == '3.1':
        # 融资融券数据导致不一致，需要清理旧数据
        start = start - pd.Timedelta(days=7)
        class_ = MODEL_MAPS[level]
        expr = getattr(class_, DATE_MAPS[level][0])
        table_name = class_.__tablename__
        session = get_session(db_dir_name)
        num = session.query(class_).filter(
            expr >= start, class_.证券代码 == code).delete(False)
        logger.notice(f"删除 表:{table_name} 股票代码：{code} {num}行")
        session.commit()
        session.close()
    today = pd.Timestamp('today')
    freq = DATE_MAPS[level][1]
    if freq == 'D':
        t1 = start.strftime(r'%Y-%m-%d')
        t2 = today.strftime(r'%Y-%m-%d')
        df = api.query(level, code, t1, t2)
        if df.empty:
            return
        df = fixed_data(df, level)
        _save_to_sql(level, df, start, code)
    else:
        ps = loop_period_by(start, today, freq)
        for _, e in ps:
            if freq == 'Y':
                t1 = e.year
                t2 = None
            elif freq == 'Q':
                t1 = e.year
                t2 = e.quarter
            df = api.query(level, code, t1, t2)
            if df.empty:
                continue
            df = fixed_data(df, level)
            _save_to_sql(level, df, e, code)


def _replace(api, level, code):
    # 删除旧数据
    delete_data_of(MODEL_MAPS[level], code)
    df = api.query(level, code)
    if df.empty:
        return
    df = fixed_data(df, level)
    engine = get_engine(db_dir_name)
    if_exists = 'append'
    table_name = MODEL_MAPS[level].__tablename__
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
    api.logger.notice(f"更新 表{table_name}, 股票代码 {code} 共 {len(df)} 条记录")


def _refresh(api, level, code):
    """刷新项目数据
    
    Arguments:
        api {api} -- api
        level {str} -- 项目
        codes {str} -- 股票代码列表
        b {int} -- 批号
    """
    if DATE_MAPS[level][1] is None:
        _replace(api, level, code)
    else:
        _loop_by(api, level, code)


def _valid_level(to_do):
    for l in to_do:
        if l not in MODEL_MAPS.keys():
            raise ValueError(f'不支持数据项目"{l}"')


def _ipo(api, codes):
    # 当前貌似不支持多代码获取数据，临时措施
    codes = sorted(codes)
    dfs = []
    for code in codes:
        df = api.query('7.5', code)
        if not df.empty:
            dfs.append(df)
    to_add = pd.concat(dfs)
    engine = get_engine(db_dir_name)
    table_name = MODEL_MAPS['7.5'].__tablename__
    to_add = fixed_data(to_add, '7.5')
    to_add.to_sql(table_name, con=engine, if_exists='replace', index=False)
    api.logger.notice(f"{'更新'} 表{table_name}, 共 {len(to_add)} 条记录")


def refresh(levles=None):
    """专题统计项目数据刷新"""
    if levles is None or len(levles) == 0:
        to_do = list(DATE_MAPS.keys())
    else:
        to_do = ensure_list(levles)
        _valid_level(to_do)
    done = {}
    api = DataBrowser(True)
    codes = api.get_all_codes()
    # 测试代码
    # codes = ['000001', '000002', '000007', '300467', '300483', '603633', '603636',
    #          '000333', '600017', '600645', '603999']
    for code in codes:
        for i in range(10):
            for level in to_do:
                key = ','.join([level, code])
                if done.get(key):
                    continue
                api.logger.notice(f'第{i+1}次尝试：{LEVEL_MAPS[level][0]}')
                try:
                    _refresh(api, level, code)
                    done[key] = True
                except (IntegrityError, ElementNotInteractableException, NoSuchElementException, TimeoutException, ValueError, ConnectionError) as e:
                    api.logger.notice(
                        f'{LEVEL_MAPS[level][0]} {code} \n {e!r}')
                    done[key] = False
                    api.driver.quit()
                    time.sleep(5)
                    api = DataBrowser(True)
    api.driver.quit()


def _update_classify_bom(api):
    table = ClassificationBom.__tablename__
    engine = get_engine(db_dir_name)
    bom = api.classify_bom
    bom.to_sql(table, engine, if_exists='replace')
    api.logger.info(f'表：{table} 更新 {len(bom):>4}行')


def _update_stock_classify(api):
    """更新股票分类信息"""
    table = Classification.__tablename__
    levels = []
    for nth in (1, 2, 3, 4, 5, 6):
        levels.extend(api.get_levels_for(nth))
    # CDR当前不可用
    levels = [x for x in levels if x not in (
        '6.6', '6.9') and api._is_end_level(x)]
    done = []
    dfs = []
    for i in range(10):
        to_do = sorted(list(set(levels) - set(done)))
        if len(to_do) == 0:
            break
        api.logger.notice(f'第{i+1}次尝试，剩余分类层级数量:{len(to_do)}')
        for level in sorted(to_do):
            try:
                df = api.get_classify_stock(level)
                dfs.append(df)
                done.append(level)
            except Exception as e:
                api.logger.notice(f"{e!r}")
        time.sleep(0.5)

    # 完整下载后，才删除旧数据
    delete_data_of(Classification)

    for df in dfs:
        if not df.empty:
            df.to_sql(table, get_engine(db_dir_name),
                      if_exists='append', index=False)
            api.logger.info(
                f'表：{table} 添加{len(df):>4}行')


def update_stock_classify():
    """刷新股票分类信息"""
    with DataBrowser(True) as api:
        _update_classify_bom(api)
        _update_stock_classify(api)
