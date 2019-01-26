"""

深证信数据搜索刷新脚本

备注：
    1. 需要确定网站数据更新时段。此时段容易出出错 18-19?
"""
import time

import logbook
import pandas as pd
from selenium.common.exceptions import (ElementNotInteractableException,
                                        TimeoutException,
                                        NoSuchElementException)
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from cnswd.sql.base import get_engine, get_session
from cnswd.utils import ensure_list, loop_period_by, loop_codes
from cnswd.websource.szx.data_browse import LEVEL_MAPS, DataBrowser

from .base import DATE_MAPS, MODEL_MAPS
from .utils import fixed_data


db_dir_name = 'dataBrowse'
logger = logbook.Logger('数据搜索')
batch_num = 50


def _get_start_date(level, offset=1):
    """开始刷新时间"""
    session = get_session(db_dir_name)
    class_ = MODEL_MAPS[level]
    expr = getattr(class_, DATE_MAPS[level][0])
    t_end_date = session.query(func.max(expr)).scalar()
    session.close()
    if t_end_date is None:
        return DATE_MAPS[level][2]
    else:
        # 调整天数
        return t_end_date + pd.Timedelta(days=offset)


def _need_repalce(level, e):
    today = pd.Timestamp('today')
    if (today - e).days <= 90:
        return True
    else:
        return False


def _save_to_sql(level, df, e, b):
    """对于部分项目，由于每日可能新增行，采用重写方式完成保存"""
    class_ = MODEL_MAPS[level]
    expr = getattr(class_, DATE_MAPS[level][0])
    table_name = class_.__tablename__
    engine = get_engine(db_dir_name)
    freq = DATE_MAPS[level][1]
    # 首先删除旧数据
    if freq == 'Q' and _need_repalce(level, e) and b == 0:
        session = get_session(db_dir_name)
        num = session.query(class_).filter(expr == e).delete(False)
        logger.notice(f"删除 表:{table_name} {num}行")
        session.commit()
        session.close()
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    logger.notice(f"表：{table_name}, 添加 {len(df)} 条记录")


def _loop_by(api, level, codes, b):
    start = _get_start_date(level)
    if level == '3.1':
        # 融资融券数据导致不一致，需要清理旧数据
        start = pd.Timestamp(start) - pd.Timedelta(days=7)
        class_ = MODEL_MAPS[level]
        expr = getattr(class_, DATE_MAPS[level][0])
        table_name = class_.__tablename__
        session = get_session(db_dir_name)
        num = session.query(class_).filter(expr >= start).delete(False)
        logger.notice(f"删除 表:{table_name} {num}行")
        session.commit()
        session.close()
    today = pd.Timestamp('today')
    freq = DATE_MAPS[level][1]
    ps = loop_period_by(start, today, freq)
    for _, e in ps:
        if freq == 'Q':
            t1 = e.year
            t2 = e.quarter
        else:
            t2 = t1 = e.strftime(r'%Y-%m-%d')
        df = api.query(level, codes, t1, t2)
        if df.empty:
            continue
        df = fixed_data(df, level)
        _save_to_sql(level, df, e, b)
        api.driver.implicitly_wait(0.3)


def _replace(api, level, codes, b):
    df = api.query(level, codes)
    if df.empty:
        return
    df = fixed_data(df, level)
    engine = get_engine(db_dir_name)
    if b == 0:
        action = '重写'
        if_exists = 'replace'
    else:
        if_exists = 'append'
        action = '添加'
    table_name = MODEL_MAPS[level].__tablename__
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
    api.logger.notice(f"{action} 表{table_name}, 共 {len(df)} 条记录")


def _refresh(api, level, codes, b):
    """刷新项目数据
    
    Arguments:
        api {api} -- api
        level {str} -- 项目
        codes {list} -- 股票代码列表
        b {int} -- 批号
    """
    if DATE_MAPS[level][1] is None:
        _replace(api, level, codes, b)
    else:
        _loop_by(api, level, codes, b)


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
        to_do.remove('7.5')
    else:
        to_do = ensure_list(levles)
        _valid_level(to_do)
    done = {}
    api = DataBrowser(True)
    codes = api.get_all_codes()
    # 测试代码
    # codes = ['000001', '000002', '000007', '300467', '300483', '603633', '603636',
    #          '000333', '600017', '600645', '603999']
    if '7.5' in to_do:
        to_do.remove('7.5')
        _ipo(api, codes)
    b_codes = loop_codes(codes, batch_num)
    for b, codes in enumerate(b_codes):
        for i in range(10):
            for level in to_do:
                key = ','.join([level] + codes)
                if done.get(key):
                    continue
                api.logger.notice(f'第{i+1}次尝试：{LEVEL_MAPS[level][0]}')
                try:
                    _refresh(api, level, codes, b)
                    done[key] = True
                except (IntegrityError, ElementNotInteractableException, NoSuchElementException, TimeoutException, ValueError, ConnectionError) as e:
                    api.logger.notice(
                        f'{LEVEL_MAPS[level][0]} {codes} \n {e!r}')
                    done[key] = False
                    api.driver.quit()
                    time.sleep(5)
                    api = DataBrowser(True)
    api.driver.quit()
