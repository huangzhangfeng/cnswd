import os
import time
from functools import partial
from multiprocessing import Pool

import logbook
import pandas as pd
from selenium.common.exceptions import (ElementNotInteractableException,
                                        NoSuchElementException,
                                        TimeoutException)
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from cnswd.sql.base import get_engine, get_session
from cnswd.utils import ensure_list, loop_period_by
from cnswd.websource.szx.thematic_statistics import (LEVEL_MAPS,
                                                     ThematicStatistics)

from .base import DATE_MAPS, MODEL_MAPS
from .utils import fixed_data
from ..utils import kill_proc


db_dir_name = 'thematicStatistics'
logger = logbook.Logger('专题统计')
max_worker = max(1, int(os.cpu_count()/2))


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


def _save_to_sql(level, df, e):
    """对于部分项目，由于每日可能新增行，采用重写方式完成保存"""
    class_ = MODEL_MAPS[level]
    expr = getattr(class_, DATE_MAPS[level][0])
    table_name = class_.__tablename__
    engine = get_engine(db_dir_name)
    # 首先删除旧数据
    if level in ('7.1',) and _need_repalce(level, e):
        session = get_session(db_dir_name)
        num = session.query(class_).filter(expr == e).delete(False)
        logger.notice(f"删除 表:{table_name} {num}行")
        session.commit()
        session.close()
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    logger.notice(f"表：{table_name}, 添加 {len(df)} 条记录")


def _loop_by(api, level):
    start = _get_start_date(level)
    today = pd.Timestamp('today')
    freq = DATE_MAPS[level][1]
    # 业绩预测应该包含未来日期
    if level in ('6.1',):
        ps = loop_period_by(start, today, freq, False)
    else:
        if start > today:
            return
        ps = loop_period_by(start, today, freq, True)
    for _, e in ps:
        if freq == 'Q':
            t1 = e.year
            t2 = e.quarter
        else:
            t2 = t1 = e.strftime(r'%Y-%m-%d')
        df = api.query(level, t1, t2)
        if df.empty:
            continue
        df = fixed_data(df)
        _save_to_sql(level, df, e)
        api.driver.implicitly_wait(0.3)


def _replace(api, level):
    df = api.query(level)
    if df.empty:
        return
    df = fixed_data(df)
    engine = get_engine(db_dir_name)
    if_exists = 'replace'
    table_name = MODEL_MAPS[level].__tablename__
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
    api.logger.notice(f"重写表{table_name}, 共 {len(df)} 条记录")


def _refresh(api, level):
    # if level in ('5.1', '5.4', '12.1', '12.2', '12.3'):
    if DATE_MAPS[level][1] is None:
        _replace(api, level)
    else:
        _loop_by(api, level)


def _valid_level(to_do):
    for l in to_do:
        if l not in MODEL_MAPS.keys():
            raise ValueError(f'不支持项目层级{l}')


def _refresh_data(level, retry):
    done = {}
    try:
        api = ThematicStatistics(True)
    except Exception as e:
        time.sleep(5)
        api = ThematicStatistics(True)
    for i in range(retry):
        if done.get(level):
            continue
        api.logger.notice(f'第{i+1}次尝试：{LEVEL_MAPS[level][0]}')
        try:
            _refresh(api, level)
            done[level] = True
        except Exception as e:
            api.logger.notice(f'{LEVEL_MAPS[level][0]} \n {e!r}')
            done[level] = False
            api.driver.quit()
            api = ThematicStatistics(True)
    api.driver.quit()


def refresh(levles=None):
    """专题统计项目数据刷新"""
    if levles is None or len(levles) == 0:
        to_do = MODEL_MAPS.keys()
    else:
        to_do = ensure_list(levles)
        _valid_level(to_do)
    func = partial(_refresh_data, retry=3)
    try:
        with Pool(max_worker) as p:
            p.map(func, to_do)
    except Exception:
        # 再次尝试
        time.sleep(10)
        with Pool(max_worker) as p:
            p.map(func, to_do)
    finally:
        kill_proc()
