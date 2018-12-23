"""
刷新股票日线成交数据

性能：刷新1天数据大约2分钟
"""
import math
import time
from multiprocessing import Pool, cpu_count
# from concurrent.futures import ThreadPoolExecutor
import logbook
import pandas as pd
from sqlalchemy import func

from cnswd.sql.base import get_engine, get_session
from cnswd.sql.szsh import StockDaily, TradingCalendar
from cnswd.websource.wy import fetch_history

from .base import get_ipo_date, get_valid_codes, last_date, need_refresh

logger = logbook.Logger('股票日线')
db_dir_name = 'szsh'
max_worker = int(cpu_count()/2)


def _fix_data(df):
    """整理数据"""
    df['股票代码'] = df['股票代码'].map(lambda x: x[1:])
    # df.reset_index(inplace=True)
    return df


def get_data(code):
    """获取单个股票的日线数据"""
    sess = get_session(db_dir_name)
    d_ = last_date(sess, StockDaily, code)
    sess.close()
    if d_ is None:
        s = get_ipo_date(code)
    else:
        s = d_ + pd.Timedelta(days=1)
    if s > pd.Timestamp('today').normalize():
        return pd.DataFrame()
    df = _fix_data(fetch_history(code, s))
    return df


def refresh_daily():
    """刷新股票日线数据"""
    # 使用 `not need_refresh()`
    # 未来日期在数据库中无数据，返回None。 not None -> True
    if not need_refresh():
        return
    start = time.time()
    codes = get_valid_codes()
    with Pool(max_worker) as p:
        dfs = p.map(get_data, codes)
    # with ThreadPoolExecutor(max_worker) as executor:
    #     dfs = executor.map(get_data, codes)
    engine = get_engine(db_dir_name)
    table_name = 'stock_dailies'
    to_add = pd.concat(dfs)
    if len(to_add):
        to_add.to_sql(table_name, engine, if_exists='append')
        logger.info(f'添加{to_add.shape[0]:>4}行')
    print(f"总用时：{(time.time() - start):>.4f}秒")