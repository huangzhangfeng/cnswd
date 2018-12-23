"""
新浪网可以提取任意日期的分时成交数据，但对频繁提取采取了限制
网易只提供了最近5日的分时成交数据，速度极快。目前观测结果：当日只能采集到昨日数据。

只支持近期数据刷新
"""
import os
import time
from functools import partial
from urllib.error import URLError

import logbook
import pandas as pd
from numpy import random
from sqlalchemy import func

from cnswd.sql.base import get_engine, get_session, session_scope
from cnswd.sql.szsh import CJMX, StockDaily
from cnswd.utils import data_root, loop_codes
from cnswd.websource._selenium import make_headless_browser
from cnswd.websource.exceptions import NoWebData
from cnswd.websource.wy import fetch_cjmx as wy_fetch_cjmx

from ..runner import TryToCompleted
from .base import get_ipo_date, get_valid_codes, need_refresh

logger = logbook.Logger('股票成交明细')
DATE_FMT = r'%Y-%m-%d'
db_dir_name = 'szsh'
BACK_DAYS = 20
SINA_WAIT = 6 * 60  # 6分钟


class NoopException(Exception):
    pass


def _add_prefix(stock_code):
    """查询代码"""
    pre = stock_code[0]
    if pre == '6':
        return 'sh{}'.format(stock_code)
    else:
        return 'sz{}'.format(stock_code)


def _to_date_str(date):
    """转换为查询日期格式"""
    return pd.Timestamp(date).strftime(DATE_FMT)


def _fix_data(df, code, date_str):
    """整理数据框"""
    if '涨跌幅' in df:
        del df['涨跌幅']
    df.columns = ['成交时间', '成交价', '价格变动', '成交量', '成交额', '性质']
    df.成交时间 = df.成交时间.map(lambda x: pd.Timestamp('{} {}'.format(date_str, x)))
    df['股票代码'] = code
    df['成交量'] = df['成交量'] * 100
    df = df.sort_values('成交时间')
    return df


def _fetch_one_page(driver, url):
    """提取单页成交明细数据"""
    driver.get(url)
    df = pd.read_html(driver.page_source, attrs={
                      'id': 'datatbl'}, na_values=['--'])[0]
    return df


def _sleep_time(d):
    today = pd.Timestamp('today').date()
    if d < today - pd.Timedelta(days=BACK_DAYS):
        return random.randint(10, 20)
    else:
        return random.randint(10, 20) / 10


def is_downloaded(driver, url):
    try:
        df = _fetch_one_page(driver, url)
        return (df, True)
    except ValueError as e:
        logger.info(f'{e!r}')
        logger.notice(f"休眠{6*60}秒")
        time.sleep(6*60)
        return (None, False)


def fetch_cjmx(driver, code, date_str):
    qcode = _add_prefix(code)
    date = pd.Timestamp(date_str).date()
    date_str = date.strftime(DATE_FMT)
    dfs = []
    for page in range(1, 1000):
        url = f"http://market.finance.sina.com.cn/transHis.php?symbol={qcode}&date={date_str}&page={page}"
        for _ in range(3):
            df, satus = is_downloaded(driver, url)
            if satus:
                break
        if df is None:
            raise NoopException(f'下载股票：{code} {date_str} 分时数据不成功')
        if df.empty:
            break
        dfs.append(df)
        logger.info(f'股票：{code} {date_str} 第{page:>3}页共{len(df):>3}行')
        time.sleep(random.randint(3, 5) / 10)
    df = pd.concat(dfs)
    df = _fix_data(df, code, date_str)
    return df


def data_path(code, date_str):
    name = pd.Timestamp(date_str).strftime(r'%Y%m%d')
    data_dir = os.path.join(data_root('cjmx'), code)
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)
    return os.path.join(data_dir, f'{name}.pkl')


def download_to_local(driver, code, date_str, path, t):
    try:
        df = fetch_cjmx(driver, code, date_str)
        df.to_pickle(path)
    except NoopException:
        logger.info(f'下载股票：{code} {date_str} 分时数据不成功，跳过')


def init_cjmx():
    """初始化股票成交明细"""
    t = 6*60
    # 限定在2018年以后
    begin = 2018
    codes = get_valid_codes(False)
    driver = make_headless_browser()
    for i, code in enumerate(codes):
        if (i+1) % 5 == 0:
            time.sleep(1)
        start = get_ipo_date(code)
        # 跳过没有开始日期的股票
        if start is None:
            continue
        date_rng = pd.date_range(
            start, pd.Timestamp('today')-pd.Timedelta(days=1), freq='B')
        for d in date_rng:
            if d.year >= begin:
                date_str = pd.Timestamp(d).strftime(DATE_FMT)
                path = data_path(code, date_str)
                if not os.path.exists(path):
                    download_to_local(driver, code, date_str, path, t)
    driver.quit()


def sina_refresh_cjmx(date):
    """新浪刷新股票成交明细"""
    codes = get_valid_codes(True)
    driver = make_headless_browser()
    date_str = pd.Timestamp(date).strftime(DATE_FMT)
    for i, code in enumerate(codes):
        if (i+1) % 5 == 0:
            time.sleep(1)
        path = data_path(code, date_str)
        if not os.path.exists(path):
            download_to_local(driver, code, date_str, path, SINA_WAIT)
    driver.quit()


def has_traded(code, date):
    """查询股票当天是否交易"""
    with session_scope(db_dir_name) as session:
        q = session.query(StockDaily).filter(
            StockDaily.股票代码 == code,
            func.date(StockDaily.日期) == date,
            StockDaily.成交量 > 1,
        )
        return session.query(q.exists()).scalar()


def has_cjmx(code, date):
    """查询是否已经存在分时数据"""
    with session_scope(db_dir_name) as session:
        q = session.query(CJMX).filter(
            CJMX.股票代码 == code,
            func.date(CJMX.成交时间) == date
        )
        return session.query(q.exists()).scalar()


def _wy_fix_data(df):
    dts = df.日期.dt.strftime(r'%Y-%m-%d') + ' ' + df.时间
    df['成交时间'] = pd.to_datetime(dts)
    del df['时间']
    del df['日期']
    df = df.rename(columns={'价格': '成交价',
                            '涨跌额': '价格变动', '方向': '性质'})
    return df


def wy_to_db(code, date):
    engine = get_engine(db_dir_name)
    if has_traded(code, date) and not has_cjmx(code, date):
        date_str = date.strftime(DATE_FMT)
        try:
            df = wy_fetch_cjmx(code, date_str)
        except NoWebData as e:
            logger.info(f'股票：{code} {date_str} {e!r}')
            return
        df = _wy_fix_data(df)
        df.to_sql(CJMX.__tablename__, engine, if_exists='append', index=False)
        logger.info(f'股票：{code} {date_str} 共{len(df):>3}行')


def wy_refresh_cjmx(date_str):
    """刷新指定日期成交明细数据"""
    if not need_refresh(date_str):
        return
    codes = get_valid_codes(True)
    date = pd.Timestamp(date_str).date()
    p_func = partial(wy_to_db, date=date)
    p_codes = loop_codes(codes, 300)
    # 当一次性map时，经常会出现长时间堵塞，尝试分批执行
    for i, b_codes in enumerate(p_codes):
        logger.notice(f"第{i+1}批，共{len(b_codes)}代码")
        runner = TryToCompleted(p_func, b_codes, (URLError, TimeoutError), 10, 3)
        runner.run()
        time.sleep(1)
