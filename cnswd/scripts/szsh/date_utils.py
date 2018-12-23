"""

日期辅助模块

注：
    1. 由于网易指数数据可能存在超过1天的延时，在判断交易日时，需要增加当天是否为交易日
"""

import datetime as dt
from functools import lru_cache
import numpy as np
import pandas as pd

from cnswd.constants import MARKET_START
from cnswd.utils import sanitize_dates
from cnswd.websource.sina import fetch_quotes
from cnswd.websource.tencent import get_recent_trading_stocks
from cnswd.websource.wy import fetch_history


@lru_cache(None)
def _historical_trading_dates(start, end):
    """
    以指数历史数据计算期间的所有交易日期

    网络数据存在延迟，如当天交易，历史数据可能在次日或次次日
    才提供，导致近期交易日的计算存在误差。
    """
    df = fetch_history('000001', start, end, True)
    return df.index


def is_trading_day(oneday=dt.datetime.today()):
    """判断是否为交易日"""
    oneday = pd.Timestamp(oneday).date()
    if oneday == dt.datetime.today().date():
        # codes = np.random.choice(get_recent_trading_stocks(), 10, False)
        # 四只股票同一天同时停牌的概率极小
        codes = ['000001','300001','600000','002024']
        t_dt = fetch_quotes(codes)['日期'].values.max()
        return pd.Timestamp(t_dt).date() == oneday
    else:
        trading_dates = _historical_trading_dates(None, oneday)
        return oneday in [x.date() for x in trading_dates]


def get_trading_dates(start=None, end=None, tz='utc'):
    """期间所有交易日
    
    Keyword Arguments:
        start {date like} -- 开始日期 (default: {None})
        end {[type]} -- 结束日期 (default: {None})
        tz {str} -- 输出目标时区 (default: {'utc'})
    
    Returns:
        DatetimeIndex -- 期间交易日期
            如含未来日期，则未来工作日视同为交易日

    存在的情形：
        1. start > today
                start-----end
              ^
            today
        2. today == start
            start-----end 
              ^
            today
        3. start < today < end
            start-----end 
                   ^
                 today
        4. today == end
            start-----end 
                       ^
                     today
        5. today > end    
            start-----end        
                           ^
                         today
    """
    start, end = sanitize_dates(start, end)
    assert (end - start).days >= 1, '期间最少相隔1天'
    today = dt.datetime.today().date()
    if start > today:
        dates = pd.bdate_range(start, end, freq='B').sort_values()
    if start == today:
        dates = pd.bdate_range(today + pd.Timedelta(days=1), end, freq='B')
        if is_trading_day(today):
            dates = dates.append(pd.DatetimeIndex([today]))
    if start < today < end:
        dates = _historical_trading_dates(start, today - pd.Timedelta(days=1))
        if is_trading_day(today):
            dates = dates.append(pd.DatetimeIndex([today]))
        future = pd.bdate_range(today + pd.Timedelta(days=1), end, freq='B')
        dates = dates.append(future)
    if end == today:
        dates = _historical_trading_dates(start, today - pd.Timedelta(days=1))
        if is_trading_day(today):
            dates = dates.append(pd.DatetimeIndex([today]))
    if end < today:
        dates = _historical_trading_dates(start, end)
    return dates.tz_localize(tz).sort_values()


def get_non_trading_days(start, end, tz='utc'):
    """自然日历中除交易日外的日期定义为非交易日期"""
    start, end = sanitize_dates(start, end)
    assert (end - start).days >= 1, '期间最少相隔1天'
    all_days = pd.date_range(start, end, tz=tz)
    trading_dates = get_trading_dates(start, end, tz)
    diff_ = all_days.difference(trading_dates)
    return diff_


def get_adhoc_holidays(start, end, tz='utc'):
    """
    非交易日的其中的工作日

    **注意**
        不同于非交易日
            adhoc_holidays = 非交易日 - 周末日期
    """
    start, end = sanitize_dates(start, end)
    assert (end - start).days >= 1, '期间最少相隔1天'
    b_dates = pd.bdate_range(start, end, tz=tz)
    trading_dates = get_trading_dates(start, end, tz)
    diff_ = b_dates.difference(trading_dates)
    return diff_


def is_working_day(oneday=dt.datetime.today()):
    """判断是否为工作日"""
    oneday = pd.Timestamp(oneday).date()
    return oneday.isoweekday() in range(1, 6)
