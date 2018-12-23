import pandas as pd
from sqlalchemy import func, text
from sqlalchemy.sql import exists

from cnswd.sql.base import get_session
from cnswd.sql.szsh import (Delisting, Stock, StockDaily, Suspend,
                            TradingCalendar)

DT_FMT = r'%Y-%m-%d %H:%M:%S.%f'


def get_valid_codes(is_trading=False):
    """有效A股股票代码"""
    session = get_session('szsh')
    codes = session.query(
        Stock.A股代码
    ).filter(
        Stock.A股代码.isnot(None)
    ).all()
    d_codes = session.query(
        Delisting.证券代码
    ).all()
    s_codes = session.query(
        Suspend.证券代码
    ).all()
    if is_trading:
        res = [x[0] for x in codes]
    else:
        res = [x[0] for x in codes] + [x[0]
                                       for x in d_codes] + [x[0] for x in s_codes]
    res = set([x for x in res if x[0] in ('0', '3', '6')])
    session.close()
    return sorted(res)


def get_ipo_date(code):
    """获取股票上市日期"""
    session = get_session('szsh')
    res = session.query(
        Stock.A股上市日期
    ).filter(
        Stock.A股代码 == code
    ).scalar()
    session.close()
    return res


def has_data(session, obj, code):
    """查询项目中是否存在指定代码的数据"""
    q = session.query(obj).filter(
        obj.股票代码 == code
    )
    return session.query(q.exists()).scalar()


def last_date(session, obj, code):
    """查询股票项目数据最后一天"""
    return session.query(func.max(obj.日期)).filter(
        obj.股票代码 == code
    ).scalar()


def is_trading_day(one_day):
    """查询日期是否为交易日"""
    db_dir_name = 'szsh'
    sess = get_session(db_dir_name)
    res = sess.query(
        TradingCalendar.交易日
    ).filter(
        func.date(TradingCalendar.日期) == one_day,
    ).scalar()
    sess.close()
    return res


def need_refresh(oneday=None):
    """
    判断指定日期是否需要更新
    0. 如指定日期，则判断指定日期是否为交易日；
    如不指定，分以下二种情形：
    1. 如当前时点为下午18点后，判断当日是否为交易日；
    2. 如当前时点为下午18点前，判断昨日是否为交易日；
    
    如为交易日则更新
    """
    if oneday is None:
        today = pd.Timestamp('today')
        hour = today.hour
        if hour >= 18:
            oneday = today.date()
        else:
            oneday = (today - pd.Timedelta(days=1)).date()
    else:
        oneday = pd.Timestamp(oneday).date()
    return is_trading_day(oneday)


def get_precomputed_shanghai_holidays():
    """自开市以来至今，除周六周日外的假期"""
    db_dir_name = 'szsh'
    sess = get_session(db_dir_name)
    res = sess.query(
        func.date(TradingCalendar.日期)
    ).filter(
        TradingCalendar.交易日 == 0,
    ).all()
    sess.close()
    return [x[0] for x in res if pd.Timestamp(x[0]).day_name() not in ('Saturday','Sunday')]
 
