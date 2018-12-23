"""
刷新交易日历
"""
import logbook
import pandas as pd
from sqlalchemy import func

from cnswd.sql.base import get_session
from cnswd.sql.szsh import TradingCalendar

from .date_utils import get_non_trading_days, get_trading_dates

logger = logbook.Logger('交易日历')

start_date = pd.Timestamp('1990-01-01').date()
end_date = pd.Timestamp('now').date()


db_dir_name = 'szsh'

def has_data():
    sess = get_session(db_dir_name)
    q = sess.query(TradingCalendar)
    res = sess.query(q.exists()).scalar()
    sess.close()
    return res


def _add_or_update(d, status):
    sess = get_session(db_dir_name)
    old = sess.query(
        TradingCalendar
    ).filter(
        func.date(TradingCalendar.日期) == d.date()
    ).one_or_none()
    if old:
        old.交易日 = status
    else:
        to_add = TradingCalendar(日期=d.date(), 交易日=status)
        sess.add(to_add)
    info = '交易日' if status else '*非*交易日'
    logger.info('添加或者刷新{}：{}'.format(info, d.date()))
    sess.commit()
    sess.close()


def update_trading_calendars():
    """添加或更新交易日历"""
    trading_days = get_trading_dates()
    non_trading_days = get_non_trading_days(None, None)
    if not has_data():
        for d in trading_days:
            _add_or_update(d, True)
        for d in non_trading_days:
            _add_or_update(d, False)
    else:
        # 可能存在长期放假，稳妥起见，刷新最近30天的交易日期
        # 按降序排列，加速处理
        b30 = pd.Timedelta(days=30)
        limit = end_date - b30
        for d in sorted(trading_days, reverse=True):
            if d.date() < limit:
                break
            else:
                _add_or_update(d, True)
        for d in sorted(non_trading_days, reverse=True):
            if d.date() < limit:
                break
            else:
                _add_or_update(d, False)
