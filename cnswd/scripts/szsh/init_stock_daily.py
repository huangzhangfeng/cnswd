"""

初始化所有股票的日线交易数据模块(cn)

"""
import logbook
import pandas as pd

from cnswd.constants import MARKET_START
from cnswd.scripts.szsh.base import last_date
from cnswd.sql.base import get_session
from cnswd.sql.szsh import StockDaily
from cnswd.websource.wy import fetch_history

from .base import get_ipo_date, get_valid_codes

logger = logbook.Logger('初始化股票日线数据')
db_dir_name = 'szsh'

def _fix_data(df):
    """整理数据"""
    df['股票代码'] = df['股票代码'].map(lambda x: x[1:])
    # df.reset_index(inplace=True)
    return df


def _refresh_data(code, sess):
    """刷新或者添加单个股票的日线数据"""
    d_ = last_date(sess, StockDaily, code)
    if d_ is None:
        s = get_ipo_date(code)
        if s is None:
            s = MARKET_START.tz_convert(None)
    else:
        s = d_ + pd.Timedelta(days=1)
    if s > pd.Timestamp('today').normalize():
        logger.info('{} 数据已经是最新状态'.format(code))
        return
    df = _fix_data(fetch_history(code, s))
    if len(df):
        table_name = 'stock_dailies'
        df.to_sql(table_name, sess.bind.engine, if_exists='append')
        logger.info('添加{}数据共{}行'.format(code, df.shape[0]))
    else:
        logger.info('{} {} 无数据添加'.format(code, s))


def init_stock_daily_data():
    """初始化所有股票日线数据(含已经退市股票)"""
    # 多进程容易引起数据库死锁
    # 单进程速度极快，不必要复杂化
    codes = get_valid_codes(False)
    sess = get_session(db_dir_name)
    for code in codes:
        _refresh_data(code, sess)
    sess.close()
