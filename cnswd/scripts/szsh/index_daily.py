"""
刷新指数日线数据
"""

from datetime import datetime, timedelta

import logbook
from sqlalchemy import func

from cnswd.sql.base import get_session
from cnswd.sql.szsh import IndexDaily, TradingCalendar
from cnswd.websource.wy import get_main_index, fetch_history

logger = logbook.Logger('指数日线')
db_dir_name = 'szsh'

def _get_start_date(sess, code):
    """获取数据库中指定代码最后一日"""
    last_date = sess.query(func.max(IndexDaily.日期)).filter(
        IndexDaily.指数代码 == code).scalar()
    if last_date is None:
        start = None
    else:
        start = last_date + timedelta(days=1)
    return start


def _gen(df):
    res = []
    for d, row in df.iterrows():
        obj = IndexDaily(指数代码=row['股票代码'][1:],
                         日期=d.date(),
                         开盘价=row['开盘价'],
                         最高价=row['最高价'],
                         最低价=row['最低价'],
                         收盘价=row['收盘价'],
                         成交量=row['成交量'],
                         成交额=row['成交金额'],
                         涨跌幅=row['涨跌幅'])
        res.append(obj)
    return res


def flush(codes, end):
    for code in codes:
        sess = get_session(db_dir_name)
        start = _get_start_date(sess, code)
        if start is not None and start > end:
            logger.info('代码：{} 无需刷新'.format(code))
            continue
        try:
            df = fetch_history(code=code, start=start, end=end, is_index=True)
            # 按日期排序（升序）
            df.sort_index(inplace=True)
        except ValueError:
            # 当开始日期大于结束日期时，触发值异常
            logger.info('无法获取网页数据。代码：{}，开始日期：{}, 结束日期：{}'.format(
                code, start, end))
            continue
        objs = _gen(df)
        sess.add_all(objs)
        sess.commit()
        logger.info('代码：{}, 新增{}行'.format(
            code, len(objs)))
        sess.close()


def flush_index_daily():
    """刷新指数日线数据"""
    sess = get_session(db_dir_name)
    end = sess.query(func.max(TradingCalendar.日期)).filter(
        TradingCalendar.交易日 == True).scalar().date()
    if end is None:
        raise NotImplementedError('尚未初始化交易日历数据！')
    codes = get_main_index().index
    sess.close()
    flush(codes, end)
