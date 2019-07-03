from functools import partial
from multiprocessing import Pool, cpu_count
import math
import logbook
import pandas as pd
from sqlalchemy.exc import IntegrityError

from cnswd.sql.base import get_engine, get_session
from cnswd.sql.info import EconomicNews
from cnswd.websource.sina_news import TOPIC_MAPS, Sina247News

logger = logbook.Logger('财经新闻')
db_dir_name = 'info'


def _append_historical_news(tag, times):
    """追加历史消息"""
    session = get_session(db_dir_name)
    count = 0
    with Sina247News() as api:
        data = api._get_topic_news(tag, times)
    for news in data:
        obj = EconomicNews()
        obj.序号 = news[0]
        obj.时间 = pd.Timestamp('{} {}'.format(news[1], news[2]))
        obj.概要 = news[3]
        obj.分类 = news[4]
        session.add(obj)
        try:
            session.commit()
            count += 1
            logger.info(f"栏目：{TOPIC_MAPS[tag]:>4} 已添加，序号：{news[0]}")
        except IntegrityError:
            logger.notice(f"序号：{news[0]}已经存在，自动回滚撤销")
            session.rollback()
    logger.info(f"栏目：{TOPIC_MAPS[tag]:>4} 累计添加{count:>4}条")
    session.close()


def append_historical_news(times):
    """追加历史消息"""
    logger.info(f'追加历史消息，翻页次数：{times}')
    func = partial(_append_historical_news, times=times)
    worker = math.ceil(cpu_count() / 2)
    with Pool(worker) as p:
        p.map(func, TOPIC_MAPS.keys())


def refresh_news():
    """刷新最新财经消息"""
    session = get_session(db_dir_name)
    count = 0
    with Sina247News() as api:
        data = api.live_data
    data = sorted(data, key=lambda item: item[0], reverse=True)
    for news in data:
        obj = EconomicNews()
        obj.序号 = news[0]
        obj.时间 = pd.Timestamp('{} {}'.format(news[1], news[2]))
        obj.概要 = news[3]
        obj.分类 = news[4]
        session.add(obj)
        try:
            session.commit()
            count += 1
        except IntegrityError:
            session.rollback()
            # # 排序后的数据，如已经存在，则退出循环
            # break
    logger.info(f'添加{count:4} 行')
    session.close()
