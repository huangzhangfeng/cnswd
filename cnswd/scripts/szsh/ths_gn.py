"""
覆盖式更新

同花顺网站禁止多进程提取数据
"""
import math
import os
import random
import time
import warnings
from multiprocessing import Pool

import logbook
import pandas as pd
from cnswd.sql.base import get_session, get_engine
from cnswd.sql.szsh import THSGN
from cnswd.utils import loop_codes
from cnswd.websource.ths import THS

from ..utils import is_trading_time

max_worker = 1 # max(1, int(os.cpu_count()/2)) 网站对多进程有限制

log = logbook.Logger('同花顺')
db_dir_name = 'szsh'


def batch_codes(iterable):
    """
    切分可迭代对象，返回长度max_worker*4批次列表

    说明：
        1. 初始化浏览器很耗时，且占有大量内存
        2. 切分目标主要是平衡速度与稳定性
    """
    min_batch_num = max_worker * 4
    batch_num = max(min_batch_num, math.ceil(len(iterable) / max_worker / 4))
    return loop_codes(iterable, batch_num)


def has_data(session, gn_code, page_no):
    """查询项目中是否存在指定代码的数据"""
    q = session.query(THSGN).filter(
        THSGN.概念编码 == gn_code,
        THSGN.页码 == page_no
    )
    return session.query(q.exists()).scalar()


def _update_page(api, sess, gn_code, gn_name, page):
    """更新概念页信息"""
    if not has_data(sess, gn_code, page):
        try:
            df = api.get_gn_detail(gn_code, page)
            df['概念'] = gn_name
            df['页码'] = page
            df.to_sql('thsgns', sess.bind.engine,
                      index=False, if_exists='append')
            log.info('保存 {} 第{}页 {}行'.format(gn_name, page, df.shape[0]))
            time.sleep(random.randint(3, 6)/10)
            return True
        except ValueError:
            return False
    return True


def _add_gn_page(api, sess, gn_codes, d):
    failed = []
    for gn in gn_codes:
        page_num = api.get_gn_page_num(gn)
        for page in range(1, page_num+1):
            if not _update_page(api, sess, gn, d[gn], page):
                failed.append(gn)
    return set(failed)


def _update_gn_list(urls):
    sess = get_session(db_dir_name)
    api = THS()
    codes = [x[0][-7:-1] for x in urls]
    d = {x[0][-7:-1]: x[1] for x in urls}
    for i in range(20):
        log.info('第{}次尝试，剩余{}个概念'.format(i+1, len(codes)))
        codes = _add_gn_page(api, sess, codes, d)
        if len(codes) == 0:
            break
        time.sleep(1)
    api.browser.quit()
    sess.close()


def update_gn_list():
    """
    更新股票概念列表

    盘后更新有效
    """
    if is_trading_time():
        warnings.warn('建议非交易时段更新股票概念。交易时段内涨跌幅经常变动，容易产生重复值！！！')
    sess = get_session(db_dir_name)
    # 首先删除原有数据
    sess.query(THSGN).delete()
    sess.commit()
    sess.close()
    api = THS()
    urls = api.gn_urls
    api.browser.quit()
    _update_gn_list(urls)


def update_gn_time():
    """
    更新股票概念概述列表
    """
    engine = get_engine(db_dir_name)
    with THS() as api:
        df = api.gn_times
        df.to_sql('thsgn_times', engine, index=False, if_exists='replace')