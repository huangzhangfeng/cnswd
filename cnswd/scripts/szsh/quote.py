import asyncio
import time
import re
import aiohttp
import logbook
import pandas as pd
from cnswd.sql.base import get_engine, session_scope
from cnswd.utils import loop_codes
from cnswd.constants import QUOTE_COLS

from .base import get_valid_codes, need_refresh

logger = logbook.Logger('实时报价')

QUOTE_PATTERN = re.compile('"(.*)"')
CODE_PATTERN = re.compile(r'hq_str_s[zh](\d{6})')
db_dir_name = 'szsh'


def _convert_to_numeric(s, exclude=()):
    if pd.api.types.is_string_dtype(s):
        if exclude:
            if s.name not in exclude:
                return pd.to_numeric(s, errors='coerce')
    return s


def _to_dataframe(content):
    """解析网页数据，返回DataFrame对象"""
    res = [x.split(',') for x in re.findall(QUOTE_PATTERN, content)]
    codes = [x for x in re.findall(CODE_PATTERN, content)]
    df = pd.DataFrame(res).iloc[:, :32]
    df.columns = QUOTE_COLS[1:]
    df.insert(0, '股票代码', codes)
    df.dropna(inplace=True)
    return df


def _add_prefix(stock_code):
    pre = stock_code[0]
    if pre == '6':
        return 'sh{}'.format(stock_code)
    else:
        return 'sz{}'.format(stock_code)


async def fetch(codes):
    url_fmt = 'http://hq.sinajs.cn/list={}'
    url = url_fmt.format(','.join(map(_add_prefix, codes)))
    async with aiohttp.request('GET', url) as r:
        data = await r.text()
    return data


async def to_dataframe(codes):
    """解析网页数据，返回DataFrame对象"""
    content = await fetch(codes)
    df = _to_dataframe(content)
    df = df.apply(_convert_to_numeric, exclude=('股票代码', '股票简称', '日期', '时间'))
    df = df[df.成交额 > 0]
    if len(df) > 0:
        df['时间'] = pd.to_datetime(df.日期 + ' ' + df.时间)
        del df['日期']
        return df
    return pd.DataFrame()


async def fetch_all(batch_num=800):
    """获取所有股票实时报价原始数据"""
    stock_codes = get_valid_codes()
    b_codes = loop_codes(stock_codes, batch_num)
    tasks = [to_dataframe(codes) for codes in b_codes]
    dfs = await asyncio.gather(
        *tasks
    )
    return pd.concat(dfs)


def part1():
    today = pd.Timestamp('today')
    if today.hour == 9 and today.minute < 30:
        df = asyncio.run(fetch_all())
        df = df.loc[df['时间'] >= today.normalize(), :]
        if len(df) > 0:
            engine = get_engine(db_dir_name)
            table_name = 'live_quotes'
            df.to_sql(table_name, engine,
                      index=False, if_exists='append')
            logger.info('添加{}行'.format(df.shape[0]))


def refresh_live_quote():
    """刷新实时报价"""
    # 早盘市场竞价在计划任务程序中，自9：15开始，连续执行15分钟
    part1()
    # 9：31分开始，连续2小时
    # 12：01开始，连续2小时
    today = pd.Timestamp('today')
    # 如果当日非交易日，直接返回
    if not need_refresh(today):
        return
    df = asyncio.run(fetch_all())
    if len(df) > 0:
        df = df.loc[df['时间'] >= today.normalize(), :]
        engine = get_engine(db_dir_name)
        table_name = 'live_quotes'
        df.to_sql(table_name, engine,
                  index=False, if_exists='append')
        logger.info('添加{}行'.format(df.shape[0]))
