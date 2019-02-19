"""
命令行

用法
# 创建数据库
$ stock create --db_dir_name=cn
# 刷新股票日线数据
$ stock stock-daily

建议使用后台任务计划，自动刷新数据。参考`cnswd/regular_tasks/bg_tasks.cron`说明

深证信凌晨时段比较可靠。

"""
from __future__ import absolute_import, division, print_function

import asyncio

import click
import logbook
import pandas as pd
from logbook.more import ColorizedStderrHandler

from .db.core import refresh_data, refresh_info, update_stock_classify
from .infoes.disclosures import init_disclosure, refresh_disclosure
from .infoes.news import append_historical_news, refresh_news
from .szsh import cjmx
from .szsh.index_daily import flush_index_daily
from .szsh.init_stock_daily import init_stock_daily_data
from .szsh.quote import refresh_live_quote
from .szsh.stock_daily import refresh_daily
from .szsh.stock_info import refresh as refresh_szsh_stock_info
from .szsh.tct_gn import refresh as tct_gn_refresh
from .szsh.ths_gn import update_gn_list
from .szsh.trading_calendar import update_trading_calendars
from .szsh.treasury import refresh_treasury
from .ts.core import refresh as ts_refresh
from .utils import create_tables, remove_temp_files

logbook.set_datetime_format('local')
handler = ColorizedStderrHandler()
handler.push_application()


MESSAGE = """
选择yes将删除所有数据表，重新建立数据库
    如果数据混乱，可选择删除
选择no保留原有数据，但会创建原数据库不存在的表
    1. 初始化数据中断，需要继续初始化；
    2. 需要新添加表，同时保留原数据；
"""

# ====================基本操作==================== #


@click.group()
def stock():
    pass


@stock.command()
@click.option('--db_dir_name', type=click.Choice(['dataBrowse', 'info', 'szsh', 'thematicStatistics']), help='数据库目录名称')
@click.option('--rewrite/--no-rewrite', default=False, help='是否重写数据库')
def create(db_dir_name, rewrite):
    """创建数据表"""
    if rewrite:
        click.secho(MESSAGE, fg='red')
        if click.confirm('删除原数据库，然后重写？'):
            create_tables(db_dir_name, True)
    else:
        create_tables(db_dir_name, False)


@stock.command()
def remove():
    """删除临时文件"""
    remove_temp_files()


# ====================专题统计数据库==================== #

@stock.command()
@click.argument('levels', nargs=-1)
def ts_data(levels):
    """刷新股票数据"""
    ts_refresh(levels)


# ====================数据搜索数据库==================== #
@stock.command()
@click.option('--codes', default=None, help='要刷新的股票代码')
@click.option('--update/--no-update', default=False, help='是否更新股票信息或IPO')
@click.option('--retry', default=3, help='尝试次数')
def db_info(codes, update, retry):
    """刷新股票基础信息"""
    refresh_info(codes, update, retry)


@stock.command()
@click.option('--codes', default=None, help='要刷新的股票代码')
@click.option('--retry', default=3, help='尝试次数')
@click.argument('levels', nargs=-1)
def db_data(codes, levels, retry):
    """刷新股票项目数据"""
    refresh_data(codes, levels, retry)


@stock.command()
def db_classify():
    """刷新股票分类信息"""
    update_stock_classify()


# ====================INFO数据库==================== #


@stock.command()
@click.option('--times', default=10000, help='翻页次数')
def info_news_init(times):
    """初始化新浪财经消息"""
    append_historical_news(times)


@stock.command()
def info_news():
    """新浪财经消息"""
    refresh_news()


@stock.command()
def info_disclosure_init():
    """初始化2010年至今的公司公告"""
    asyncio.run(init_disclosure())


@stock.command()
def info_disclosure():
    """刷新公司公告"""
    async def main(): return await refresh_disclosure()
    asyncio.run(main())


# ====================SZSH数据库==================== #


@stock.command()
def szsh_stock_info():
    """刷新上交所及深交所股票基本信息"""
    refresh_szsh_stock_info()


@stock.command()
def szsh_trading_calendar():
    """交易日历"""
    update_trading_calendars()


@stock.command()
def szsh_stock_daily():
    """股票日线数据"""
    refresh_daily()


@stock.command()
def szsh_stock_daily_init():
    """初始化所有股票日线数据(含已经退市股票)"""
    init_stock_daily_data()


@stock.command()
@click.option('--date', default=None, help='刷新日期')
def szsh_cjmx(date):
    """刷新指定日期的股票成交明细"""
    if date is None:
        today = pd.Timestamp('today')
        if today.hour <= 18:
            input_date = pd.Timestamp('today')-pd.Timedelta(days=1)
        else:
            input_date = today
    else:
        input_date = date
    cjmx.wy_refresh_cjmx(input_date)


# @stock.command()
# def szsh_cjmx_init():
#     """初始化成交明细"""
#     init_cjmx()


@stock.command()
def szsh_quote():
    """股票实时报价"""
    refresh_live_quote()


@stock.command()
def szsh_treasury():
    """刷新国库券利率数据"""
    refresh_treasury()


@stock.command()
def szsh_tct_gn():
    """刷新腾讯概念股票列表"""
    tct_gn_refresh()


@stock.command()
def szsh_main_index_daily():
    """刷新主要指数日线数据"""
    flush_index_daily()
