from __future__ import absolute_import

import random
import time

import logbook
import pandas as pd
import requests

from cnswd.websource.base import friendly_download
from cnswd.websource._selenium import make_headless_browser

log = logbook.Logger('提取成交明细网页数据')

BASE_URL_FMT = 'http://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradehistory.php?symbol={symbol}&date={date_str}'
DATE_FMT = '%Y-%-m-%-d'  # 不填充0


def _add_prefix(stock_code):
    """查询代码"""
    pre = stock_code[0]
    if pre == '6':
        return 'sh{}'.format(stock_code)
    else:
        return 'sz{}'.format(stock_code)


def _to_str(date):
    """转换为查询日期格式"""
    # Visual Studio 不能直接处理
    # return pd.Timestamp(date).strftime(DATE_FMT)
    dt_stru = pd.Timestamp(date).timetuple()
    return str(dt_stru.tm_year) + '-' + str(dt_stru.tm_mon) + '-' + str(dt_stru.tm_mday)


def _query_url(code, date):
    """查询url"""
    symbol = _add_prefix(code)
    date_str = _to_str(date)
    return BASE_URL_FMT.format(symbol=symbol, date_str=date_str)


def _fix_data(df, code, date):
    """整理数据框"""
    df.columns = ['成交时间', '成交价', '价格变动', '成交量', '成交额', '性质']
    date_str = _to_str(date)
    df.成交时间 = df.成交时间.map(lambda x: pd.Timestamp('{} {}'.format(date_str, x)))
    df['股票代码'] = code
    # df['涨跌幅'] = df['涨跌幅'].str.replace('%', '').astype(float) * 0.01
    df['成交量'] = df['成交量'] * 100
    df = df.sort_values('成交时间')
    return df


def _get_cjmx_1(code, date):
    url_fmt = 'http://vip.stock.finance.sina.com.cn/quotes_service/view/vMS_tradehistory.php?symbol={symbol_}&date={date_str}&page={page}'
    dfs = []
    symbol_ = _add_prefix(code)
    d = pd.Timestamp(date)
    if d < pd.Timestamp('today').normalize() - pd.Timedelta(days=20):
        raise NotImplementedError('尚未完成')
    for i in range(1, 1000):
        url = url_fmt.format(symbol_=symbol_, date_str=d.strftime(r'%Y-%m-%d'), page=i)
        r = requests.get(url)
        r.encoding = 'gb18030'
        # 当天不交易时，返回空`DataFrame`对象
        try:
            df = pd.read_html(r.text, attrs={'id': 'datatbl'}, na_values=['--'])[0]
        except ValueError:
            return pd.DataFrame()
        if '没有交易数据' in df.iat[0, 0]:
            df = pd.DataFrame()
            break
        dfs.append(df)
    df = pd.concat(dfs)
    del df['涨跌幅']
    return _fix_data(df, code, date)


def _get_cjmx_2(browser, code, date, page_sleep=0.2):
    """获取指定日期股票历史成交明细"""
    url = _query_url(code, date)
    browser.get(url)
    # time.sleep(0.3)
    # 主页信息
    # 如果反馈信息有提示，代表当日没有数据，否则提示为''
    msg = browser.find_element_by_css_selector('.msg').text  # 所选日期非交易日，请重新选择
    if msg != '':
        return pd.DataFrame()

    # 以下在子框架内操作
    browser.switch_to.frame('list_frame')
    if '输入的代码有误或没有交易数据' in browser.page_source:
        return pd.DataFrame()
    dfs = []

    # 排除第一项div元素
    num = len(browser.find_elements_by_css_selector('.pages > div')) - 1

    ## 然后移动时间线
    css_fmt = '.pages > div:nth-child({}) > a'
    for i in range(num):
        css = css_fmt.format(i + 2)
        target = browser.find_element_by_css_selector(css)
        browser.execute_script("arguments[0].click();", target)
        time.sleep(page_sleep)  # 休眠时间取决于本机环境
        df = pd.read_html(browser.page_source, attrs={
                          'id': 'datatbl'}, na_values=['--'])[0]
        dfs.append(df)
    df = pd.concat(dfs)

    return _fix_data(df, code, date)


@friendly_download(10)
def get_cjmx(code, date, browser=None):
    """获取股票指定日期成交明细"""
    d = pd.Timestamp(date)
    if d >= pd.Timestamp('today').normalize() - pd.Timedelta(days=20):
        return _get_cjmx_1(code, date)
    if browser is None:
        browser = make_headless_browser()
        df = _get_cjmx_2(browser, code, date)
        browser.quit()
        return df
    else:
        return _get_cjmx_2(browser, code, date)