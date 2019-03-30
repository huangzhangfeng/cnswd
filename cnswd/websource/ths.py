"""

同花顺数据模块

ubuntun操作系统中selenium配置过程
# 安装geckodriver
1. 下载geckodriver对应版本 网址：https://github.com/mozilla/geckodriver/releases
2. 解压：tar -xvzf geckodriver*
3. $sudo mv ./geckodriver /usr/bin/geckodriver
"""
from __future__ import absolute_import, division, print_function

import random
import time

import logbook
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from ._selenium import make_headless_browser
from .szx._firefox import clear_firefox_cache

log = logbook.Logger('同花顺')


class THS(object):
    """同花顺网页信息api"""

    def __init__(self):
        self.browser = make_headless_browser()
        clear_firefox_cache(self.browser)
        log.info('清理缓存')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.browser.quit()

    def _get_page_num(self):
        """当前页数"""
        try:
            p = self.browser.find_element_by_css_selector(
                '.page_info').text.split('/')[1]
            return int(p)
        except Exception:
            return 1

    def get_gn_page_num(self, gn_code):
        """概念分页数量"""
        info_fmt = 'http://q.10jqka.com.cn/gn/detail/code/{}/'
        info_url = info_fmt.format(gn_code)
        self.browser.get(info_url)
        return self._get_page_num()

    def get_gn_detail(self, gn_code, page_no):
        """获取股票概念编码第N页成分表"""
        url_fmt = 'http://q.10jqka.com.cn/gn/detail/order/desc/page/{page}/ajax/1/code/{gn}'
        url = url_fmt.format(page=page_no, gn=gn_code)
        self.browser.get(url)
        df = pd.read_html(self.browser.page_source,
                          attrs={'class': 'm-table m-pager-table'})[0]
        df['概念编码'] = gn_code
        df['股票代码'] = df.代码.map(lambda x: str(x).zfill(6))
        return df.loc[:, ['概念编码', '股票代码']]

    @property
    def gn_urls(self):
        """股票概念网址列表"""
        url = 'http://q.10jqka.com.cn/gn/'
        self.browser.get(url)
        self.browser.find_element_by_css_selector('.cate_toggle').click()
        time.sleep(0.5)
        url_css = '.category a'
        info = self.browser.find_elements_by_css_selector(url_css)
        res = []
        for a in info:
            res.append((a.get_attribute('href'), a.text))
        return res

    def _gn_times(self, page):
        na_values = ['--', '无']
        url_fmt = 'http://q.10jqka.com.cn/gn/index/field/addtime/order/desc/page/{}/ajax/1/'
        url = url_fmt.format(page)
        self.browser.get(url)
        df = pd.read_html(self.browser.page_source, na_values=na_values)[0]
        log.info('读取第{}页数据'.format(page))
        return df

    @property
    def gn_times(self):
        """股票概念概述列表"""
        url = 'http://q.10jqka.com.cn/gn/'
        self.browser.get(url)
        page_num = int(self.browser.find_element_by_css_selector(
            '.page_info').text.split('/')[1])
        dfs = []
        status = {}
        for t in range(40):
            for i in range(page_num):
                page = i+1
                ok = status.get(page)
                if ok:
                    continue
                try:
                    df = self._gn_times(page)
                    dfs.append(df)
                    status[page] = True
                except:
                    status[page] = False
                    time.sleep(0.5)
                    log.info('第{}次尝试 第{}页'.format(t+1, page))
        res = pd.concat(dfs)
        res.columns = ['日期', '概念名称', '驱动事件', '龙头股', '成分股数量']
        return res
