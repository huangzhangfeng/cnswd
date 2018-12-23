"""
公司公告

来源：巨潮资讯(新版)
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import re
import time
import itertools
import logbook
import pandas as pd
from logbook.more import ColorizedStderrHandler
from selenium.common.exceptions import (
    ElementNotInteractableException,
    NoAlertPresentException,
    NoSuchElementException,
    TimeoutException
)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

from cnswd.websource.szx._selenium import make_headless_browser
from cnswd.websource.szx._firefox import clear_firefox_cache

PAT = re.compile(r'announcementId=(\d{10})&announcementTime=(\d{4}-\d{2}-\d{2})$')
START_DATE = pd.Timestamp('2010-01-01')
log = logbook.Logger('公司公告')


class DisclosureAPI(object):
    """巨潮咨询网公司公告"""

    def __init__(self):
        # 初始化无头浏览器
        self.browser = make_headless_browser()
        clear_firefox_cache(self.browser)
        self.current_plate = ''
        self.data = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.browser.quit()

    def _switch_to_list(self):
        """转换为列表查询"""
        css = 'a.sub-more:nth-child(4) > i:nth-child(1)'
        self.browser.find_element_by_css_selector(css).click()
        time.sleep(0.3)

    def _recent_3_year(self):
        """选取近三年"""
        self.browser.find_element_by_link_text('近三年').click()
        time.sleep(3)
        # self.browser.save_screenshot('view/3.png')

    def _pdf_url(self, announcementTime, announcementId):
        """pdf文件下载网址"""
        url_fmt = 'http://www.cninfo.com.cn/finalpage/{}/{}.PDF'
        return url_fmt.format(announcementTime, announcementId)

    def _read_page_table(self):
        """读取页数据"""
        tr_css = 'table.page-list-list:nth-child(1) > tbody:nth-child(1) > tr'
        # 含表头共31行
        trs = self.browser.find_elements_by_css_selector(tr_css)
        tab = []
        col_names = ['股票代码', '股票简称', '公告标题', '公告时间', '下载网址']
        for i in range(1, len(trs)):
            tr = trs[i]
            code = tr.find_element_by_class_name('sub-code').text
            short_name = tr.find_element_by_class_name('sub-name').text
            elem = tr.find_element_by_css_selector('td:nth-child(3) > a')
            title = elem.text
            href = elem.get_attribute('href')
            result = re.findall(PAT, href)
            announcementTime = result[0][1]
            announcementId = result[0][0]
            url = self._pdf_url(announcementTime, announcementId)
            tab.append((code, short_name, title, announcementTime, url))
        df = pd.DataFrame.from_records(tab)
        df.columns = col_names
        df['公告时间'] = pd.to_datetime(df['公告时间'], errors='coerce')
        return df

    def _to_page(self, page_num):
        """下一页"""
        self.browser.find_element_by_link_text(str(page_num)).click()
        time.sleep(0.5)

    def _read_all_pages(self):
        """读取全部页数据"""
        for i in itertools.count():
            try:
                df = self._read_page_table()
                log.info('{} 第{}页'.format(self.current_plate, i+1))
                self.data.append(df)
                self._to_page(i+2)
            except Exception:
                break

    def get_sse(self, get_history=False):
        """沪市(今日公告)"""
        self.data = []
        self.current_plate = '沪市主板'
        #url = 'http://three.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice-sse'
        url = 'http://www.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice-sse#'
        self.browser.get(url)
        time.sleep(0.5)
        self._switch_to_list()
        if get_history:
            self._recent_3_year()
        else:
            self.browser.find_element_by_link_text('今日').click()
        self._read_all_pages()
        return self.data

    def get_szse(self, get_history=False):
        """深主板(今日公告)"""
        self.data = []
        self.current_plate = '深市主板'
        #url = 'http://three.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice-szse-main'
        url = 'http://www.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice-sse#'
        self.browser.get(url)
        time.sleep(0.5)
        self._switch_to_list()
        if get_history:
            self._recent_3_year()
        else:
            self.browser.find_element_by_link_text('今日').click()
        self._read_all_pages()
        return self.data
