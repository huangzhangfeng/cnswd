"""

深证信基础模块

"""
import math
import time
import os
import logbook
import pandas as pd
from logbook.more import ColorizedStderrHandler
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select, WebDriverWait

from . import ops
from .._selenium import make_headless_browser
from ._firefox import clear_firefox_cache
from .constants import TIMEOUT

HOME_URL_FMT = 'http://webapi.cninfo.com.cn/#/{}'

API_MAPS = {
    5:  ('个股API', 'dataDownload'),
    6:  ('行情中心', 'marketData'),
    7:  ('数据搜索', 'dataBrowse'),
    8:  ('专题统计', 'thematicStatistics'),
    9:  ('公司快照', 'company'),
    10:  ('公告定制', 'notice'),
}
# 轮询时间缩短
POLL_FREQUENCY = 0.2

# 设置显示日志
logbook.set_datetime_format('local')
handler = ColorizedStderrHandler()
handler.push_application()


class SZXPage(object):
    """深证信基础网页"""
    # 子类需重新定义
    current_level = ''  # 左侧菜单层级
    view_selection = {1: 10, 2: 20, 3: 50}
    page_num = 0
    level_maps = {}
    retry_times = 1

    def __init__(self, clear_cache, retry_times, **kwds):
        self.driver = make_headless_browser()
        self.logger = logbook.Logger("深证信")
        # 由于经常会遭遇到未知故障，需要清理缓存，提高成功加载的概率
        if clear_cache:
            clear_firefox_cache(self.driver)
        # 通用部分
        self.wait = WebDriverWait(
            self.driver, TIMEOUT, poll_frequency=POLL_FREQUENCY)
        self.retry_times = retry_times
        super().__init__(**kwds)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.driver.quit()

    def _load_page(self, ht):
        # 如果重复加载同一网址，耗时约为1ms
        url = HOME_URL_FMT.format(ht)
        self.driver.get(url)

    def _switch_to(self, num, check_loaded_css):
        """转移至顶部指定栏目，完成页面首次加载"""
        assert num in API_MAPS.keys(), '可接受范围{}'.format(API_MAPS)
        ht = API_MAPS[num][1]
        self.logger.info(f'{API_MAPS[num][0]}')
        self._load_page(ht)
        # 确保元素可见
        ops.wait_first_loaded(
            self.wait, check_loaded_css, API_MAPS[num][0])

    def _change_data_item(self, level):
        """更改数据项目相关设定"""
        # 更改项目会重新加载数据表所包含的字段。
        if level != self.current_level:
            # 滚动到页码顶部左侧位置
            self.driver.execute_script("window.scrollTo(0, 0);")
            # 注意操作顺序
            self._select_level(level)
            self.current_level = level
            # 改变层级后，才进行相关与层级相关的设定
            self._data_item_related(level)

    def _select_level(self, level):
        raise NotImplementedError('必须在子类完成选定数据项目')

    def _data_item_related(self, level):
        raise NotImplementedError('必须在子类完成与数据项目相关的设定')

    # ===============通用设置=============== #
    def _change_year(self, css_id, year):
        """改变查询指定id元素的年份"""
        js = 'document.getElementById("{}").value="{}";'.format(css_id, year)
        self.driver.execute_script(js)

    def _change_date(self, css, date_str):
        """设置日期"""
        elem = self.driver.find_element_by_css_selector(css)
        elem.clear()
        # 自动补全
        elem.send_keys(date_str, Keys.TAB)

    def _auto_change_view_row_num(self, total):
        """
        根据提取数据行数，自动调整到每页最大可显示行数
        
        Arguments:
            total {int} -- 提取的数据行数
        """
        min_row_num = min(self.view_selection.values())
        max_row_num = max(self.view_selection.values())

        if total <= min_row_num:
            nth = min(self.view_selection.keys())
            per_page = min(self.view_selection.values())
        elif total >= max_row_num:
            nth = max(self.view_selection.keys())
            per_page = max(self.view_selection.values())
        else:
            for k, v in self.view_selection.items():
                if total <= v:
                    nth = k
                    per_page = v
                    break

        def change_view_row_num():
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            css_1 = '.dropdown-toggle'
            self.driver.find_element_by_css_selector(css_1).click()
            css_2 = '.dropdown-menu > li:nth-child({})'.format(nth)
            self.driver.find_element_by_css_selector(css_2).click()

        # 只有总行数大于最小行数，才有必要调整显示行数
        if total > min_row_num:
            change_view_row_num()
        page_num = math.ceil(total / per_page)
        # 记录页数
        self.page_num = page_num
