"""

深证信专题统计模块

作为数据提取工具，专题统计的大部分项目没有实际意义，只是对数据进行的统计加工。
有效栏目如`股票状态`等数据搜索未出现的部分
"""
import re
import time
import numpy as np
import pandas as pd
from cnswd.utils import loop_period_by
from selenium.common.exceptions import (ElementNotInteractableException,
                                        NoSuchElementException,
                                        StaleElementReferenceException,
                                        TimeoutException)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from .constants import TS_NAME, TS_CSS, TS_DATE_FREQ
from .base import SZXPage, _concat


class ThematicStatistics(SZXPage):
    """深证信专题统计api"""
    current_t1_value = ''      # 开始日期
    current_t2_value = ''      # 结束日期

    # 改写的属性
    preview_btn_css = '.thematicStatisticsBtn'
    wait_for_preview_css = '.fixed-table-loading' # '.fixed-table-header'
    view_selection = {1: 20, 2: 50, 3: 100, 4: 200}
    name_map = TS_NAME
    css_map = TS_CSS
    date_map = TS_DATE_FREQ
    api_name = '专题统计'
    api_e_name = 'thematicStatistics'
    check_loaded_css = '#apiName2'  # 以此元素是否显示为标准，检查页面是否正确加载
    level_input_css = '.api-search-left > input:nth-child(1)'
    level_query_bnt_css = '.api-search-left > i:nth-child(2)'

    def select_level(self, level):
        """
        设定数据项目

        备注：
            选定左侧数据项目后，同时执行以下操作：
            1. 全选数据字段
            2. 设定相应的日期css
        """
        if self.current_level != level:
            self._select_level(level)
            # self.driver.save_screenshot(f'{level}.png')
            self.current_level = level
            # 专题统计还需要等待日期输入元素呈现
            if self.current_t1_css:
                self._wait_for_visibility(self.current_t1_css)


    def set_t1_value(self, t1):
        """更改查询t1值"""
        # 因为更改项目后，t值清零。所以无论是否更改前值，都需要重置。
        if self.current_t1_css:
            # 输入日期字符串时
            if 'input' in self.current_t1_css:
                self._datepicker(self.current_t1_css, t1)
                self.current_t1_value = t1
            elif 'sele' in self.current_t1_css:
                self._change_year(self.current_t1_css, t1)
                self.current_t1_value = t1

    def set_t2_value(self, t2):
        """更改查询t2值"""
        # 因为更改项目后，t值清零。所以无论是否更改前值，都需要重置。
        if self.current_t2_css:
            if 'input' in self.current_t2_css:
                self._datepicker(self.current_t2_css, t2)
                self.current_t2_value = t2
            elif self.current_t2_css.startswith('.condition2'):
                elem = self.driver.find_element_by_css_selector(
                    self.current_t2_css)
                t2 = int(t2)
                assert t2 in (1, 2, 3, 4), '季度有效值为(1,2,3,4)'
                select = Select(elem)
                # 序列一致
                select.select_by_index(t2 - 1)
                self.current_t2_value = t2

    def _get_data(self, level, t1, t2):
        """读取项目数据"""
        self.set_t1_value(t1)
        self.set_t2_value(t2)
        return self._loop_options(level)

    def _loop_by_period(self, level, start, end):
        # 循环指示字符位于第5项
        loop_str = self.date_map[level][0]
        # 排除未来日期指示在第6项
        include = self.date_map[level][1]
        if loop_str is None:
            return self._get_data(level, None, None)
        freq = loop_str[0]
        fmt_str = loop_str[1]
        if fmt_str in ('B', 'D', 'W', 'M'):
            def t1_fmt_func(x): return x.strftime(r'%Y-%m-%d')
            def t2_fmt_func(x): return x.strftime(r'%Y-%m-%d')
        elif fmt_str == 'Q':
            def t1_fmt_func(x): return x.year
            def t2_fmt_func(x): return x.quarter
        elif fmt_str == 'Y':
            def t1_fmt_func(x): return x.year
            def t2_fmt_func(x): return None
        else:
            raise ValueError(f'{loop_str}为错误格式。')
        ps = loop_period_by(start, end, freq, include)
        dfs = []
        for i, (s, e) in enumerate(ps, 1):
            t1, t2 = t1_fmt_func(s), t2_fmt_func(e)
            self._log_info('>', level, t1, t2)
            df = self._get_data(level, t1, t2)
            dfs.append(df)
            if i % 10 == 0:
                time.sleep(np.random.random())
        return _concat(dfs)

    def get_data(self, level, start=None, end=None):
        """获取项目数据

        Arguments:
            level {str} -- 项目层级

        Keyword Arguments:
            start {str} -- 开始日期 (default: {None})，如为空，使用市场开始日期
            end {str} -- 结束日期 (default: {None})，如为空，使用当前日期


        Usage:
            >>> api = WebApi()
            >>> api.get_data('4.1',2018-01-01','2018-08-01')

        Returns:
            pd.DataFrame -- 如期间没有数据，返回长度为0的空表
        """
        self.select_level(level)
        if self.current_t1_css:
            # 由于专题统计会预先加载默认数据，需要等待日期元素可见后，才可执行下一步
            self._wait_for_visibility(self.current_t1_css, self.api_name)
        df = self._loop_by_period(level, start, end)
        return df

    def _loop_options(self, level):
        """循环读取所有可选项目数据"""
        # 第三项为选项css
        css = self.css_map[level][2]
        if css is None:
            return self._read_html_table()
        label_css = css.split('>')[0] + ' > label:nth-child(1)'
        label = self.driver.find_element_by_css_selector(label_css)
        if label.text in ('交易市场','控制类型'):
            return self._read_all_option(css)
        else:
            return self._read_one_by_one(css)

    def _read_one_by_one(self, css):
        """逐项读取选项数据"""
        dfs = []
        elem = self.driver.find_element_by_css_selector(css)
        select = Select(elem)
        options = select.options
        for o in options:
            self.logger.info(f'{o.text}')
            select.select_by_visible_text(o.text)
            df = self._read_html_table()
            dfs.append(df)
        return _concat(dfs)

    def _read_all_option(self, css):
        """读取`全部`选项数据"""
        elem = self.driver.find_element_by_css_selector(css)
        select = Select(elem)
        select.select_by_value("")
        return self._read_html_table()

    @property
    def is_available(self):
        """
        故障概率低，返回True
        """
        return True

    def _wait_for_preview(self):
        """等待预览结果完全呈现"""
        # 与数据搜索有差异
        css = '#contentTable'
        self._wait_for_visibility(css)