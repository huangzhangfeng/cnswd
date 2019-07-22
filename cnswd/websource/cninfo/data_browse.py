"""

深证信数据搜索API

"""
import re
import time
from functools import lru_cache
import math
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

from cnswd.utils import (ensure_list, get_exchange_from_code, loop_codes,
                         loop_period_by, sanitize_dates)
from cnswd.websource.exceptions import RetryException
from selenium.common.exceptions import TimeoutException
from cnswd.websource.cninfo.base import SZXPage, _concat
from .constants import DB_NAME, DB_CSS, DB_DATE_FREQ

BATCH_CODE_NUM = 50
CLASS_ID = re.compile(r'=(\d{6})&')
PLATE_MAPS = {
    '137001': '市场分类',
    '137002': '证监会行业分类',
    '137003': '国证行业分类',
    '137004': '申万行业分类',
    '137006': '地区分类',
    '137007': '指数分类',
}


class DataBrowse(SZXPage):
    """数据搜索页"""
    # 变量
    code_loaded = False
    current_codes = ()         # 当前已选代码
    current_t1_value = ''      # 开始日期
    current_t2_value = ''      # 结束日期

    # 改写的属性
    preview_btn_css = '.dataBrowseBtn'
    wait_for_preview_css = '.onloading'
    view_selection = {1: 10, 2: 20, 3: 50}
    name_map = DB_NAME
    css_map = DB_CSS
    date_map = DB_DATE_FREQ
    api_name = '数据搜索'
    api_e_name = 'dataBrowse'
    check_loaded_css = '#apiName'  # 以此元素是否显示为标准，检查页面是否正确加载
    level_input_css = '.api-search-left > input:nth-child(1)'
    level_query_bnt_css = '.api-search-left > i:nth-child(2)'

    def _select_all_fields(self):
        """全选字段"""
        field_label_css = 'div.select-box:nth-child(2) > div:nth-child(1) > label:nth-child(1)'
        field_btn_css = 'div.arrows-box:nth-child(3) > div:nth-child(1) > button:nth-child(1)'
        # 全选数据字段
        self._add_or_delete_all(field_label_css, field_btn_css)

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
            self._select_all_fields()

    def load_all_code(self):
        """全选股票代码"""
        if self.code_loaded:
            return
        markets = ['深市A', '深市B', '沪市A', '沪市B', '科创板']
        market_cate_css = '.classify-tree > li:nth-child(6)'
        self._wait_for_visibility(market_cate_css)
        li = self.driver.find_element_by_css_selector(market_cate_css)
        self._toggler_open(li)
        xpath_fmt = "//a[@data-name='{}']"
        to_select_css = 'div.select-box:nth-child(1) > div:nth-child(3) > ul:nth-child(1) li'
        add_label_css = 'div.select-box:nth-child(1) > div:nth-child(1) > label:nth-child(1)'
        add_btn_css = 'div.arrows-box:nth-child(2) > div:nth-child(1) > button:nth-child(1)'
        for market in markets:
            self.logger.info(f"加载{market}代码")
            # 选择市场分类
            self.driver.find_element_by_xpath(xpath_fmt.format(market)).click()
            self._wait_for_activate(market)
            # 等待加载代码
            self._wait_for_all_presence(to_select_css, '加载市场分类代码')
            # 全部添加
            self._add_or_delete_all(add_label_css, add_btn_css)
            # self.driver.save_screenshot(f'{market}.png')
        self.code_loaded = True

    @property
    def stock_code_list(self):
        """所有股票代码列表"""
        if not self.code_loaded:
            self.load_all_code()
        selected_css = 'div.select-box:nth-child(3) > div:nth-child(2) > ul:nth-child(1) span'
        spans = self.driver.find_elements_by_css_selector(selected_css)
        return [span.get_attribute('data-id') for span in spans]

    def _clear_selected_codes(self):
        """清除已选代码，置放于待选区"""
        # 首先判断数量，如无则跳出
        selected_css = 'div.select-box:nth-child(3) > div:nth-child(2) > ul:nth-child(1) span'
        # 如有选定代码，则放于待选区
        if len(self.driver.find_elements_by_css_selector(selected_css)) >= 1:
            label_css = 'div.select-box:nth-child(3) > div:nth-child(1) > label:nth-child(1)'
            btn_css = 'div.arrows-box:nth-child(2) > div:nth-child(1) > button:nth-child(2)'
            self._add_or_delete_all(label_css, btn_css)

    def _add_codes(self, codes):
        """从待选区选中代码"""
        if not self.code_loaded:
            self.load_all_code()
        self._clear_selected_codes()
        unselect_css_fmt = "div.select-box:nth-child(1) > div:nth-child(3) > ul:nth-child(1) span[data-id='{}']"
        for code in codes:
            elem = self.driver.find_element_by_css_selector(
                unselect_css_fmt.format(code))
            elem.click()
        btn_css = 'div.arrows-box:nth-child(2) > div:nth-child(1) > button:nth-child(1)'
        self.driver.find_element_by_css_selector(btn_css).click()

    def add_codes(self, codes):
        if self.current_codes != tuple(codes):
            self._add_codes(codes)
            # 保存当前已选代码
            self.current_codes = tuple(codes)

    def set_t1_value(self, t1):
        """更改查询t1值"""
        if self.current_t1_css and (t1 != self.current_t1_value):
            # 输入日期字符串时
            if 'input' in self.current_t1_css:
                self._datepicker(self.current_t1_css, t1)
                self.current_t1_value = t1
            elif self.current_t1_css in ('#se1_sele', '#se2_sele'):
                self._change_year(self.current_t1_css, t1)
                self.current_t1_value = t1

    def set_t2_value(self, t2):
        """更改查询t2值"""
        if self.current_t2_css and (t2 != self.current_t2_value):
            if 'input' in self.current_t2_css:
                self._datepicker(self.current_t2_css, t2)
                self.current_t2_value = t2
            elif self.current_t2_css.startswith('.condition2'):
                elem = self.driver.find_element_by_css_selector(
                    self.current_t2_css)
                t2 = int(t2)
                assert t2 in (1, 2, 3, 4), '季度有效值为(1,2,3,4)'
                select = Select(elem)
                select.select_by_index(t2 - 1)
                # self.driver.save_screenshot(f'季度{t2}.png')
                self.current_t2_value = t2

    def _get_data_by_batch_codes(self):
        """所有代码分批获取数据"""
        codes = self.stock_code_list
        batch = math.ceil(len(codes) / BATCH_CODE_NUM)
        dfs = []
        for i in range(batch):
            batch_codes = codes[i*BATCH_CODE_NUM:i *
                                BATCH_CODE_NUM+BATCH_CODE_NUM]
            self.add_codes(batch_codes)
            msg = f">>> {batch_codes[0]} ~ {batch_codes[-1]} 共{len(batch_codes)}只"
            self.logger.info(msg)
            dfs.append(self._read_html_table())
        return _concat(dfs)

    def _get_data(self, level, t1, t2):
        """读取项目数据"""
        self.set_t1_value(t1)
        self.set_t2_value(t2)
        # 当可以一次性读取全部股票时
        return self._read_html_table()
        # 无法一次性读取时，采用分批代码读取
        # return self._get_data_by_batch_codes()

    def _loop_by_period(self, level, start, end):
        """分时期段读取数据"""
        loop_str = self.date_map[level][0]
        include = self.date_map[level][1]
        if loop_str is None:
            return self._get_data(level, None, None)
        # 第一个字符指示循环周期freq
        freq = loop_str[0]
        # 第二个字符指示值的表达格式
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
        for s, e in ps:
            t1, t2 = t1_fmt_func(s), t2_fmt_func(e)
            self.logger.info(f'>  时段 {t1} ~ {t2}')
            df = self._get_data(level, t1, t2)
            dfs.append(df)
        return _concat(dfs)

    def get_data(self, level, start=None, end=None):
        """获取项目数据

        Arguments:
            level {str} -- 项目层级

        Keyword Arguments:
            start {str} -- 开始日期 (default: {None})，如为空，使用市场开始日期
            end {str} -- 结束日期 (default: {None})，如为空，使用当前日期


        Usage:
            >>> api = DataBrowse()
            >>> api.get_data('4.1',2018-01-01','2018-08-01')
            >>> api.driver.quit()

        Returns:
            pd.DataFrame -- 如期间没有数据，返回长度为0的空表
        """
        self.select_level(level)
        self.load_all_code()
        self._log_info('==> ', level, start, end, " <==")
        return self._loop_by_period(level, start, end)

    @property
    def is_available(self):
        """
        API可用状态

        数据搜索经常会出现异常。
        判定标准为加载全部代码，读取基本资料信息。如行数与股票代码数量一致，表明正常；否则异常。

        如果此时状态异常，很容易丢失数据，导致数据不完整。须立即停止刷新。
        """
        level = '1.1'
        self.select_level(level)  # 使用`select_level`方法
        self.load_all_code()
        span_css = 'div.select-box:nth-child(3) > div:nth-child(1) > span:nth-child(2)'
        expected = self._get_count_tip(span_css)
        # # 选择单一字段，加快检查状态速度
        # field_css = 'div.select-box:nth-child(2) > div:nth-child(3) > ul:nth-child(1) > li:nth-child(1) > label:nth-child(1) > span:nth-child(3)'
        # self.driver.find_element_by_css_selector(field_css).click()
        # add_btn_css = 'div.arrows-box:nth-child(3) > div:nth-child(1) > button:nth-child(1)'
        # self.driver.find_element_by_css_selector(add_btn_css).click()
        # 预览数据
        self.driver.find_element_by_css_selector(self.preview_btn_css).click()
        self._wait_for_preview()
        actual = self._get_row_num()
        return expected == actual

    def get_levels_for(self, nth=3):
        """获取第nth种类别的分类层级"""
        res = []

        def get_children_level(li, level):
            """获取元素子li元素层级"""
            sub_lis = li.find_elements_by_xpath('ul/li')
            for i in range(len(sub_lis)):
                yield (sub_lis[i], '{}.{}'.format(level, i+1))

        def get_all_children(li, level):
            # 对于给定的li元素，递归循环至没有子级别的li元素
            if len(li.find_elements_by_xpath('ul/li')):
                for e, l in get_children_level(li, level):
                    res.append((e, l))
                    get_all_children(e, l)

        tree_css = '.classify-tree > li:nth-child({})'.format(nth)
        li = self.driver.find_element_by_css_selector(tree_css)
        get_all_children(li, nth)
        return [x for _, x in res]

    def get_total_classify_levels(self):
        """获取分类树编码"""
        levels = []
        for nth in (1, 2, 3, 4, 5, 6):
            levels.extend(self.get_levels_for(nth))
        return levels

    @property
    def classify_bom(self):
        roots = self.driver.find_elements_by_css_selector(
            '.classify-tree > li')
        items = []
        for r in roots:
            # 需要全部级别的分类编码名称
            items.extend(r.find_elements_by_tag_name('span'))
            items.extend(r.find_elements_by_tag_name('a'))
        data = []
        attrs = ('data-id', 'data-name')
        for item in items:
            data.append([item.get_attribute(name) for name in attrs])
        df = pd.DataFrame.from_records(data, columns=['分类编码', '分类名称'])
        return df.dropna().drop_duplicates(['分类编码', '分类名称'])

    def _construct_css(self, level):
        nums = level.split('.')
        head = f'.classify-tree > li:nth-child({nums[0]})'
        if len(nums) == 1:
            return head
        rest = ['ul:nth-child(2) > li:nth-child({})'.format(x)
                for x in nums[1:]]
        return ' > '.join([head] + rest)

    def _read_classify(self):
        """读取当前分类的股票代码表"""
        res = []
        span_css = 'div.select-box:nth-child(1) > div:nth-child(3) > ul:nth-child(1) span'
        for s in self.driver.find_elements_by_css_selector(span_css):
            res.append([s.get_attribute('data-id'),
                        s.get_attribute('data-name')])
        return pd.DataFrame.from_records(res, columns=['证券代码', '证券简称'])

    def _get_classify_tree(self, level):
        """获取层级股票列表"""
        df = None
        self.logger.info(f'选中分类层级：{level}')
        cum_level = []
        span_css = 'div.select-box:nth-child(1) > div:nth-child(3) > ul:nth-child(1) span'
        for l in level.split('.'):
            cum_level.append(l)
            css = self._construct_css('.'.join(cum_level))
            li = self.driver.find_element_by_css_selector(css)
            self._toggler_open(li)
            if li.get_attribute('class') == 'tree-empty':
                # 点击加载分类项下股票代码
                a = li.find_element_by_tag_name('a')
                name, code, platetype = self._parse_classify_info(a)
                a.click()
                try:
                    # 等待全部代码加载完毕
                    self._wait_for_all_presence(span_css)
                    # 此时读取数据
                    # self.driver.save_screenshot(f"{'.'.join(cum_level)}.png")
                    df = self._read_classify()
                    df['分类层级'] = level
                    df['分类名称'] = name
                    df['分类编码'] = code
                    df['平台类别'] = platetype
                except Exception:
                    # 由于`CDR`或者科创板没有数据，引发超时异常，忽略
                    pass
        return df

    def get_classify_tree(self, n):
        """获取分类树层级下的股票列表"""
        levels = self.get_levels_for(n)
        res = [self._get_classify_tree(level) for level in levels]
        df = _concat(res)
        return df

    def _parse_classify_info(self, a):
        """解析分类树基础信息"""
        name = a.get_attribute('data-name')
        code = a.get_attribute('data-id')
        param = a.get_attribute('data-param')
        platetype = None
        if param:
            m = re.search(CLASS_ID, param)
            if m:
                platetype = PLATE_MAPS[m.group(1)]
        return (name, code, platetype)
