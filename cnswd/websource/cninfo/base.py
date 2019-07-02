"""

深证信基础模块

"""
import math
import os
import time
import re
import logbook
import pandas as pd
from logbook.more import ColorizedStderrHandler
from selenium.common.exceptions import ElementNotInteractableException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from cnswd.websource.exceptions import RetryException
from cnswd.websource._selenium import make_headless_browser
from cnswd.websource.cninfo._firefox import clear_firefox_cache
from cnswd.websource.cninfo.constants import TIMEOUT

HOME_URL_FMT = 'http://webapi.cninfo.com.cn/#/{}'
PAGINATION_PAT = re.compile(r'共\s(\d{1,})\s条记录')
API_MAPS = {
    5:   ('个股API', 'dataDownload'),
    6:   ('行情中心', 'marketData'),
    7:   ('数据搜索', 'dataBrowse'),
    8:   ('专题统计', 'thematicStatistics'),
    9:   ('公司快照', 'company'),
    10:  ('公告定制', 'notice'),
}

# 设置显示日志
logbook.set_datetime_format('local')
handler = ColorizedStderrHandler()
handler.push_application()


def _concat(dfs):
    try:
        # 务必维持原始列顺序
        return pd.concat(dfs, ignore_index=True)
    except ValueError:
        return pd.DataFrame()


class element_attribute_change_to(object):
    """An expectation for checking that an element has a particular css class.

    locator - used to find the element
    returns the WebElement once it's name has the particular attribute
    """

    def __init__(self, locator, name, attribute):
        self.locator = locator
        self.name = name
        self.attribute = attribute

    def __call__(self, driver):
        # Finding the referenced element
        element = driver.find_element(*self.locator)
        # 当指定名称的属性变更为指定属性时，返回该元素
        if self.attribute == element.get_attribute(self.name):
            return element
        else:
            return False


class SZXPage(object):
    """深证信基础网页"""

    # 变量
    code_loaded = False
    current_t1_value = ''      # 开始日期
    current_t2_value = ''      # 结束日期
    current_level = ''         # 左侧菜单层级

    # 字类需要改写的属性
    api_name = ''
    api_e_name = ''
    check_loaded_css = ''    # 以此元素是否显示为标准，检查页面是否正确加载
    name_map = {}
    css_map = {}
    date_map = {}
    level_input_css = ''
    level_query_bnt_css = ''
    preview_btn_css = ''       # 预览数据按钮
    wait_for_preview_css = ''  # 检验预览结果css
    view_selection = {}        # 可调显示行数 如 {1:10,2:20,3:50}

    def __init__(self, clear_cache=False):
        start = time.time()
        self.driver = make_headless_browser()
        self.logger = logbook.Logger("深证信")
        if clear_cache:
            clear_firefox_cache(self.driver)
            self.logger.notice("清理缓存")
        self.wait = WebDriverWait(self.driver, TIMEOUT)
        try:
            self._load_page()
        except Exception as e:
            self.logger.error(e)
            self.driver.implicitly_wait(1)
            self._load_page()
        self.logger.notice(f'加载主页用时：{(time.time() - start):>0.4f}秒')

    @property
    def current_t1_css(self):
        return self.css_map[self.current_level][0]

    @property
    def current_t2_css(self):
        return self.css_map[self.current_level][1]

    def reset(self):
        self.driver.quit()

        # 恢复变量默认值
        self.code_loaded = False
        self.current_t1_value = ''      # 开始日期
        self.current_t2_value = ''      # 结束日期

        start = time.time()
        self.driver = make_headless_browser()
        self.logger = logbook.Logger("深证信")
        clear_firefox_cache(self.driver)
        self.logger.notice("清理缓存")
        self.wait = WebDriverWait(self.driver, TIMEOUT)
        try:
            self._load_page()
        except Exception as e:
            self.logger.warning(e)
            # self.driver.implicitly_wait(1)
            self._load_page()
        self.logger.notice(f'重新加载主页用时：{(time.time() - start):>0.4f}秒')

    def _load_page(self):
        # 如果重复加载同一网址，耗时约为1ms
        self.logger.info(self.api_name)
        url = HOME_URL_FMT.format(self.api_e_name)
        self.driver.get(url)
        # 特定元素可见，完成首次页面加载
        self._wait_for_visibility(self.check_loaded_css, self.api_name)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.driver.quit()

    def __repr__(self):
        item = f"{self.name_map[self.current_level]}"
        start = self.current_t1_value
        end = self.current_t2_value
        if start and end:
            return f"{item} {start} ~ {end}"
        elif start and not end:
            return f"{item} {start}"
        else:
            return f"{item}"

    def _wait_for_activate(self, data_name, status='active'):
        """等待元素激活"""
        xpath_fmt = "//a[@data-name='{}']"
        locator = (By.XPATH, xpath_fmt.format(data_name))
        self.wait.until(element_attribute_change_to(
            locator, 'class', status), f'{self.api_name} {data_name} 激活元素超时')

    def _wait_for_preview(self, style='display: none;'):
        """等待预览结果完全呈现"""
        # # 以属性值改变来判断
        # locator = (By.CSS_SELECTOR, self.wait_for_preview_css)
        # self.wait.until(element_attribute_change_to(
        #     locator, 'style', style), f'{self.api_name} 等待预览结果超时')
        # 以加载指示元素不可见来判断
        self._wait_for_invisibility(
            self.wait_for_preview_css, f'{self.api_name} 等待预览结果超时')

    def _wait_for_invisibility(self, elem_css, msg=''):
        """
        等待指定css元素不可见

        Arguments:
            elem_css {str} -- 可见元素的css表达式
        """
        m = EC.invisibility_of_element((By.CSS_SELECTOR, elem_css))
        self.wait.until(m, message=msg)

    def _wait_for_visibility(self, elem_css, msg=''):
        """
        等待指定css元素可见

        Arguments:
            elem_css {str} -- 可见元素的css表达式
        """
        m = EC.visibility_of_element_located((By.CSS_SELECTOR, elem_css))
        self.wait.until(m, message=msg)

    def _wait_for_all_presence(self, elem_css, msg=''):
        """
        等待指定css的所有元素出现

        Arguments:
            elem_css {str} -- 元素的css表达式
        """
        m = EC.presence_of_all_elements_located((By.CSS_SELECTOR, elem_css))
        self.wait.until(m, message=msg)

    def _get_count_tip(self, span_css):
        """获取元素数量提示"""
        i = self.driver.find_element_by_css_selector(
            span_css).find_element_by_tag_name('i')
        try:
            return int(i.text)
        except:
            return 0

    def _toggler_open(self, elem):
        """展开元素"""
        assert elem.tag_name == 'li', '必须为"li"元素'
        attr = elem.get_attribute('class')
        if 'closed' in attr:
            elem.find_element_by_tag_name('span').click()

    def _toggler_close(self, elem):
        """折叠或隐藏元素"""
        assert elem.tag_name == 'li', '必须为"li"元素'
        attr = elem.get_attribute('class')
        if 'opened' in attr:
            elem.find_element_by_tag_name('span').click()

    def _add_or_delete_all(self, label_css, btn_css):
        """添加或删除所选全部元素"""
        # 点击全选元素
        self.driver.find_element_by_css_selector(label_css).click()
        # 点击命令按钮
        self.driver.find_element_by_css_selector(btn_css).click()

    def _query(self, input_text, input_css, query_bnt_css):
        """
        执行查询

        在指定`input_css`元素输入`input_text`，点击`query_bnt_css`
        执行查询
        """
        input_elem = self.driver.find_element_by_css_selector(input_css)
        input_elem.clear()
        input_elem.send_keys(input_text)
        self.driver.find_element_by_css_selector(query_bnt_css).click()

    def _select_level(self, level):
        msg = f'"{self.api_name}"指标导航可接受范围：{list(self.name_map.keys())}'
        assert level in self.name_map.keys(), msg
        input_text = self.name_map[level].lower()
        input_css = self.level_input_css
        query_bnt_css = self.level_query_bnt_css
        self._query(input_text, input_css, query_bnt_css)
        xpath_fmt = "//a[@data-name='{}']"
        self.driver.find_element_by_xpath(xpath_fmt.format(input_text)).click()
        self._wait_for_activate(input_text)

    def _no_data(self):
        """等待预览呈现后，首先需要检查查询是否无数据返回"""
        try:
            self.driver.find_element_by_css_selector('.no-records-found')
            return True
        except Exception:
            return False

    def _has_exception(self):
        """等待预览呈现后，尽管有数据返回，检查是否存在异常提示"""
        csss = ['.tips', '.cancel', '.timeout', '.sysbusy']
        for css in csss:
            try:
                elem = self.driver.find_element_by_css_selector(css)
                if elem.get_attribute('style') == 'display: inline;':
                    self.logger.notice(f"{elem.text}")
                    return True
            except Exception:
                return False

    def _get_row_num(self):
        """获取预览输出的总行数"""
        pagination_css = '.pagination-info'
        pagination = self.driver.find_element_by_css_selector(pagination_css)
        row_num = int(re.search(PAGINATION_PAT, pagination.text).group(1))
        return row_num

    def _get_pages(self):
        """获取预览输出的总页数"""
        # 如无法定位到`.page-last`元素，可能的情形
        # 1. 存在指示页数的li元素，倒数第2项的li元素text属性标示页数；
        # 2. 不存在li元素，意味着只有1页
        try:
            li_css = '.page-last'
            li = self.driver.find_element_by_css_selector(li_css)
            return int(li.text)
        except Exception:
            pass
        # 尝试寻找li元素
        try:
            li_css = 'ul.pagination li'
            lis = self.driver.find_elements_by_css_selector(li_css)
            return int(lis[-2].text)
        except Exception:
            return 1

    def _auto_change_view_row_num(self):
        """自动调整到每页最大可显示行数"""
        min_row_num = min(self.view_selection.values())
        max_row_num = max(self.view_selection.values())
        total = self._get_row_num()

        if total <= min_row_num:
            nth = min(self.view_selection.keys())
        elif total >= max_row_num:
            nth = max(self.view_selection.keys())
        else:
            for k, v in self.view_selection.items():
                if total <= v:
                    nth = k
                    break
        # 只有总行数大于最小行数，才有必要调整显示行数
        if total > min_row_num:
            # 点击触发可选项
            self.driver.find_element_by_css_selector(
                '.dropdown-toggle').click()
            locator = (By.CSS_SELECTOR, '.btn-group')
            self.wait.until(element_attribute_change_to(
                locator, 'class', 'btn-group dropup open'), '调整每页显示行数超时')
            css = '.btn-group > ul:nth-child(2) li'
            lis = self.driver.find_elements_by_css_selector(css)
            lis[nth-1].click()

    def _read_html_table(self):
        """读取当前网页数据表"""
        # 点击`预览数据`
        if self.api_e_name == 'thematicStatistics':
            # 专题统计中，12开头的数据项目无命令按钮
            if any(self.css_map[self.current_level]):
                # 预览数据
                self.driver.find_element_by_css_selector(
                    self.preview_btn_css).click()
            else:
                # 没有预览按钮时，等待一小段时间
                self.driver.implicitly_wait(0.3)
        else:
            # 预览数据
            self.driver.find_element_by_css_selector(
                self.preview_btn_css).click()
        # 等待预览数据完成加载。如数据量大，可能会比较耗时。最长约6秒。
        self._wait_for_preview()
        # 是否无数据返回
        if self._no_data():
            return pd.DataFrame()
        # 是否存在异常
        if self._has_exception():
            item = self.name_map[self.current_level]
            raise RetryException(f'项目：{item} 提取的网页数据不完整')
        # 自动调整显示行数，才读取页数
        self._auto_change_view_row_num()
        pages = self._get_pages()
        i_width = 14
        n_width = 5  # 最多为万
        dfs = []
        na_values = ['-', '无', ';']
        level = self.current_level
        if level:
            item = self.name_map[level]
        else:
            item = ''
        for i in range(pages):
            df = pd.read_html(self.driver.page_source, na_values=na_values)[0]
            dfs.append(df)
            # 点击进入下一页
            if i != (pages - 1):
                next_page = self.driver.find_element_by_link_text(str(i + 2))
                next_page.click()
            self.logger.info(f'>>{item:{i_width}} 第{i+1:{n_width}}页 / 共{pages:{n_width}}页')
        return _concat(dfs)

    def _change_year(self, css, year):
        """改变查询指定id元素的年份"""
        elem = self.driver.find_element_by_css_selector(css)
        elem.clear()
        elem.send_keys(str(year))

    def _datepicker(self, css, date_str):
        """指定日期"""
        elem = self.driver.find_element_by_css_selector(css)
        elem.clear()
        elem.send_keys(date_str, Keys.TAB)

    def _log_info(self, p, level, start, end):
        i_w = 14
        item = self.name_map[level]
        if pd.api.types.is_number(start):
            if pd.api.types.is_number(end):
                msg = f'{start}年{end}季度'
            else:
                if end:
                    msg = f'{start}年 ~ {end}'
                else:
                    msg = f'{start}年'
        else:
            if start:
                msg = pd.Timestamp(start).strftime(r'%Y-%m-%d')
                if end:
                    msg += f" ~ {pd.Timestamp(end).strftime(r'%Y-%m-%d')}"
                else:
                    msg = f"{pd.Timestamp(start).strftime(r'%Y-%m-%d')} ~ 至今"
            else:
                msg = ''
        self.logger.info(f"{p}{item:{i_w}} {msg}")

    def scroll(self, size):
        """
        上下滚动到指定位置

        参数:
        ----
        size: float, 屏幕自上而下的比率
        """
        # 留存
        # 滚动到屏幕底部
        # self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        h = self.driver.get_window_size()['height']
        js = f"var q=document.documentElement.scrollTop={int(size * h)}"
        self.driver.execute_script(js)
