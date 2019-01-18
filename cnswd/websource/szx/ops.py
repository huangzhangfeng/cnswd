"""
通用元素操作模块
"""
import re
import time
from enum import Enum

import pandas as pd
from selenium.common.exceptions import (ElementNotInteractableException,
                                        NoSuchElementException)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from .constants import TINY_WAIT_SECOND

CLASS_ID = re.compile(r'=(\d{6})&')
PLATE_MAPS = {
    '137001': '市场分类',
    '137002': '证监会行业分类',
    '137003': '国证行业分类',
    '137004': '申万行业分类',
    '137006': '地区分类',
    '137007': '指数分类',
}
TIP_CSS = '.tips'                 # 响应提示
CANCEL_CSS = '.cancel'            # 终止
TIMEOUT_CSS = '.timeout'          # 超时提示
BUSY_CSS = '.sysbusy'             # 系统繁忙
NODATA_CSS = '.no-records-found'  # 无数据


class RetryException(Exception):
    """重试"""
    pass


class ResponseStatus(Enum):
    retry = 1
    nodata = 2
    failed = 3
    completed = 4


def wait_first_loaded(wait, elem_css, msg=''):
    """
    等待指定css元素可见，确保网页完成加载。

    Arguments:
        wait {WebDriverWait} -- 实例
        elem_css {str} -- 可见元素的css表达式
    """
    m = EC.visibility_of_element_located((By.CSS_SELECTOR, elem_css))
    wait.until(m, message=msg)


def toggler_open(elem):
    """展开元素"""
    assert elem.tag_name == 'li', '必须为"li"元素'
    attr = elem.get_attribute('class')
    if 'closed' in attr:
        elem.find_element_by_tag_name('span').click()


def toggler_close(elem):
    """折叠或隐藏元素"""
    assert elem.tag_name == 'li', '必须为"li"元素'
    attr = elem.get_attribute('class')
    if 'opened' in attr:
        elem.find_element_by_tag_name('span').click()


def _gen_sub_css(root_css, level):
    """生成层级子级联"""
    temp = []
    for i in level.split('.'):
        temp.append('ul > li:nth-child({})'.format(i))
        css = ' > '.join([root_css] + temp)
        yield css


def select_level(driver, root_css, level, close_parent=False):
    """选中层级所对应的元素"""
    root_nav = driver.find_element_by_css_selector(root_css)
    tag_name = root_nav.tag_name
    assert tag_name in ('div', 'li'), '根元素必须为"div"或"li"'
    for css in _gen_sub_css(root_css, level):
        elem = driver.find_element_by_css_selector(css)
        toggler_open(elem)
    attr = elem.get_attribute('class')
    if 'tree-empty' in attr:
        time.sleep(TINY_WAIT_SECOND)
        elem.find_element_by_tag_name('a').click()
    # 如果关闭根导航
    if close_parent:
        p_css = next(_gen_sub_css(root_css, level))
        p_elem = driver.find_element_by_css_selector(p_css)
        toggler_close(p_elem)
    return elem


def reset_level(driver, root_css, level):
    """复原折叠状态"""
    root_nav = driver.find_element_by_css_selector(root_css)
    tag_name = root_nav.tag_name
    assert tag_name in ('div', 'li'), '根元素必须为"div"或"li"'
    for css in reversed(list(_gen_sub_css(root_css, level))):
        elem = driver.find_element_by_css_selector(css)
        toggler_close(elem)


def wait_code_loaded(wait):
    """更改市场分类后，等待加载代码"""
    # 除CDR外，股票基础分类代码数量总是大于0
    # 如细分不存在代码，如台湾地区，则以非空的0值显示
    # 使用待选数量css
    css = '.cont-top-right #unselectcount'

    def f(driver):
        text = driver.find_element_by_css_selector(css).text
        # 如果细分概念不存在相应的股票，其值为0
        return text != ''
    wait.until(f, '代码加载超时')


def get_count(elem):
    """解析元素中提示数量信息"""
    if elem.tag_name != 'i':
        # 用i元素的值
        i = elem.find_element_by_tag_name('i')
    else:
        i = elem
    try:
        return int(i.text)
    except:
        return -1000


def get_unselect_count(driver):
    """当前可选代码数量"""
    css = 'div.select-box:nth-child(1) #unselectcount'
    i = driver.find_element_by_css_selector(css)
    try:
        return int(i.text)
    except:
        return 0


def parse_classify_info(elem):
    """解析span元素有关分类信息"""
    name = elem.get_attribute('data-name')
    code = elem.get_attribute('data-id')
    param = elem.get_attribute('data-param')
    platetype = None
    if param:
        m = re.search(CLASS_ID, param)
        if m:
            platetype = PLATE_MAPS[m.group(1)]
    return (name, code, platetype)


def get_classify_bom(driver):
    """获取分类编码表"""
    span_css = '.classify-tree span'
    span_elems = driver.find_elements_by_css_selector(span_css)
    span_data = [parse_classify_info(e) for e in span_elems]
    a_css = '.classify-tree a'
    a_elems = driver.find_elements_by_css_selector(a_css)
    a_data = [parse_classify_info(e) for e in a_elems]
    df = pd.DataFrame.from_records(
        span_data+a_data, columns=['分类名称', '分类编码', '平台类别'])
    df.dropna(how='all', inplace=True)
    df.drop_duplicates(subset=['分类编码', '平台类别'], inplace=True)
    df.set_index(['分类编码', '平台类别'], inplace=True)
    return df


def read_li_table(driver, li, tag_name):
    """获取单个元素的分类信息，返回数据框"""
    elem = li.find_element_by_tag_name(tag_name)
    name, code, platetype = parse_classify_info(elem)
    df = get_classify_codes(driver)
    if len(df):
        df.columns = ['股票代码', '股票简称']
        df['分类名称'] = name
        df['分类编码'] = code
        df['平台类别'] = platetype
        return df
    return pd.DataFrame()


def get_classify_codes(driver, only_code=False):
    """获取当前分类代码列表"""
    res = []
    css = 'div.select-box:nth-child(1) > div:nth-child(3) > ul:nth-child(1) > li'
    lis = driver.find_elements_by_css_selector(css)
    if only_code:
        for li in lis:
            span = li.find_element_by_tag_name('span')
            # data-id为股票代码
            res.append(span.get_attribute('data-id'))
        return res
    else:
        for li in lis:
            span = li.find_element_by_tag_name('span')
            res.append((span.get_attribute('data-id'),
                        span.get_attribute('data-name')))
        df = pd.DataFrame.from_records(res)
        return df


def get_classify_table(wait, driver, level, only_end):
    """获取层级分类信息表
    
    Arguments:
        wait {wait对象} -- wait对象
        driver {driver对象} -- driver对象
        level {str} -- 分类层级，如`1.2`,`2.4.11`
    
    Keyword Arguments:
        only_end {bool} -- 是否只提取最末端的层级 (default: {True})
                           默认为真。忽略含有子级的父层级
                           如`2.1.2`为末端，则`2.1`会忽略
    """
    root_css = '.detail-cont-tree'
    li = select_level(driver, root_css, level)
    attr = li.get_attribute('class')
    if attr == 'tree-empty':
        tag_name = 'a'
    else:
        tag_name = 'span'
        if only_end:
            reset_level(driver, root_css, level)
            return pd.DataFrame()
    wait_code_loaded(wait)
    # 层级越高，股票数量越多，等待的时间越长。等待代码加载还是没有真正完成
    num = get_unselect_count(driver)
    if num == 0:
        reset_level(driver, root_css, level)
        return pd.DataFrame()
    sl = min(1.5, max(num / 300, TINY_WAIT_SECOND))  # 最长不超过1.5秒
    time.sleep(sl)
    # 读取信息
    df = read_li_table(driver, li, tag_name)
    # 折叠根树
    reset_level(driver, root_css, level)
    return df


def _add_maket_codes(driver):
    """添加当前市场分类项下所有股票代码"""
    # 全选代码
    css = 'div.select-box:nth-child(1) > div:nth-child(1) > label'
    label = driver.find_element_by_css_selector(css)
    label.click()
    # 添加代码
    btn_css = 'div.arrows-box:nth-child(2) > div:nth-child(1) > button:nth-child(1)'
    driver.find_element_by_css_selector(btn_css).click()


def add_all_codes(api, include_b=False):
    """添加所有股票代码"""
    markets = ['沪市A', '深主板A', '中小板', '创业板']
    if include_b:
        markets += ['深市B', '沪市B']
    for market in markets:
        api._change_market_classify(market)
        _add_maket_codes(api.driver)


def remove_choosed_code(driver):
    """移除当前已选股票"""
    num_css = 'div.select-box:nth-child(3) > div:nth-child(1) > span:nth-child(2)'
    num_elem = driver.find_element_by_css_selector(num_css)
    num = get_count(num_elem)
    label_css = 'div.select-box:nth-child(3) > div:nth-child(1) > label:nth-child(1)'
    btn_css = 'div.arrows-box:nth-child(2) > div:nth-child(1) > button:nth-child(2)'
    if num >= 1:
        # 确保全选按钮已经选中
        label = driver.find_element_by_css_selector(label_css)
        if not is_checked(label):
            label.find_element_by_tag_name('i').click()
        # 移除所选代码
        btn = driver.find_element_by_css_selector(btn_css)
        btn.click()


def add_code(driver, code):
    """添加查询代码"""
    css_fmt = 'div.select-box:nth-child(1) > div:nth-child(3) > ul:nth-child(1) span[data-id="{}"]'
    code_css = css_fmt.format(code)
    add_css = 'div.arrows-box:nth-child(2) > div:nth-child(1) > button:nth-child(1)'
    driver.find_element_by_css_selector(code_css).click()
    driver.find_element_by_css_selector(add_css).click()


def is_checked(elem):
    """
    检查元素是否选中

    注意：
        1. 由`i`元素触发
        2. 检查`input`元素是否选中
    """

    if elem.tag_name != 'input':
        i = elem.find_element_by_tag_name('input')
    else:
        i = elem
    return i.is_selected()


def get_response_status(driver):
    """获取响应提示信息及状态"""
    try:
        driver.find_element_by_css_selector(NODATA_CSS)
        return ResponseStatus.nodata
    except NoSuchElementException:
        pass
    csss = [BUSY_CSS, TIMEOUT_CSS, CANCEL_CSS]
    for css in csss:
        elem = driver.find_element_by_css_selector(css)
        if elem.get_attribute('style') == 'display: inline;':
            return ResponseStatus.retry
    return ResponseStatus.completed


def _data_browse(api, css):
    """预览数据"""
    try:
        api.driver.find_element_by_css_selector(css).click()
        locator = (By.CSS_SELECTOR, '.onloading')
        m = EC.invisibility_of_element_located(locator)
        api.wait.until(m, message='查询数据超时')
        # 延迟等待后续完成
        time.sleep(0.2)
    except Exception as e:
        api.logger.error(f'{e!r}')


def wait_responsive_table_loaded(api, css, times):
    """等待响应数据完成加载"""
    for _ in range(times):
        _data_browse(api, css)
        status = get_response_status(api.driver)
        if status != ResponseStatus.retry:
            return status
        time.sleep(1)
    return ResponseStatus.failed


def convert_to_item(input_item, data):
    """确保输入项目转换为项目名称"""
    if '.' in input_item:
        # 转换指标
        item = data[input_item][0]
    else:
        item = input_item.strip()
    values = (x[0] for x in data.values())
    # 验证
    assert item in values, '不存在指标“{}”，可接受范围{}'.format(
        item, values)
    return item


def item_to_level(item, data):
    """数据项目名称转换为层级编码"""
    for l, v in data.items():
        if v[0] == item:
            break
    return l


def get_children_elem(li, depth):
    """获取元素第depth层的子li元素"""
    res = []
    for _ in range(depth):
        res.append('ul/li')
    css = '/'.join(res)
    return li.find_elements_by_xpath(css)


def get_children_level(li, level):
    """获取元素子li元素层级"""
    sub_lis = li.find_elements_by_xpath('ul/li')
    for i in range(len(sub_lis)):
        yield (sub_lis[i], '{}.{}'.format(level, i+1))
