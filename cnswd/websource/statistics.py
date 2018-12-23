"""
深证信专题统计模块
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from itertools import count
import math
import re
import time
import os
import logbook
import pandas as pd
from logbook.more import ColorizedStderrHandler
from selenium.common.exceptions import (ElementNotInteractableException,
                                        NoAlertPresentException,
                                        NoSuchElementException,
                                        TimeoutException)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

from cnswd.websource import helper_statistics as helper
from cnswd.utils import (ensure_list, get_exchange_from_code, loop_codes,
                     loop_period_by, sanitize_dates)
from cnswd.websource._selenium import make_headless_browser

# 设置显示日志
logbook.set_datetime_format('local')
handler = ColorizedStderrHandler()
handler.push_application()

logger = logbook.Logger('深证信专题统计')


LONG_SLEEP = 10 * 60         # 系统繁忙时，长时间休眠10分钟
REFRESH_COUNT = 100       # 运行N次后，刷新、休眠
MAX_WAIT_SECOND = 60   # 尽量细化，防止请求大量数据
POLL_FREQUENCY = 0.2      # 默认值0.5太大
CHANGE_MARKET = 1.5      # 转换市场分类

PAGE_MAX_ROW = 50        # 每页显示50行
BATCH_CODE_NUM = 10
PAGINATION_PAT = re.compile(r'共\s(\d{1,})\s条记录')



class StatisticsApi(object):
    """深证信专题统计api"""

    def __init__(self, clear_cache=True):
        # 初始化无头浏览器
        self.browser = make_headless_browser()
        # 清理缓存
        if clear_cache:
            helper.clear_firefox_cache(self.browser)
            logger.info('完成初始化及缓存清理')
        self.wait = WebDriverWait(
            self.browser, MAX_WAIT_SECOND, POLL_FREQUENCY)
        # 数据项目层级
        self.current_level = ''

        self.page_counter = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.browser.quit()

    def _change_data_item(self, level):
        """更改数据项目"""
        # 更改项目会重新加载数据表所包含的字段。
        if level != self.current_level:
            helper.select_data_item(self.browser, level)
            time.sleep(0.2)
            self.current_level = level

    def _change_view_row_num(self, num):
        """数据正常响应后，更改每页显示行数"""
        if num == 20:
            nth = 1
        elif num == 50:
            nth = 2
        elif num == 100:
            nth = 3
        else:
            nth = 4

        def change_view_row():
            css_1 = '.dropdown-toggle'
            self.browser.find_element_by_css_selector(css_1).click()
            css_2 = '.dropdown-menu > li:nth-child({})'.format(nth)
            self.browser.find_element_by_css_selector(css_2).click()
        change_view_row()

    def _read_html_table(self):
        """读取当前网页数据表"""
        self.page_counter += 1
        helper.wait_responsive_table_loaded(self.wait, self.browser)
        # time.sleep(0.2)
        no_data_tip = helper.get_response_status(self.browser)
        # 如果存在提示或者无数据，返回空表
        if no_data_tip:
            if '系统繁忙' in no_data_tip:
                logger.notice('系统繁忙，休眠{}秒'.format(LONG_SLEEP))
                time.sleep(LONG_SLEEP)
            return pd.DataFrame()
        # 排除无数据或者延时(数据量大导致加载延时)情形，以下部分至少有一页数据
        # 一般情况下不会发生，在此前已经将提取单元缩小到必要程度，除非网络问题导致
        # 读取分页信息
        pagination_css = '.pagination-info'
        # 根本问题在于异步等待，无法确定响应是否加载完成
        # 对于行情数据，理论上指定期间的数据，也可以完成
        # 经过实验，单次请求不超过75行，故障率比较低
        try:
            pagination = self.browser.find_element_by_css_selector(
                pagination_css)
        except NoSuchElementException:
            time.sleep(0.2)
        try:
            row_num = int(re.search(PAGINATION_PAT, pagination.text).group(1))
        except UnboundLocalError:
            msg = '读取表信息失败。项目:{}'.format(self.current_level)
            logger.error(msg)
            return pd.DataFrame()
        # 分页
        page_max_row = 20  # 默认值
        if row_num > 100:
            page_max_row = 200
            self._change_view_row_num(200)
        elif row_num > 50:
            page_max_row = 100
            self._change_view_row_num(100)
        elif row_num > 20:
            page_max_row = 50
            self._change_view_row_num(50)
        # 低于20行，无需操作
        page_num = math.ceil(row_num / page_max_row)
        dfs = []
        for i in range(1, page_num + 1):
            try:
                df = pd.read_html(self.browser.page_source,
                                  converters={'证券代码': str})[0]
            except ValueError:
                # 如果触发异常，首先是无法解析，ValueError
                df = pd.DataFrame()
            except IndexError:
                # 如果没有解析到表格数据，返回空表
                df = pd.DataFrame()
            finally:
                dfs.append(df)
                del df
            if i+1 <= page_num:
                next_page = self.browser.find_element_by_link_text(str(i+1))
                next_page.click()
            logger.info('>>>  分页进度 第{}页/共{}页'.format(i, page_num))
        if (self.page_counter + 1) % REFRESH_COUNT == 0:
            time.sleep(1)
        return pd.concat(dfs)

    def _get(self, level):
        """获取项目数据"""
        self.browser.get(helper.API_URL)
        time.sleep(0.5)
        self._change_data_item(level)
        return self._read_html_table()

    def _get_one_day(self, level, date):
        """获取项目特定日期的数据"""
        self.browser.get(helper.API_URL)
        time.sleep(0.5)
        self._change_data_item(level)
        date_str = pd.Timestamp(date).strftime(r'%Y-%m-%d')
        date_css = '#fBDatepair > input:nth-child(1)'
        helper.set_date(self.browser, date_css, date_str)
        return self._read_html_table()

    def _get_period(self, level, start, end):
        """获取项目特定期间的数据"""
        self.browser.get(helper.API_URL)
        time.sleep(0.5)
        self._change_data_item(level)
        start_str = pd.Timestamp(start).strftime(r'%Y-%m-%d')
        start_css = '#dBDatepair > input:nth-child(1)'
        helper.set_date(self.browser, start_css, start_str)
        end_str = pd.Timestamp(end).strftime(r'%Y-%m-%d')
        end_css = 'input.date:nth-child(2)'
        helper.set_date(self.browser, end_css, end_str)
        return self._read_html_table()

    def get_transactions(self, date):
        """大宗交易报表"""
        logger.info('当前项目：大宗交易报表')
        return self._get_one_day('1.1', date)

    def get_margin_detail(self, date):
        """融资融券明细"""
        logger.info('当前项目：融资融券明细')
        return self._get_one_day('2.1', date)

    def get_lift_detail(self, date):
        """解禁报表明细"""
        logger.info('当前项目：解禁报表明细')
        return self._get_one_day('3.1', date)

    def get_reduction_detail(self, date):
        """按天减持明细"""
        logger.info('当前项目：按天减持明细')
        return self._get_one_day('4.1', date)

    def get_overweight_detail(self, date):
        """按天增持明细"""
        logger.info('当前项目：按天增持明细')
        return self._get_one_day('4.2', date)

    def get_reduction_summary(self, start, end):
        """减持汇总统计"""
        logger.info('当前项目：减持汇总统计')
        return self._get_period('4.3', start, end)

    def get_overweight_summary(self, start, end):
        """增持汇总统计"""
        logger.info('当前项目：增持汇总统计')
        return self._get_period('4.4', start, end)

    def get_share_detail(self):
        """股本情况"""
        self.browser.get(helper.API_URL)
        time.sleep(0.5)
        self._change_data_item('5.1')
        css = '.condition6 > select:nth-child(2)'
        select = Select(self.browser.find_element_by_css_selector(css))
        select.select_by_value("")
        logger.info('当前项目：股本情况')
        return self._read_html_table()

    def _get_stock_change_details(self, level, start, end):
        self.browser.get(helper.API_URL)
        time.sleep(0.5)
        self._change_data_item(level)
        start_str = pd.Timestamp(start).strftime(r'%Y-%m-%d')
        start_css = '#dBDatepair > input:nth-child(1)'
        helper.set_date(self.browser, start_css, start_str)
        end_str = pd.Timestamp(end).strftime(r'%Y-%m-%d')
        end_css = 'input.date:nth-child(2)'
        helper.set_date(self.browser, end_css, end_str)
        # 类型循环
        dfs = []
        css = '.condition6 > select:nth-child(2)'
        select = Select(self.browser.find_element_by_css_selector(css))
        for i in range(2):
            select.select_by_index(i)
            df = self._read_html_table()
            dfs.append(df)
        return pd.concat(dfs)

    def get_executive_stock_change_details(self, start, end):
        """高管持股变动明细"""
        logger.info('当前项目：高管持股变动明细')
        return self._get_stock_change_details('5.2', start, end)

    def get_executive_stock_change_summary(self, start, end):
        """高管持股变动汇总"""
        logger.info('当前项目：高管持股变动汇总')
        return self._get_stock_change_details('5.3', start, end)

    def get_actual_controller_change(self):
        """实际控制人持股变动"""
        self.browser.get(helper.API_URL)
        time.sleep(0.5)
        self._change_data_item('5.4')
        css = '.condition6 > select:nth-child(2)'
        select = Select(self.browser.find_element_by_css_selector(css))
        select.select_by_value("")
        logger.info('当前项目：实际控制人持股变动')
        return self._read_html_table()

    def _change_year(self, year, css_id='seee1_sele'):
        """改变查询年份"""
        js = 'document.getElementById("{}").value="{}";'.format(css_id, year)
        self.browser.execute_script(js)

    def _get_quarterly_data(self, level, year, quarter):
        assert quarter in (1, 2, 3, 4), '数字1代表第一季度；2代表第二季度；3代表第三季度；4代表第四季度'
        self.browser.get(helper.API_URL)
        time.sleep(0.5)
        self._change_data_item(level)
        self._change_year(year)
        css = '.condition2 > select:nth-child(2)'
        select = Select(self.browser.find_element_by_css_selector(css))
        select.select_by_index(quarter-1)
        return self._read_html_table()

    def get_concentration(self, year, quarter):
        """股东人数及持股集中度"""
        logger.info('当前项目：股东人数及持股集中度')
        return self._get_quarterly_data('5.5', year, quarter)

    def get_performance_forecast(self, year, quarter):
        """业绩预告"""
        logger.info('当前项目：业绩预告')
        return self._get_quarterly_data('6.1', year, quarter)

    def get_performance_forecast_to_win(self, year, quarter):
        """预告业绩扭亏个股"""
        logger.info('当前项目：预告业绩扭亏个股')
        return self._get_quarterly_data('6.2', year, quarter)

    def get_performance_forecast_dramatically_down(self, year, quarter):
        """预告业绩大幅下降个股"""
        logger.info('当前项目：预告业绩大幅下降个股')
        return self._get_quarterly_data('6.3', year, quarter)

    def get_performance_forecast_dramatically_up(self, year, quarter):
        """预告业绩大幅上升个股"""
        logger.info('当前项目：预告业绩大幅上升个股')
        return self._get_quarterly_data('6.4', year, quarter)

    def get_main_indicators(self, year, quarter):
        """个股定报主要指标"""
        logger.info('当前项目：个股定报主要指标')
        return self._get_quarterly_data('7.1', year, quarter)

    def get_to_be_listed(self):
        """12.1 拟上市公司清单"""
        logger.info('当前项目：拟上市公司清单')
        return self._get('12.1')

    def get_Initial_review(self):
        """13.1 首发审核"""
        logger.info('当前项目：首发审核')
        return self._get_quarterly_data('13.1', year, quarter)


api = StatisticsApi()
#api.get_to_be_listed()
browser = api.browser
level = '2.2'
'.tree'
'a.active'
'li.tree-opened:nth-child(5) > ul:nth-child(2) > li:nth-child(2) > a:nth-child(2)'