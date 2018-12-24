"""

深证信数据搜索API

备注
    1. 加载代码故障率高，尽量固定使用代码，循环其他选项
    2. 必须更换代码时，启用较长时间休眠
"""
import re
import time
from functools import lru_cache

import pandas as pd
from selenium.common.exceptions import (NoSuchElementException,
                                        ElementClickInterceptedException,
                                        UnexpectedAlertPresentException)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

from cnswd.utils import (ensure_list, get_exchange_from_code, loop_codes,
                         loop_period_by, sanitize_dates)

from . import ops
from .base import SZXPage
from .constants import TIMEOUT, MID_WAIT_SECOND, MIN_WAIT_SECOND

MARKETS = {
    # '深市A': 1,
    '深市B': 2,
    '深主板A': 3,
    '中小板': 4,
    '创业板': 5,
    # '深市CDR': 6,
    '沪市A': 7,
    '沪市B': 8,
    # '沪市CDR': 9,
}

DATE_1_CSS = 'input.form-control:nth-child(1)'
DATE_2_CSS = 'input.date:nth-child(2)'
YEAR_1_CSS = '#se1_sele'
YEAR_2_CSS = '#se2_sele'
QUARTER_CSS = '.condition2 > select:nth-child(2)'

T_GROUP_1 = (None, None)
T_GROUP_2 = (YEAR_2_CSS, None)
T_GROUP_3 = (YEAR_1_CSS, QUARTER_CSS)
T_GROUP_4 = (DATE_1_CSS, DATE_2_CSS)


LEVEL_MAPS = {
    '1.1':   ('基本资料', T_GROUP_1),
    '2.1':   ('公司股东实际控制人', T_GROUP_4),
    '2.2':   ('公司股本变动', T_GROUP_4),
    '2.3':   ('上市公司高管持股变动', T_GROUP_4),
    '2.4':   ('股东增（减）持情况', T_GROUP_4),
    '2.5':   ('持股集中度', T_GROUP_3),
    '3.1':   ('行情数据', T_GROUP_4),
    '4.1':   ('投资评级', T_GROUP_4),
    '5.1':   ('上市公司业绩预告', T_GROUP_3),
    '6.1':   ('分红指标', T_GROUP_2),
    '7.1':   ('公司增发股票预案', T_GROUP_4),
    '7.2':   ('公司增发股票实施方案', T_GROUP_4),
    '7.3':   ('公司配股预案', T_GROUP_4),
    '7.4':   ('公司配股实施方案', T_GROUP_4),
    '7.5':   ('公司首发股票', T_GROUP_1),
    '8.1.1': ('个股TTM财务利润表', T_GROUP_3),
    '8.1.2': ('个股TTM现金流量表', T_GROUP_3),
    '8.2.1': ('个股单季财务利润表', T_GROUP_3),
    '8.2.2': ('个股单季现金流量表', T_GROUP_3),
    '8.2.3': ('个股单季财务指标', T_GROUP_3),
    '8.3.1': ('个股报告期资产负债表', T_GROUP_3),
    '8.3.2': ('个股报告期利润表', T_GROUP_3),
    '8.3.3': ('个股报告期现金表', T_GROUP_3),
    '8.3.4': ('金融类资产负债表2007版', T_GROUP_3),
    '8.3.5': ('金融类利润表2007版', T_GROUP_3),
    '8.3.6': ('金融类现金流量表2007版', T_GROUP_3),
    '8.4.1': ('个股报告期指标表', T_GROUP_3),
    '8.4.2': ('财务指标行业排名', T_GROUP_3),
}

# 控制参数
BATCH_CODE_NUM = 50               # 每批查询股票最大数量(此批量似乎最合适)

PAGINATION_PAT = re.compile(r'共\s(\d{1,})\s条记录')


def _concat(dfs):
    try:
        return pd.concat(dfs)
    except ValueError:
        return pd.DataFrame()


class DataBrowser(SZXPage):
    """数据搜索页"""
    # 其他属性
    root_nav_css = '.tree > li'
    current_market = None      # 当前市场分类
    current_code = set()       # 代码
    current_t1_css = ''        # 开始日期css
    current_t2_css = ''        # 结束日期css
    current_t1_value = ''      # 开始日期
    current_t2_value = ''      # 结束日期
    level_maps = LEVEL_MAPS

    def __init__(self, clear_cache=False, retry_times=3, **kwds):
        start = time.time()
        super().__init__(clear_cache=clear_cache, retry_times=retry_times, **kwds)
        check_loaded_css = 'a.active'
        self._switch_to(7, check_loaded_css)
        self.logger.notice(f'加载主页用时：{time.time() - start:>0.4f}秒')
        self._loaded = False

    def _load_all_code(self, include_b=True):
        """全选代码后再删除，将所有代码置放在待选区域"""
        if self._loaded:
            return
        markets = ['沪市A', '深主板A', '中小板', '创业板']
        label_css = 'div.select-box:nth-child(1) > div:nth-child(1) > label:nth-child(1)'
        btn_css = 'div.arrows-box:nth-child(2) > div:nth-child(1) > button:nth-child(1)'
        num_css = 'div.select-box:nth-child(3) > div:nth-child(1) > span:nth-child(2)'
        if include_b:
            markets += ['深市B', '沪市B']
        for market in markets:
            self._change_market_classify(market)
            # 确保全选按钮已经选中
            label = self.driver.find_element_by_css_selector(label_css)
            if not ops.is_checked(label):
                label.find_element_by_tag_name('i').click()
            # 全选代码
            btn = self.driver.find_element_by_css_selector(btn_css)
            btn.click()
            num_elem = self.driver.find_element_by_css_selector(num_css)
            num = ops.get_count(num_elem)
            self.logger.info(f"加载{market} 累计{num}个代码")
        ops.remove_choosed_code(self.driver)
        self._loaded = True

    def _add_all_code(self):
        """全选并添加代码"""
        self._load_all_code()
        css = 'div.select-box:nth-child(1) > div:nth-child(1) > label'
        self.driver.find_element_by_css_selector(css).click()
        btn_css = 'div.arrows-box:nth-child(2) > div:nth-child(1) > button:nth-child(1)'
        self.driver.find_element_by_css_selector(btn_css).click()

    def _on_pop_alert(self):
        """显示弹出框内容并确认"""
        alert = self.driver.switch_to_alert()
        text = alert.text
        self.logger.warn(text)
        alert.accept()

    def _select_level(self, level):
        """选定数据项目"""
        assert level in self.level_maps.keys(), f'数据搜索指标导航可接受范围：{self.level_maps}'
        ops.select_level(self.driver, self.root_nav_css, level, True)

    def _data_item_related(self, level):
        self._choose_data_fields()
        # 设置t1、t2定位格式
        self._set_date_css_by(level)

    def _choose_data_fields(self):
        """全选字段"""
        # 当更改数据项目时，自动将所选字段清除并复位
        field = '.detail-cont-bottom'
        chk_css = '{} label > i'.format(field)
        chk = self.driver.find_element_by_css_selector(chk_css)
        chk.click()
        btn_css = '{} button[class="arrow-btn right"]'.format(field)
        btn = self.driver.find_element_by_css_selector(btn_css)
        btn.click()  # 添加后`chk`自动复位

    def _set_date_css_by(self, level):
        """根据数据指标，确定t1与t2的css"""
        self.current_t1_css = self.level_maps[level][1][0]
        self.current_t2_css = self.level_maps[level][1][1]

    def _change_market_classify(self, market):
        """更改市场分类"""
        assert market in MARKETS.keys(), '可接受股票市场分类：{}'.format(MARKETS)
        root_css = '.detail-cont-tree'
        if market != self.current_market:
            level = '6.{}'.format(MARKETS[market])
            try:
                ops.select_level(self.driver, root_css, level)
            except UnexpectedAlertPresentException as e:
                self.logger.error(f'{e!r}')
                time.sleep(MID_WAIT_SECOND)
                ops.select_level(self.driver, root_css, level)
            try:
                # 使用隐式等待
                css = 'div.select-box:nth-child(1) > div:nth-child(3) li'
                self.wait.until(EC.visibility_of_any_elements_located(
                    (By.CSS_SELECTOR, css)))
            except Exception as e:
                self.logger.error(f'{e!r}')
            self.current_market = market

    def _change_code(self, codes):
        """
        更改查询股票代码

        注意
        ----
            1. 代码数量一般控制在20个以内
            2. 批次代码尽量在同一市场
        """
        if set(codes) != self.current_code:
            if len(codes) > BATCH_CODE_NUM:
                self.logger.warn(
                    '为减少失败可能，每批查询代码数量不应超过{}个'.format(BATCH_CODE_NUM))
            ops.remove_choosed_code(self.driver)
            for code in sorted(codes):
                market = get_exchange_from_code(code)
                self._change_market_classify(market)
                ops.add_code(self.driver, code)
            self.current_code = set(codes)

    # 速度慢，废弃
    def _select_code(self, code):
        """选择并添加代码"""
        css = '.codes-search-left > input:nth-child(1)'
        input_elem = self.driver.find_element_by_css_selector(css)
        input_elem.clear()
        input_elem.send_keys(code)
        search_css = '.codes-search-left > i:nth-child(2)'
        self.driver.find_element_by_css_selector(search_css).click()
        btn = 'div.arrows-box:nth-child(2) > div:nth-child(1) > button:nth-child(1)'
        self.driver.find_element_by_css_selector(btn).click()

    def _change_t1_value(self, t1):
        """更改查询t1值"""
        if self.current_t1_css and (t1 != self.current_t1_value):
            ## 输入日期字符串时
            if self.current_t1_css.startswith('input'):
                self._change_date(self.current_t1_css, t1)
                self.current_t1_value = t1
            if self.current_t1_css in ('#se1_sele', '#se2_sele'):
                css_id = self.current_t1_css[1:4]
                self._change_year(css_id, t1)
                self.current_t1_value = t1

    def _change_t2_value(self, t2):
        """更改查询t2值"""
        if self.current_t2_css and (t2 != self.current_t2_value):
            if self.current_t2_css.startswith('input'):
                self._change_date(self.current_t2_css, t2)
                self.current_t2_value = t2
            if self.current_t2_css.startswith('.condition2'):
                elem = self.driver.find_element_by_css_selector(
                    self.current_t2_css)
                t2 = int(t2)
                assert t2 in (1, 2, 3, 4), '季度有效值为(1,2,3,4)'
                select = Select(elem)
                select.select_by_index(t2 - 1)
                self.current_t2_value = t2

    @lru_cache()
    def get_stock_code(self):
        """获取所有股票代码

        Returns:
            list -- 股票代码列表
        """
        self._add_all_code()
        css = 'div.select-box:nth-child(3) > div:nth-child(2) > ul:nth-child(1) span'
        spans = self.driver.find_elements_by_css_selector(css)
        res = [span.get_attribute('data-id') for span in spans]
        ops.remove_choosed_code(self.driver)
        return res

    def get_all_stock_info(self):
        """获取所有股票基本资料
        
        备注：
            当前不稳定，凌晨时段比较可靠
        """
        level = '1.1'
        self._change_data_item(level)
        self._add_all_code()
        df = self._read_html_table()
        ops.remove_choosed_code(self.driver)
        return df

    def get_all_stock_ipo(self):
        """获取所有股票IPO信息
        
        备注：
            当前不稳定，凌晨时段比较可靠
        """
        level = '7.5'
        self._change_data_item(level)
        self._add_all_code()
        df = self._read_html_table()
        ops.remove_choosed_code(self.driver)
        return df

    def query(self, indicator, codes=None, t1=None, t2=None):
        """查询股票期间指标数据
            
        股票搜索从3个维度切分数据
        1. 指标:要搜索的数据项目
        2. 代码:搜索哪些股票的数据
        3. 时间:限定开始日期至结束日期之间的期间
                或者简单限定为年季度组合
                对于非频繁数据，只需指定年度

        Arguments:
            indicator {str} -- 指标名称或对应的指标层级，如"8.2.1"
                                      代表：个股单季财务利润表
            codes {str或可迭代对象} -- 股票代码或股票代码列表
        
        Keyword Arguments:
            t1 {str} -- t1日期(一般为开始日期) (default: {None})
            t2 {str} -- t2日期(日期或者季度数) (default: {None})

        Returns:
            pd.DataFrame -- 查询返回的数据框对象。如无数据，返回空对象：pd.DataFrame()
        """
        indicator = ops.convert_to_item(indicator, LEVEL_MAPS)
        # 设置指标
        level = ops.item_to_level(indicator, LEVEL_MAPS)
        self._change_data_item(level)

        # 验证及设置期间
        if self.current_t1_css and (t1 is None):
            raise ValueError('"{}"必须指定"t1"值'.format(indicator))
        self._change_t1_value(t1)

        if self.current_t2_css and ('select' in self.current_t2_css) and (t2 is None):
            raise ValueError('"{}"必须指定"t2"值'.format(indicator))
        self._change_t2_value(t2)

        if codes is not None:
            # 排序代码
            codes = sorted(ensure_list(codes))
        else:
            codes = self.get_stock_code()
        self.logger.info('>    项目：{} 共 {} 只股票({} - {})，期间：{} - {}'.format(
            indicator, len(codes), codes[0], codes[-1], t1, t2)
        )
        df = self._query_by_batch_code(codes)
        return df

    def _read_html_table(self):
        """读取当前网页数据表"""
        status = ops.wait_responsive_table_loaded(
            self, '.dataBrowseBtn', self.retry_times)
        if status == ops.ResponseStatus.nodata:
            return pd.DataFrame()
        elif status == ops.ResponseStatus.failed:
            tip = (self.current_level, self.current_code, self.current_t1_value, self.current_t2_value)
            raise ops.RetryException(f'<id={id(self)}> {tip} 加载失败，请重试')
        # 排除无数据或者延时(数据量大导致加载延时)情形，以下部分至少有一页数据
        # 一般情况下不会发生，在此前已经将提取单元缩小到必要程度，除非网络问题导致
        # 读取分页信息
        pagination_css = '.pagination-info'
        # 根本问题在于异步等待，无法确定响应是否加载完成
        # 对于行情数据，理论上指定期间的数据，也可以完成
        # 经过实验，单次请求不超过75行，故障率比较低
        pagination = self.driver.find_element_by_css_selector(pagination_css)
        try:
            total = int(re.search(PAGINATION_PAT, pagination.text).group(1))
        except UnboundLocalError:
            msg = '读取表信息失败。项目:{},代码:{},t1:{},t2:{}'.format(
                self.current_level,
                self.current_code,
                self.current_t1_value,
                self.current_t2_value
            )
            self.logger.error(msg)
            return pd.DataFrame()
        # 调整显示行数
        self._auto_change_view_row_num(total)
        dfs = []
        for i in range(1, self.page_num + 1):
            df = pd.read_html(self.driver.page_source)[0]
            dfs.append(df)
            # 点击进入下一页
            if i + 1 <= self.page_num:
                next_page = self.driver.find_element_by_link_text(str(i + 1))
                next_page.click()
            self.logger.info('>>>  分页进度 第{}页/共{}页'.format(i, self.page_num))
        return _concat(dfs)

    def _query_by_batch_code(self, codes):
        """代码分批读取数据"""
        dfs = []
        total_num = len(codes)
        count = 0
        batch_codes = loop_codes(codes, BATCH_CODE_NUM)
        msg_fmt = '<<   代码进度 {} / {} 批次数据：{}行({} - {})'
        for choosed_codes in batch_codes:
            count += len(choosed_codes)
            self._change_code(choosed_codes)
            df = self._read_html_table()
            self.logger.info(
                msg_fmt.format(
                    count, total_num, df.shape[0], choosed_codes[0], choosed_codes[-1])
            )
            dfs.append(df)
        return _concat(dfs)

    def get_stock_info(self, codes=None):
        """获取股票基本资料(1.1)"""
        return self.query('基本资料', codes)

    def get_actual_controller(self, codes=None, start=None, end=None):
        """获取公司股东实际控制人(2.1)"""
        start, end = sanitize_dates(start, end)
        return self.query('公司股东实际控制人', codes, start.strftime(r'%Y-%m-%d'), end.strftime(r'%Y-%m-%d'))

    def get_company_share_change(self, codes=None, start=None, end=None):
        """获取公司股本变动(2.2)"""
        start, end = sanitize_dates(start, end)
        return self.query('公司股本变动', codes, start.strftime(r'%Y-%m-%d'), end.strftime(r'%Y-%m-%d'))

    def get_executives_share_change(self, codes=None, start=None, end=None):
        """获取上市公司高管持股变动(2.3)"""
        start, end = sanitize_dates(start, end)
        return self.query('上市公司高管持股变动', codes, start.strftime(r'%Y-%m-%d'), end.strftime(r'%Y-%m-%d'))

    def get_shareholder_share_change(self, codes=None, start=None, end=None):
        """获取股东增（减）持情况（2.4）"""
        start, end = sanitize_dates(start, end)
        return self.query('股东增（减）持情况', codes, start.strftime(r'%Y-%m-%d'), end.strftime(r'%Y-%m-%d'))

    def get_shareholding_concentration(self, codes=None, start=None, end=None):
        """获取持股集中度（2.5）"""
        ps = loop_period_by(start, end, 'Q', False)
        # 按季度循环
        dfs = []
        for _, e in ps:
            df = self.query('持股集中度', codes, e.year, e.quarter)
            dfs.append(df)
        return _concat(dfs)

    def get_quote(self, codes=None, start=None, end=None, loop_ps=True):
        """获取行情数据(3.1)"""
        if loop_ps:
            dfs = []
            ps = loop_period_by(start, end, 'M', False)
            # 按月循环
            for s, e in ps:
                df = self.query('行情数据', codes, s.strftime(
                    r'%Y-%m-%d'), e.strftime(r'%Y-%m-%d'))
                dfs.append(df)
            if len(dfs) >= 1:
                return _concat(dfs)
            else:
                return pd.DataFrame()
        else:
            s, e = sanitize_dates(start, end)
            df = self.query('行情数据', codes, s.strftime(
                r'%Y-%m-%d'), e.strftime(r'%Y-%m-%d'))
            return df

    def get_investment_rating(self, codes=None, start=None, end=None):
        """获取投资评级数据(4.1)"""
        ps = loop_period_by(start, end, 'Y', False)
        dfs = []
        # 按年循环
        for s, e in ps:
            df = self.query('投资评级', codes, s.strftime(
                r'%Y-%m-%d'), e.strftime(r'%Y-%m-%d'))
            dfs.append(df)
        return _concat(dfs)

    def get_performance_forecast(self, codes=None, start=None, end=None):
        """获取上市公司业绩预告数据(5.1)"""
        ps = loop_period_by(start, end, 'Q', False)
        dfs = []
        # 按季度循环
        for _, e in ps:
            df = self.query('上市公司业绩预告', codes, e.year, e.quarter)
            dfs.append(df)
        return _concat(dfs)

    def get_dividend(self, codes=None, start=None, end=None):
        """获取分红指标(6.1)"""
        ps = loop_period_by(start, end, 'Y', False)
        # 按季度循环
        dfs = []
        for _, e in ps:
            df = self.query('分红指标', codes, str(e.year))
            dfs.append(df)
        return _concat(dfs)

    def get_additional_stock_plan(self, codes=None, start=None, end=None):
        """获取公司增发股票预案(7.1)"""
        ps = loop_period_by(start, end, 'Y', False)
        # 按季度循环
        dfs = []
        for s, e in ps:
            df = self.query('公司增发股票预案', codes, s.strftime(
                r'%Y-%m-%d'), e.strftime(r'%Y-%m-%d'))
            dfs.append(df)
        return _concat(dfs)

    def get_additional_stock_implementation(self, codes=None, start=None, end=None):
        """获取公司增发股票实施方案(7.2)"""
        ps = loop_period_by(start, end, 'Y', False)
        # 按年度循环
        dfs = []
        for s, e in ps:
            df = self.query('公司增发股票实施方案', codes, s.strftime(
                r'%Y-%m-%d'), e.strftime(r'%Y-%m-%d'))
            dfs.append(df)
        return _concat(dfs)

    def get_share_placement_plan(self, codes=None, start=None, end=None):
        """获取公司配股预案(7.3)"""
        ps = loop_period_by(start, end, 'Y', False)
        # 按年度循环
        dfs = []
        for s, e in ps:
            df = self.query('公司配股预案', codes, s.strftime(
                r'%Y-%m-%d'), e.strftime(r'%Y-%m-%d'))
            dfs.append(df)
        return _concat(dfs)

    def get_share_placement_implementation(self, codes=None, start=None, end=None):
        """获取公司配股实施方案(7.4)"""
        ps = loop_period_by(start, end, 'Y', False)
        # 按年度循环
        dfs = []
        for s, e in ps:
            df = self.query('公司配股实施方案', codes, s.strftime(
                r'%Y-%m-%d'), e.strftime(r'%Y-%m-%d'))
            dfs.append(df)
        return _concat(dfs)

    def get_IPO(self, codes=None):
        """获取公司首发股票(7.5)"""
        return self.query('7.5', codes)

    def get_ttm_income_statement(self, codes, start=None, end=None):
        """获取个股TTM财务利润表(8.1.1)"""
        ps = loop_period_by(start, end, freq='Q')
        # 按季度循环
        dfs = []
        for _, e in ps:
            df = self.query('个股TTM财务利润表', codes, e.year, e.quarter)
            dfs.append(df)
        return _concat(dfs)

    def get_ttm_cash_flow_statement(self, codes, start=None, end=None):
        """获取个股TTM现金流量表"""
        ps = loop_period_by(start, end, freq='Q')
        # 按季度循环
        dfs = []
        for _, e in ps:
            df = self.query('个股TTM现金流量表', codes, e.year, e.quarter)
            dfs.append(df)
        return _concat(dfs)

    def get_quarterly_income_statement(self, codes, start=None, end=None):
        """获取个股单季财务利润表"""
        ps = loop_period_by(start, end, freq='Q')
        # 按季度循环
        dfs = []
        for _, e in ps:
            df = self.query('个股单季财务利润表', codes, e.year, e.quarter)
            dfs.append(df)
        return _concat(dfs)

    def get_quarterly_cash_flow_statement(self, codes, start=None, end=None):
        """获取个股单季现金流量表"""
        ps = loop_period_by(start, end, freq='Q')
        # 按季度循环
        dfs = []
        for _, e in ps:
            df = self.query('个股单季现金流量表', codes, e.year, e.quarter)
            dfs.append(df)
        return _concat(dfs)

    def get_quarterly_financial_indicator(self, codes, start=None, end=None):
        """获取个股单季财务指标"""
        ps = loop_period_by(start, end, freq='Q')
        # 按季度循环
        dfs = []
        for _, e in ps:
            df = self.query('个股单季财务指标', codes, e.year, e.quarter)
            dfs.append(df)
        return _concat(dfs)

    def get_periodly_balance_sheet(self, codes, start=None, end=None):
        """获取个股报告期资产负债表"""
        ps = loop_period_by(start, end, freq='Q')
        # 按季度循环
        dfs = []
        for _, e in ps:
            df = self.query('个股报告期资产负债表', codes, e.year, e.quarter)
            dfs.append(df)
        return _concat(dfs)

    def get_periodly_income_statement(self, codes, start=None, end=None):
        """获取个股报告期利润表"""
        ps = loop_period_by(start, end, freq='Q')
        # 按季度循环
        dfs = []
        for _, e in ps:
            df = self.query('个股报告期利润表', codes, e.year, e.quarter)
            dfs.append(df)
        return _concat(dfs)

    def get_periodly_cash_flow_statement(self, codes, start=None, end=None):
        """获取个股报告期现金表"""
        ps = loop_period_by(start, end, freq='Q')
        # 按季度循环
        dfs = []
        for _, e in ps:
            df = self.query('个股报告期现金表', codes, e.year, e.quarter)
            dfs.append(df)
        return _concat(dfs)

    def get_periodly_balance_sheet_2007(self, codes, start=None, end=None):
        """获取金融类资产负债表2007版"""
        ps = loop_period_by(start, end, freq='Q')
        # 按季度循环
        dfs = []
        for _, e in ps:
            if e.year >= 2006:
                df = self.query('金融类资产负债表2007版', codes, e.year, e.quarter)
                dfs.append(df)
        return _concat(dfs)

    def get_periodly_income_statement_2007(self, codes, start=None, end=None):
        """获取金融类利润表2007版"""
        ps = loop_period_by(start, end, freq='Q')
        # 按季度循环
        dfs = []
        for _, e in ps:
            if e.year >= 2006:
                df = self.query('金融类利润表2007版', codes, e.year, e.quarter)
                dfs.append(df)
        return _concat(dfs)

    def get_periodly_cash_flow_statement_2007(self, codes, start=None, end=None):
        """获取金融类现金流量表2007版"""
        ps = loop_period_by(start, end, freq='Q')
        # 按季度循环
        dfs = []
        for _, e in ps:
            if e.year >= 2006:
                df = self.query('金融类现金流量表2007版', codes, e.year, e.quarter)
                dfs.append(df)
        return _concat(dfs)

    def get_periodly_financial_indicator(self, codes, start=None, end=None):
        """获取个股报告期指标表"""
        ps = loop_period_by(start, end, freq='Q')
        # 按季度循环
        dfs = []
        for _, e in ps:
            df = self.query('个股报告期指标表', codes, e.year, e.quarter)
            dfs.append(df)
        return _concat(dfs)

    def get_financial_indicator_ranking(self, codes, start=None, end=None):
        """获取财务指标行业排名"""
        ps = loop_period_by(start, end, freq='Q')
        # 按季度循环
        dfs = []
        for _, e in ps:
            df = self.query('财务指标行业排名', codes, e.year, e.quarter)
            dfs.append(df)
        return _concat(dfs)

    def get_data(self, data_item, codes=None, start=None, end=None):
        """获取项目数据
        
        Arguments:
            data_item {str} -- 项目名称或者层级
        
        Keyword Arguments:
            codes {str　或可迭代str} -- 股票代码(６位数) (default: {None})，如为空，使用当前可用代码
            start {str} -- 开始日期 (default: {None})，如为空，使用市场开始日期
            end {str} -- 结束日期 (default: {None})，如为空，使用当前日期
        
        Raises:
            NotImplementedError -- 如输入非法名称，触发异常

        Usage:
            >>> api = WebApi()
            >>> api.get_data('投资评级','000001', '2018-01-01','2018-08-01')
            >>> api.get_data('投资评级',['000001','000002'], '2018-01-01','2018-08-01')
            >>> api.get_data('4.1',['000001','000002'], '2018-01-01','2018-08-01')

        Returns:
            pd.DataFrame -- 如期间没有数据，返回长度为０的空表
        """
        item = ops.convert_to_item(data_item, LEVEL_MAPS)
        if item == LEVEL_MAPS['1.1'][0]:
            return self.get_stock_info(codes)
        elif item == LEVEL_MAPS['2.1'][0]:
            return self.get_actual_controller(codes, start, end)
        elif item == LEVEL_MAPS['2.2'][0]:
            return self.get_company_share_change(codes, start, end)
        elif item == LEVEL_MAPS['2.3'][0]:
            return self.get_executives_share_change(codes, start, end)
        elif item == LEVEL_MAPS['2.4'][0]:
            return self.get_shareholder_share_change(codes, start, end)
        elif item == LEVEL_MAPS['2.5'][0]:
            return self.get_shareholding_concentration(codes, start, end)
        elif item == LEVEL_MAPS['3.1'][0]:
                return self.get_quote(codes, start, end)
        elif item == LEVEL_MAPS['4.1'][0]:
            return self.get_investment_rating(codes, start, end)
        elif item == LEVEL_MAPS['5.1'][0]:
            return self.get_performance_forecast(codes, start, end)
        elif item == LEVEL_MAPS['6.1'][0]:
            return self.get_dividend(codes, start, end)
        elif item == LEVEL_MAPS['7.1'][0]:
            return self.get_additional_stock_plan(codes, start, end)
        elif item == LEVEL_MAPS['7.2'][0]:
            return self.get_additional_stock_implementation(codes, start, end)
        elif item == LEVEL_MAPS['7.3'][0]:
            return self.get_share_placement_plan(codes, start, end)
        elif item == LEVEL_MAPS['7.4'][0]:
            return self.get_share_placement_implementation(codes, start, end)
        elif item == LEVEL_MAPS['7.5'][0]:
            return self.get_IPO(codes)
        elif item == LEVEL_MAPS['8.1.1'][0]:
            return self.get_ttm_income_statement(codes, start, end)
        elif item == LEVEL_MAPS['8.1.2'][0]:
            return self.get_ttm_cash_flow_statement(codes, start, end)
        elif item == LEVEL_MAPS['8.2.1'][0]:
            return self.get_quarterly_income_statement(codes, start, end)
        elif item == LEVEL_MAPS['8.2.2'][0]:
            return self.get_quarterly_cash_flow_statement(codes, start, end)
        elif item == LEVEL_MAPS['8.2.3'][0]:
            return self.get_quarterly_financial_indicator(codes, start, end)
        elif item == LEVEL_MAPS['8.3.1'][0]:
            return self.get_periodly_balance_sheet(codes, start, end)
        elif item == LEVEL_MAPS['8.3.2'][0]:
            return self.get_periodly_income_statement(codes, start, end)
        elif item == LEVEL_MAPS['8.3.3'][0]:
            return self.get_periodly_cash_flow_statement(codes, start, end)
        elif item == LEVEL_MAPS['8.3.4'][0]:
            return self.get_periodly_balance_sheet_2007(codes, start, end)
        elif item == LEVEL_MAPS['8.3.5'][0]:
            return self.get_periodly_income_statement_2007(codes, start, end)
        elif item == LEVEL_MAPS['8.3.6'][0]:
            return self.get_periodly_cash_flow_statement_2007(codes, start, end)
        elif item == LEVEL_MAPS['8.4.1'][0]:
            return self.get_periodly_financial_indicator(codes, start, end)
        elif item == LEVEL_MAPS['8.4.2'][0]:
            return self.get_financial_indicator_ranking(codes, start, end)
        raise NotImplementedError('不支持数据项目“{}”'.format(item))

    def get_levels_for(self, nth=3):
        """获取第nth种类别的分类层级"""
        res = []

        def get_all_children(li, level):
            # 对于给定的li元素，递归循环至没有子级别的li元素
            if len(li.find_elements_by_xpath('ul/li')):
                for e, l in ops.get_children_level(li, level):
                    res.append((e, l))
                    get_all_children(e, l)

        tree_css = '.classify-tree > li:nth-child({})'.format(nth)
        li = self.driver.find_element_by_css_selector(tree_css)
        get_all_children(li, nth)
        return [x for _, x in res]

    def get_total_classify_info(self, nths, depth=5, only_end=True):
        """获取nths种类别，限定层级为depth的分类信息
        
        Arguments:
            nths {整数或整数列表} -- 选取哪几种分类
            depth {整数} -- 分类层次深度
        
        Returns:
            pd.DataFrame -- 数据表对象

        Usage:
            >>> api = WebApi()
            >>> # 获取第3类二层分类信息，层级类似`3.1`，`3.4`
            >>> api.get_total_classify_info(3, 2)
        """
        choosed_item = ensure_list(nths)
        for nth in choosed_item:
            assert 1 <= nth <= 6, "数字应介于1~6之间"
        assert 1 < depth <= 5, '分类层级最多5层'
        levels = []
        for nth in choosed_item:
            levels.extend(self.get_levels_for(nth))
        choosed = [l for l in levels if len(l.split('.')) <= depth]
        self.logger.info('分类层级数量:{}'.format(len(choosed)))
        dfs = []
        for l in choosed:
            self.logger.info('当前分类层级:{}'.format(l))
            df = ops.get_classify_table(self.wait, self.driver, l, only_end)
            dfs.append(df)
        return _concat(dfs)

    def yield_total_classify_table(self, only_end):
        """输出表供写入数据库"""
        levels = []
        for nth in (1, 2, 3, 4, 5, 6):
            levels.extend(self.get_levels_for(nth))
        self.logger.info('分类层级数量:{}'.format(len(levels)))
        for l in levels:
            self.logger.info('当前分类层级:{}'.format(l))
            df = ops.get_classify_table(self.wait, self.driver, l, only_end)
            if len(df):
                df['分类层级'] = l
            else:
                df = pd.DataFrame()
            yield df

    def get_total_classify_levels(self):
        """获取分类树编码"""
        levels = []
        for nth in (1, 2, 3, 4, 5, 6):
            levels.extend(self.get_levels_for(nth))
        return levels

    def get_classify_stock(self, level, only_end=True):
        """获取分类层级下的股票列表"""
        self.logger.info(f'分类层级：{level}')
        df = ops.get_classify_table(self.wait, self.driver, level, only_end)
        if len(df):
            df['分类层级'] = level
        else:
            df = pd.DataFrame()
        return df

    @property
    def classify_bom(self):
        """分类编码bom表"""
        return ops.get_classify_bom(self.driver)
