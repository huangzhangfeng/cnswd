"""

深证信专题统计模块

"""
import re
import time

import pandas as pd
from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

from . import ops
from .base import SZXPage

LEVEL_MAPS = {
    '1.1':     ('大宗交易报表', ('#fBDatepair > input:nth-child(1)', None)),
    '2.1':     ('融资融券明细', ('#fBDatepair > input:nth-child(1)', None)),
    '3.1':     ('解禁报表明细', ('#fBDatepair > input:nth-child(1)', None)),
    '4.1':     ('按天减持明细', ('#fBDatepair > input:nth-child(1)', None)),
    '4.2':     ('按天增持明细', ('#fBDatepair > input:nth-child(1)', None)),
    '4.3':     ('减持汇总统计', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '4.4':     ('增持汇总统计', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '5.1':     ('股本情况', (None, None)),
    '5.2':     ('高管持股变动汇总', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '5.3':     ('高管持股变动明细', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '5.4':     ('实际控制人持股变动', (None, None)),
    '5.5':     ('股东人数及持股集中度', ('#seee1_sele', '.condition2 > select:nth-child(2)')),
    '6.1':     ('业绩预告', ('#seee1_sele', '.condition2 > select:nth-child(2)')),
    '6.2':     ('预告业绩扭亏个股', ('#seee1_sele', '.condition2 > select:nth-child(2)')),
    '6.3':     ('预告业绩大幅下降个股', ('#seee1_sele', '.condition2 > select:nth-child(2)')),
    '6.4':     ('预告业绩大幅上升个股', ('#seee1_sele', '.condition2 > select:nth-child(2)')),
    '7.1':     ('个股定报主要指标', ('#seee1_sele', '.condition2 > select:nth-child(2)')),
    '7.2':     ('地区财务指标明细', ('#seee1_sele', '.condition2 > select:nth-child(2)')),
    '7.3':     ('行业财务指标明细', ('#seee1_sele', '.condition2 > select:nth-child(2)')),
    '7.4':     ('分市场财务指标明细', ('#seee1_sele', '.condition2 > select:nth-child(2)')),
    '8.1':     ('地区分红明细', ('#seee1_sele', None)),
    '8.2':     ('行业分红明细', ('#seee1_sele', None)),
    '9.1':     ('股东大会召开情况', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '9.2':     ('股东大会相关事项变动', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '9.3':     ('股东大会议案表', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '10.1':    ('停复牌', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '10.2':    ('市场公开信息汇总', ('#fBDatepair > input:nth-child(1)', None)),
    '11.1':    ('公司债或可转债', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '11.2':    ('增发筹资', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '11.3':    ('配股筹资', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '11.4':    ('首发筹资', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '12.1':    ('拟上市公司清单', (None, None)),
    '12.2':    ('暂停上市公司清单', (None, None)),
    '12.3':    ('终止上市公司清单', (None, None)),
    '13.1':    ('首发审核', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '13.2':    ('资产重组', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '13.3':    ('债务重组', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '13.4':    ('吸收合并', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '13.5':    ('股权变更', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '13.6':    ('公司诉讼', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
    '13.7':    ('对外担保', ('#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)')),
}
# 控制参数
BATCH_CODE_NUM = 50               # 每批查询股票最大数量(此批量似乎最合适)

PAGINATION_PAT = re.compile(r'共\s(\d{1,})\s条记录')


def _concat(dfs):
    try:
        return pd.concat(dfs)
    except ValueError:
        return pd.DataFrame()


class ThematicStatistics(SZXPage):
    """深证信专题统计api"""

    root_nav_css = '.thematicStatistics-tree'
    view_selection = {1: 20, 2: 50, 3: 100, 4: 200}
    level_maps = LEVEL_MAPS
    current_t1_css = ''        # 开始日期css
    current_t2_css = ''        # 结束日期css
    current_t1_value = ''      # 开始日期
    current_t2_value = ''      # 结束日期

    def __init__(self, clear_cache=False, retry_times=3, **kwds):
        start = time.time()
        super().__init__(clear_cache=clear_cache, retry_times=retry_times, **kwds)
        check_loaded_css = 'a.active'
        self._switch_to(8, check_loaded_css)
        # 需要再休眠一会，等待页面完全加载
        time.sleep(0.3)
        self.logger.notice(f'加载主页用时：{time.time() - start:>0.4f}秒')

    def _set_date_css_by(self, level):
        """根据数据指标，确定t1与t2的css"""
        self.current_t1_css = self.level_maps[level][1][0]
        self.current_t2_css = self.level_maps[level][1][1]

    def _select_by_value(self, value=''):
        """选择全部"""
        # 当值为空时，默认选择全部类型
        css = '.condition6 > select:nth-child(2)'
        elem = self.driver.find_element_by_css_selector(css)
        select = Select(elem)
        select.select_by_value(value)

    def _data_item_related(self, level):
        # 设置t1、t2定位格式
        self._set_date_css_by(level)

    def _select_level(self, level):
        """选定数据项目"""
        assert level in self.level_maps.keys(
        ), f'数据搜索指标导航可接受范围：{self.level_maps}'
        ops.select_level(self.driver, self.root_nav_css, level, True)
        if level in ('5.1', '5.4', '13.1', '13.2', '13.5', '13.6', '13.7'):
            self._select_by_value()

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

    def query(self, indicator, t1=None, t2=None):
        """专题统计查询
            
        专题统计从2个维度切分数据
        1. 指标:要搜索的数据项目
        2. 时间:限定开始日期至结束日期之间的期间
                或者简单限定为年季度组合
                对于非频繁数据，只需指定年度
        说明：
            对于其他专项统计，如市场、地区采用内部循环

        Arguments:
            indicator {str} -- 指标名称或对应的指标层级，如"8.2.1"
                                      代表：个股单季财务利润表
        
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

        self.logger.info('>    项目：{} 期间：{} - {}'.format(
            indicator, t1, t2)
        )
        df = self._loop_all(level)
        return df

    def _loop_all(self, level):
        """内部循环"""
        # 5.2 5.3 类型
        # 7.2 地区 7.3 行业
        # 8.1 地区 8.2 行业
        # 11.1 债券类型
        if level in ('5.2','5.3','7.2','7.3','8.1','8.2','11.1'):
            dfs = []
            css = '.condition6 > select:nth-child(2)'
            elem = self.driver.find_element_by_css_selector(css)
            select = Select(elem)
            options = select.options
            for o in options:
                self.logger.info(f'读取：{o.text}')
                select.select_by_visible_text(o.text)
                data = self._read_html_table()
                dfs.append(data)
            df = _concat(dfs)
        else:
            df = self._read_html_table()
        return df

    def _data_browse(self):
        """预览数据"""
        btn_css = '.thematicStatisticsBtn'
        try:
            # 12.1 12.2 12.3 没有查询按钮
            btn = self.driver.find_element_by_css_selector(btn_css)
            btn.click()
        except (NoSuchElementException, ElementNotInteractableException):
            pass
        locator = (By.CSS_SELECTOR, '.tip')
        m = EC.invisibility_of_element_located(locator)
        self.wait.until(m, message='查询数据超时')

    def _get_response_status(self):
        try:
            self.driver.find_element_by_css_selector('.no-records-found')
            return ops.ResponseStatus.nodata
        except ops.NoSuchElementException:
            pass
        css = '.tip'
        elem = self.driver.find_element_by_css_selector(css)
        if elem.get_attribute('style') == 'display: inline;':
            return ops.ResponseStatus.retry  
        else:
            return ops.ResponseStatus.completed      

    def _wait_responsive_table_loaded(self, retry_times=3):
        for _ in range(retry_times):
            self._data_browse()
            status = self._get_response_status()
            if status != ops.ResponseStatus.retry:
                return status    
            time.sleep(0.3)
        return ops.ResponseStatus.failed

    def _read_html_table(self):
        """读取当前网页数据表"""
        status = self._wait_responsive_table_loaded()
        if status in (ops.ResponseStatus.nodata, ops.ResponseStatus.failed):
            return pd.DataFrame()

        # 读取分页信息
        pagination_css = '.pagination-info'
        pagination = self.driver.find_element_by_css_selector(pagination_css)
        try:
            total = int(re.search(PAGINATION_PAT, pagination.text).group(1))
        except UnboundLocalError:
            msg = '读取表信息失败。项目:{},t1:{},t2:{}'.format(
                self.current_level,
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
