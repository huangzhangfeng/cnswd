"""

深证信专题统计模块

"""
import re
import time

import pandas as pd
from selenium.common.exceptions import (ElementNotInteractableException,
                                        NoSuchElementException,
                                        StaleElementReferenceException,
                                        TimeoutException)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

from . import ops
from .base import SZXPage

# 专题统计映射参数： 名称 t1_css t2_css 可选css 休眠时长
LEVEL_MAPS = {
    '1.1':     ('大宗交易报表', '#fBDatepair > input:nth-child(1)', None, None, 0.5),
    '2.1':     ('融资融券明细', '#fBDatepair > input:nth-child(1)', None, None, 1.0),
    '3.1':     ('解禁报表明细', '#fBDatepair > input:nth-child(1)', None, None, 1.0),
    '4.1':     ('按天减持明细', '#fBDatepair > input:nth-child(1)', None, None, 0.5),
    '4.2':     ('按天增持明细', '#fBDatepair > input:nth-child(1)', None, None, 0.5),
    '4.3':     ('减持汇总统计', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', None, 0.2),
    '4.4':     ('增持汇总统计', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', None, 0.2),
    '5.1':     ('股本情况', None, None, '.condition6 > select:nth-child(2)', 1.5),
    '5.2':     ('高管持股变动汇总', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', '.condition6 > select:nth-child(2)', 0.5),
    '5.3':     ('高管持股变动明细', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', '.condition6 > select:nth-child(2)', 1.0),
    '5.4':     ('实际控制人持股变动', None, None, '.condition6 > select:nth-child(2)', 1.0),
    '5.5':     ('股东人数及持股集中度', '#seee1_sele', '.condition2 > select:nth-child(2)', None, 1.0),
    '6.1':     ('业绩预告', '#seee1_sele', '.condition2 > select:nth-child(2)', None, 1.0),
    '6.2':     ('预告业绩扭亏个股', '#seee1_sele', '.condition2 > select:nth-child(2)', None, 1.0),
    '6.3':     ('预告业绩大幅下降个股', '#seee1_sele', '.condition2 > select:nth-child(2)', None, 1.0),
    '6.4':     ('预告业绩大幅上升个股', '#seee1_sele', '.condition2 > select:nth-child(2)', None, 1.0),
    '7.1':     ('个股定报主要指标', '#seee1_sele', '.condition2 > select:nth-child(2)', None, 3.5),
    '7.2':     ('地区财务指标明细', '#seee1_sele', '.condition2 > select:nth-child(2)', '.condition6 > select:nth-child(2)', 2.0),
    '7.3':     ('行业财务指标明细', '#seee1_sele', '.condition2 > select:nth-child(2)', '.condition6 > select:nth-child(2)', 2.0),
    '7.4':     ('分市场财务指标明细', '#seee1_sele', '.condition2 > select:nth-child(2)', None, 2.0),
    '8.1':     ('地区分红明细', None, None, '.condition6 > select:nth-child(2)', 1.0),
    '8.2':     ('行业分红明细', None, None, '.condition6 > select:nth-child(2)', 1.0),
    '9.1':     ('股东大会召开情况', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', None, 1.0),
    '9.2':     ('股东大会相关事项变动', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', None, 1.0),
    '9.3':     ('股东大会议案表', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', None, 2.0),
    '10.1':    ('停复牌', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', None, 0.2),
    '10.2':    ('市场公开信息汇总', '#fBDatepair > input:nth-child(1)', None, None, 1.0),
    '11.1':    ('公司债或可转债', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', '.condition6 > select:nth-child(2)', 1.0),
    '11.2':    ('增发筹资', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', None, 0.5),
    '11.3':    ('配股筹资', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', None, 0.5),
    '11.4':    ('首发筹资', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', None, 0.5),
    '12.1':    ('拟上市公司清单', None, None, None, 0.1),
    '12.2':    ('暂停上市公司清单', None, None, None, 0.1),
    '12.3':    ('终止上市公司清单', None, None, None, 0.1),
    '13.1':    ('首发审核', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', '.condition6 > select:nth-child(2)', 0.2),
    '13.2':    ('资产重组', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', '.condition6 > select:nth-child(2)', 0.2),
    '13.3':    ('债务重组', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', None, 0.2),
    '13.4':    ('吸收合并', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', None, 0.2),
    '13.5':    ('股权变更', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', '.condition6 > select:nth-child(2)', 0.2),
    '13.6':    ('公司诉讼', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', '.condition6 > select:nth-child(2)', 0.2),
    '13.7':    ('对外担保', '#dBDatepair > input:nth-child(1)', 'input.date:nth-child(2)', '.condition6 > select:nth-child(2)', 0.2),
    '14.1':    ('行业市盈率', '#fBDatepair > input:nth-child(1)', None, '.condition6 > select:nth-child(2)', 2.0),
    '15.1':    ('基金净值增长率', '#fBDatepair > input:nth-child(1)', None, '.condition6 > select:nth-child(2)', 2.0),
    '16.1':    ('投资评级', '#fBDatepair > input:nth-child(1)', None, None, 2.0),
}

PAGINATION_PAT = re.compile(r'共\s(\d{1,})\s条记录')


def _concat(dfs):
    try:
        return pd.concat(dfs)
    except ValueError:
        return pd.DataFrame()


def _split_start_and_end(df):
    df['公告日期'] = df['公告统计区间'].map(lambda x: x.split('--')[0])
    del df['公告统计区间']
    return df


class ThematicStatistics(SZXPage):
    """深证信专题统计api"""

    root_nav_css = '.thematicStatistics-tree'
    view_selection = {1: 20, 2: 50, 3: 100, 4: 200}
    level_maps = LEVEL_MAPS
    current_t1_css = ''        # 开始日期css
    current_t2_css = ''        # 结束日期css
    current_t1_value = ''      # 开始日期
    current_t2_value = ''      # 结束日期

    def __init__(self, clear_cache=False, **kwds):
        start = time.time()
        super().__init__(clear_cache=clear_cache, **kwds)
        # 首页内容加载完成，显示页数
        # check_loaded_css = '.fixed-table-footer'
        check_loaded_css = '#apiName2'
        try:
            self._switch_to(8, check_loaded_css)
        except TimeoutException:
            self.driver.implicitly_wait(2)
            self._switch_to(8, check_loaded_css)
        self.driver.implicitly_wait(1)
        self.logger.notice(f'加载主页用时：{(time.time() - start):>0.2f}秒')

    def _set_date_css_by(self, level):
        """根据数据指标，确定t1与t2的css"""
        self.current_t1_css = self.level_maps[level][1]
        self.current_t2_css = self.level_maps[level][2]

    def _data_item_related(self, level):
        # 设置t1、t2定位格式
        self._set_date_css_by(level)

    def _select_level(self, level):
        """选定数据项目"""
        assert level in self.level_maps.keys(
        ), f'数据搜索指标导航可接受范围：{self.level_maps}'
        ops.select_level(self, self.root_nav_css, level, True)

    def _change_t1_value(self, t1):
        """更改查询t1值"""
        # 因为更改项目后，t值清零。所以无论是否更改前值，都需要重置。
        if self.current_t1_css:
            ## 输入日期字符串时
            if 'input' in self.current_t1_css:
                self._datepicker(self.current_t1_css, t1)
                self.current_t1_value = t1
            elif 'sele' in self.current_t1_css:
                # css_id = self.current_t1_css[1:6]
                # self._change_year(css_id, t1)
                self._change_year(self.current_t1_css, t1)
                self.current_t1_value = t1

    def _change_t2_value(self, t2):
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
                select.select_by_index(t2 - 1)
                self.current_t2_value = t2

    def query(self, indicator, t1=None, t2=None):
        """专题统计查询
            
        专题统计从2个维度切分数据
        1. 指标:要搜索的数据项目
        2. 时间:限定开始日期至结束日期之间的期间
        
        第三项内部实现循环

        Arguments:
            indicator {str} -- 指标名称或对应的指标层级
        
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
        self.driver.implicitly_wait(0.5)
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
        if level in ('13.6', '13.7') and not df.empty:
            df = _split_start_and_end(df)
        return df

    def _loop_all(self, level):
        """循环读取所有可选项目数据"""
        if self.current_level in ('5.1', '5.4', '13.1', '13.2', '13.5', '13.6', '13.7'):
            return self._only_all(level)
        css = LEVEL_MAPS[level][3]
        if css:
            dfs = []
            elem = self.driver.find_element_by_css_selector(css)
            select = Select(elem)
            options = select.options
            for o in options:
                self.logger.info(f'{o.text}')
                select.select_by_visible_text(o.text)
                data = self._read_html_table()
                dfs.append(data)
            df = _concat(dfs)
        else:
            df = self._read_html_table()
        return df

    def _only_all(self, level):
        """只选`全部`选项"""
        css = LEVEL_MAPS[level][3]
        elem = self.driver.find_element_by_css_selector(css)
        select = Select(elem)
        select.select_by_value("")
        # self.driver.implicitly_wait(1)
        return self._read_html_table()

    def _data_browse(self):
        """预览数据"""
        btn_css = '.thematicStatisticsBtn'
        if any(LEVEL_MAPS[self.current_level][1:4]):
            # 12.1 12.2 12.3 没有查询按钮
            btn = self.driver.find_element_by_css_selector(btn_css)
            btn.click()
        sleep_time = LEVEL_MAPS[self.current_level][4]
        self.driver.implicitly_wait(sleep_time)

    def _get_response_status(self):
        """三种状态：
            1. 无数据
            2. 失败(如果网络故障或数据量太大)
            3. 完成
        """
        try:
            self.driver.find_element_by_css_selector('.no-records-found')
            return ops.ResponseStatus.nodata
        except ops.NoSuchElementException:
            pass
        css = '.tip'
        elem = self.driver.find_element_by_css_selector(css)
        if elem.get_attribute('style') == 'display: inline;':
            return ops.ResponseStatus.failed
        else:
            css = '.pagination-info'
            locator = (By.CSS_SELECTOR, css)
            m = EC.visibility_of_element_located(locator)
            self.wait.until(m, message='预览响应超时')
            return ops.ResponseStatus.completed

    def _read_html_table(self):
        """读取当前网页数据表"""
        self._data_browse()
        status = self._get_response_status()
        if status == ops.ResponseStatus.nodata:
            return pd.DataFrame()
        elif status == ops.ResponseStatus.failed:
            raise ConnectionError('网络连接异常或单次请求的数据量太大')

        # 读取分页信息
        pagination_css = '.pagination-info'
        pagination = self.driver.find_element_by_css_selector(pagination_css)
        try:
            total = int(re.search(PAGINATION_PAT, pagination.text).group(1))
            self.logger.notice(f"共{total}条记录")
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
        na_values = ['-', '无']
        for i in range(1, self.page_num + 1):
            df = pd.read_html(self.driver.page_source, na_values=na_values)[0]
            dfs.append(df)
            # 点击进入下一页
            if i + 1 <= self.page_num:
                next_page = self.driver.find_element_by_link_text(str(i + 1))
                next_page.click()
                self.driver.implicitly_wait(0.2)
            self.logger.info('>>  分页进度 第{}页/共{}页'.format(i, self.page_num))
        return _concat(dfs)
