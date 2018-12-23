import unittest
import time
import pandas as pd
from cnswd.websource import DataBrowser
from cnswd.websource.szx.data_browse import LEVEL_MAPS

from . utils import get_expected_path, get_actual_path, is_png_equal

class DataBrowserTestCase(unittest.TestCase):
    """深证信专题统计测试"""
    def setUp(self):
        self.api = DataBrowser()

    def tearDown(self):
        self.api.driver.quit()

    def test_select_level(self):
        """测试项目选择"""
        for level in ('3.1','8.3.1'):
            self.api._select_level(level)
            id_name = 'apiName'
            expected = LEVEL_MAPS[level][0]
            actual = self.api.driver.find_element_by_id(id_name).text
            self.assertEqual(expected, actual)

    def test_select_level_by_screenshot(self):
        """使用快照测试数据搜索项目选择"""
        # 不得更改顺序，否则因为折叠菜单问题，导致无法通过测试！！！
        for level in ('3.1','8.3.1'):
            self.api._select_level(level)
            # 确保网页内容完全加载
            time.sleep(0.3)
            p1 = get_expected_path(7, level)
            p2 = get_actual_path(7, level)
            self.api.driver.save_screenshot(p2)
            self.assertTrue(is_png_equal(p1, p2))

    def test_choose_data_fields(self):
        """测试全选字段"""
        # 上市公司业绩预告输出数据，全选字段，列应为13。
        self.api._select_level('5.1')
        expected = 13
        num_css = 'div.select-box:nth-child(4) > div:nth-child(1) > span:nth-child(2) > i:nth-child(1)'
        self.api._choose_data_fields()
        actual = int(self.api.driver.find_element_by_css_selector(num_css).text)
        self.assertEqual(expected, actual)

    def test_change_market_classify(self):
        """测试更改市场分类"""
        num_css = 'div.select-box:nth-child(1) > div:nth-child(1) > span:nth-child(2) > i:nth-child(1)'
        cates = ['深主板A', '中小板', '创业板', '沪市A']
        # 2018-11-18各市场股票数量，由于动态变化，测试时请修改实际值完成测试。
        expecteds = [464,919,736,1441]
        for expected, cate in zip(expecteds, cates):
            self.api._change_market_classify(cate)
            actual = int(self.api.driver.find_element_by_css_selector(num_css).text)
            self.assertEqual(actual, expected)
            #self.assertAlmostEqual(actual/100, expected/100, 2)

    def _get_dividend(self, year, codes):
        """读取分红指标"""
        level = '6.1'
        self.api._change_data_item(level)
        self.api._change_code(codes)
        css_id = 'se2'
        self.api._change_year(css_id, year)
        v_css = '.dataBrowseBtn'
        self.api.driver.find_element_by_css_selector(v_css).click()
        df = pd.read_html(self.api.driver.page_source)[0]
        return df

    def test_change_year_1(self):
        """测试设置查询年度"""
        codes = ['000001','000002']
        year = 2014 # 不在可选范围内
        df = self._get_dividend(year, codes)
        self.assertTupleEqual(df.shape, (4, 41))
        self.assertTrue(df['分红年度'].apply(lambda x:x[:4] == str(year)).all())

    def test_change_year_2(self):
        """测试设置查询年度"""
        year = pd.Timestamp('now').year-1 # 在可选范围内
        codes = ['000001','000002']
        df = self._get_dividend(year, codes)
        self.assertTupleEqual(df.shape, (4, 41))
        self.assertTrue(df['分红年度'].apply(lambda x:x[:4] == str(year)).all())