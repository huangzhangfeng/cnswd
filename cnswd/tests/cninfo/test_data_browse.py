import unittest
import time
import pandas as pd
from cnswd.websource.cninfo.data_browse import DataBrowse
from cnswd.websource.cninfo.constants import DB_NAME
from pandas.testing import assert_frame_equal


class DataBrowserTestCase(unittest.TestCase):
    """深证信专题统计测试"""

    def setUp(self):
        self.api = DataBrowse()

    def tearDown(self):
        self.api.driver.quit()

    def test_select_level(self):
        """测试项目选择"""
        for level in ('3.1', '8.3.1'):
            self.api._select_level(level)
            id_name = 'apiName'
            expected = DB_NAME[level][0]
            actual = self.api.driver.find_element_by_id(id_name).text
            self.assertEqual(expected, actual)

    def test_choose_data_fields(self):
        """测试全选字段"""
        # 上市公司业绩预告输出数据，全选字段，列应为13。
        level = '5.1'
        self.api.select_level(level)
        field_label_css = 'div.select-box:nth-child(2) > div:nth-child(1) > label:nth-child(1)'
        field_btn_css = 'div.arrows-box:nth-child(3) > div:nth-child(1) > button:nth-child(1)'
        num_css = 'div.select-box:nth-child(4) > div:nth-child(1) > span:nth-child(2) > i:nth-child(1)'
        self.api._add_or_delete_all(field_label_css, field_btn_css)
        expected = 13
        actual = int(
            self.api.driver.find_element_by_css_selector(num_css).text)
        self.assertEqual(expected, actual)

    def test_get_6_1_data(self):
        """测试读取股票分红数据"""
        # 限制股票数量
        self.api.add_codes(['000001', '000002', '002024', '600000'])
        # 读取2017年的分红送转数据
        actual = self.api.get_data('6.1', '2017-01-01', '2017-12-31')
        expected = pd.read_csv('tests/cninfo/db_6_1.csv')
        assert_frame_equal(actual, expected)
