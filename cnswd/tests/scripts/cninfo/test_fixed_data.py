import unittest
import pandas as pd
from cnswd.scripts.cninfo.units import fixed_data
from cnswd.sql.data_browse import PeriodlyCashFlowStatement
from numpy.testing import assert_array_almost_equal


class FixedDataTestCase(unittest.TestCase):
    """测试修复后的数据是否正确"""

    def test_db_3_1(self):
        """测试修复行情数据"""
        data = pd.read_csv('tests/cninfo/db_3_1.csv')
        origin = data.copy()
        actual = fixed_data(data, '3.1', 'db')
        # 名称更改，数值不变
        origin_cols = ["5日平均（MA5）", "10日平均（MA10）",
                       "30日平均（MA30）", "120日均价", "EV/EBITDA", "52周均价（360日）均价"]
        new_names = ["MA5", "MA10", "MA30", "MA120", "EV_EBITDA", "MA360"]
        for c1, c2 in zip(origin_cols, new_names):
            assert_array_almost_equal(origin[c1], actual[c2])

    def test_db_8_3_3(self):
        """测试修复现金流量表"""
        data = pd.read_csv('tests/cninfo/db_8_3_3.csv')
        # origin = data.copy()
        actual = fixed_data(data, '8.3.3', 'db')
        tab_cols = PeriodlyCashFlowStatement.__table__.columns.keys()
        # 测试列名称与表列名称完全相符
        for c in actual.columns:
            self.assertIn(c, tab_cols)
        date_cols = ['公告日期', '开始日期', '截止日期', '报告年度']
        # 测试日期类型是否正确转换
        for c in date_cols:
            self.assertTrue(pd.api.types.is_datetime64_ns_dtype(actual[c]))
        # 测试代码类型及值是否正确
        self.assertTrue(pd.api.types.is_object_dtype(actual['证券代码']))
        self.assertTrue(all(actual['证券代码'].map(len) == 6))
