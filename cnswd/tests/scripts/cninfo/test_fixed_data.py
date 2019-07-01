import unittest
import pandas as pd
from cnswd.scripts.cninfo.units import _fix_date


class FixedDataTestCase(unittest.TestCase):
    """测试修复工具"""
    # TODO：以下二者测试 列名称、数据类型、单位调整
    def test_db_6_1(self):
        """测试修复分红指标"""
        df = pd.DataFrame(
            {
                '上市日期': ['2019-06-27'],
                '股东决议有效期截止日': ['2019-06-27'],
                '停牌时间': ['2019-06-27'],
                '报告年度': ['2019-06-27'],
            }
        )
        fixed = _fix_date(df)
        for c in fixed.columns:
            self.assertTrue(c, pd.api.types.is_datetime64_ns_dtype(fixed[c]))

    def test_db_8_3_3(self):
        """测试修复现金流量表"""
        df = pd.DataFrame(
            {
                '上市日期': ['2019-06-27'],
                '股东决议有效期截止日': ['2019-06-27'],
                '停牌时间': ['2019-06-27'],
                '报告年度': ['2019-06-27'],
            }
        )
        fixed = _fix_date(df)
        for c in fixed.columns:
            self.assertTrue(c, pd.api.types.is_datetime64_ns_dtype(fixed[c]))
