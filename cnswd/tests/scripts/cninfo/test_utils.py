import unittest
import pandas as pd
from cnswd.scripts.cninfo.units import _fix_date

class FixTestCase(unittest.TestCase):
    """测试修复工具"""

    def test_fix_date(self):
        """测试修复日期列"""
        df = pd.DataFrame(
            {
                '上市日期':['2019-06-27'],
                '股东决议有效期截止日':['2019-06-27'],
                '停牌时间':['2019-06-27'],
                '报告年度':['2019-06-27'],
            }
        )
        fixed = _fix_date(df)
        for c in fixed.columns:
            self.assertTrue(c, pd.api.types.is_datetime64_ns_dtype(fixed[c]))
