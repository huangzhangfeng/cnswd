"""测试有关深证信api辅助工具部分
"""
import unittest
from parameterized import parameterized

import re
from cnswd.scripts.szx import sql
import pandas as pd


class SZXColumnConvertTestCase(unittest.TestCase):
    """深证信数据列名称转换"""
    @parameterized.expand([
        ('成交时间', True),
        ("交易日期", True),
        ('报告年度', True),
        ('除权日', True),
        ("今日开盘价", False),
        ('5日平均MA5', False),
        ('本年度数量', False),
    ])
    def test_find_date_col(self, test_input, expected):
        """测试寻找日期列"""
        if expected:
            self.assertTrue(re.search(sql.DATE_COL_PAT, test_input))
        else:
            self.assertFalse(re.search(sql.DATE_COL_PAT, test_input))

    @parameterized.expand([
    # 尾部无效字符
        ('现金及现金等价物净变动情况：', '现金及现金等价物净变动情况'),
        ('现金及现金等价物净变动情况_', '现金及现金等价物净变动情况'),
        # 数字开头
        ('1、现金及现金等价物净变动情况', 'A_现金及现金等价物净变动情况'),
        ('3、现金及现金等价物净变动情况', 'C_现金及现金等价物净变动情况'),
        ('四、汇率变动对现金的影响', '四_汇率变动对现金的影响'),
        # 加、减、其中带`：`
        ('加：现金等价物的期初余额', '加_现金等价物的期初余额'),
        ('减：现金等价物的期初余额', '减_现金等价物的期初余额'),
        ('其中：现金等价物的期初余额', '其中_现金等价物的期初余额'),
        # 固定替换
        ('5日平均（MA5）', 'MA5'),
        ('30日平均（MA30）', 'MA30'),
        ('52周均价（360日）均价', 'MA360'),
        # 特殊字符统一替换 -> `_`
        ("EV/EBITDA", "EV_EBITDA"),    
    ])
    def test_fix_col_name(self, test_input, expected):
        """测试修复列名称"""
        self.assertEqual(sql._fix_col_name(test_input), expected)
