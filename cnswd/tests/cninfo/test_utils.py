import unittest
import pandas as pd
import re
from pandas.testing import assert_frame_equal
from numpy.testing import assert_array_almost_equal
from cnswd.scripts.cninfo.units import (
    _fix_date, _fix_code,
    parse_unit,
    _fix_num_unit, get_unit_dict,
    _remove_prefix_num, _remove_suffix_unit
)


class FixTestCase(unittest.TestCase):
    """测试修复工具"""

    def test_fix_date(self):
        """测试修复日期列"""
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

    def test_fix_stock_code(self):
        """测试修复股票代码"""
        # 假设字符已经正确表达股票代码
        # 如以字符形式的1，肯定是错误的
        origin = pd.DataFrame({
            '股票代码': ['000001-SZE', 1, '000001', 'K0101'],
            '证券代码': ['000001-SZE', 1, '000001', 'K0101'],
        })
        actual = _fix_code(origin)
        expected = pd.DataFrame({
            '股票代码': ['000001', '000001', '000001', 'K0101'],
            '证券代码': ['000001', '000001', '000001', 'K0101'],
        })
        assert_frame_equal(actual, expected)

    def test_parse_unit(self):
        """测试解析数量单位"""
        col_names = ['境内上市外资股（B股）', '境外上市外资股（H股）', '发行规模(万股)',
                     '网上有效申购资金(亿元)', '二级配售有效申购股数(亿股)',
                     '重组金额(单位：万元)',
                     '营业利润(单位：元)', '占总股本比例(单位：%)']
        expected = [None, None, 10000, 100000000.0,
                    100000000, 10000.0, 1.0, 0.01]
        for col, e in zip(col_names, expected):
            actual = parse_unit(col)
            self.assertEqual(actual.get(col, None), e)

    def test_fix_num_unit(self):
        """测试修复数量单位"""
        data = pd.read_csv('tests/cninfo/ts_2_2.csv')
        origin = data.copy()
        actual = _fix_num_unit(data)
        units = get_unit_dict(data)
        for col, adj in units.items():
            assert_array_almost_equal(origin[col] * adj, actual[col])

    def test_remove_prefix_num(self):
        """测试去除列名称中的前导数字"""
        origin = [
            '一年内到期的非流动资产',
            '三费合计',
            '（一）基本每股收益',
            '四(2)、其他原因对现金的影响',
            '四(2)、其他',
            '五、净利润',
            '(2)其中：折旧',
            '1、将净利润调节为经营活动现金流量：',
        ]
        expected = [
            '一年内到期的非流动资产',
            '三费合计',
            '基本每股收益',
            '其他原因对现金的影响',
            '其他',
            '净利润',
            '其中：折旧',
            '将净利润调节为经营活动现金流量：'
        ]
        actual = [_remove_prefix_num(x) for x in origin]
        self.assertListEqual(actual, expected)

    def test_remove_suffix_unit(self):
        """测试去除列名称尾部单位"""
        origin = ['境内上市外资股（B股）',
                  '其中：限售H股',
                  '发行规模(万股)',
                  '发行规模 万股',
                  '网上有效申购资金(亿元)',
                  '二级配售有效申购股数(亿股)',
                  '重组金额(单位：万元)',
                  '营业利润(单位：元)',
                  '占总股本比例(单位：%)']
        expected =['境内上市外资股（B股）',
                  '其中：限售H股',
                  '发行规模',
                  '发行规模',
                  '网上有效申购资金',
                  '二级配售有效申购股数',
                  '重组金额',
                  '营业利润',
                  '占总股本比例']
        actual = [_remove_suffix_unit(x) for x in origin]
        self.assertListEqual(actual, expected)
