import unittest

import time

from cnswd.websource import ThematicStatistics
from cnswd.websource.szx.thematic_statistics import LEVEL_MAPS
from . utils import get_expected_path, get_actual_path, is_png_equal


class ThematicStatisticsTestCase(unittest.TestCase):
    """深证信专题统计测试"""
    def setUp(self):
        self.api = ThematicStatistics()

    def tearDown(self):
        self.api.driver.quit()

    def test_select_level(self):
        """测试项目选择"""
        for level in ('3.1','13.1'):
            self.api._select_level(level)
            id_name = 'apiName2'
            expected = LEVEL_MAPS[level][0]
            actual = self.api.driver.find_element_by_id(id_name).text
            self.assertEqual(expected, actual)

    def test_select_level_by_screenshot(self):
        """使用快照测试数据搜索项目选择"""
        for level in ('10.1', '13.2'):
            self.api._select_level(level)
            # 确保网页内容完全加载
            time.sleep(0.3)
            p1 = get_expected_path(8, level)
            p2 = get_actual_path(8, level)
            self.api.driver.save_screenshot(p2)
            self.assertTrue(is_png_equal(p1, p2))

    def test_set_query_year(self):
        """测试设置查询年度"""
        # 股东人数及持股集中度
        year = 2013 # 不在可选项中
        df = self.api.get_concentration(year,2)
        self.assertTupleEqual(df.shape, (3582, 9))
        self.assertTrue(df['变动日期'].apply(lambda x:x[:4] == str(year)).all())
        self.assertTrue(df['变动日期'].apply(lambda x: 4 <= int(x[4:6]) <= 6).all())