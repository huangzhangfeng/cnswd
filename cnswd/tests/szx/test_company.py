import unittest
import time

from cnswd.websource import Company
from cnswd.websource.szx.company import LEVEL_MAPS

from . utils import get_expected_path, get_actual_path, is_png_equal

class CompanyTestCase(unittest.TestCase):
    """深证信专题统计测试"""
    def setUp(self):
        self.api = Company()

    def tearDown(self):
        self.api.driver.quit()

    def test_select_level_by_screenshot(self):
        """使用快照测试数据搜索项目选择"""
        for level in ('5.3', '3'):
            self.api._select_level(level)
            # 确保网页内容完全加载
            time.sleep(0.3)
            p1 = get_expected_path(9, level)
            p2 = get_actual_path(9, level)
            self.api.driver.save_screenshot(p2)
            self.assertTrue(is_png_equal(p1, p2))

