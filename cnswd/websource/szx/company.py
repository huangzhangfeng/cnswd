"""

深证信公司快照模块

"""
from cnswd.websource.szx.base import SZXPage

LEVEL_MAPS = {
    '1.1'  :   ('基本资料',),
    '1.2'  :   ('公司简介',),
    '1.3'  :   ('公司高管',),
    '1.4'  :   ('前十大股东',),
    '1.5'  :   ('前十大流通股东',),
    '1.6'  :   ('交易信息',),
    '2'     :   ('历史行情',),
    '3'     :   ('历史分红',),
    '4'     :   ('主要指标',),
    '5.1'  :   ('利润表',),
    '5.2'  :   ('资产负债表',),
    '5.3'  :   ('现金流量表',),
}

class Company(SZXPage):
    """深证信公司快照api"""

    root_nav_css = '.company-tree'

    def __init__(self):
        super(Company, self).__init__()
        self._switch_to(9, 'li.tree-img:nth-child(4)')

    def _select_level(self, level):
        """选定数据项目"""
        # 重写此方法
        assert level in LEVEL_MAPS.keys(), '公司快照层级可接受范围:{}'.format(LEVEL_MAPS)
        # 公司快照左侧最多二层
        levels = level.split('.')
        temp = []
        for i in levels:
            temp.append('ul > li:nth-child({})'.format(i))
            css = ' > '.join([self.root_nav_css] + temp)
            self.driver.find_element_by_css_selector(css).click()

    def _data_item_related(self, level):
        pass
