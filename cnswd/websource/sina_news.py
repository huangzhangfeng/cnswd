"""新浪24*7财经新闻
"""
from cnswd.websource._selenium import make_headless_browser
import time
import logbook


logger = logbook.Logger('财经新闻')

TOPIC_MAPS = {
    1: '宏观',
    2: '行业',
    3: '公司',
    4: '数据',
    5: '市场',
    6: '观点',
    7: '央行',
    8: '其他',
    10: 'A股',
}


class Sina247News(object):
    def __init__(self):
        self.url_fmt = 'http://finance.sina.com.cn/7x24/?tag={}'
        self.driver = make_headless_browser()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.driver.quit()

    def scrolling(self):
        # 每次递增20条
        self.driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);")

    def _turn_off(self):
        """关闭声音提醒及自动更新"""
        csses = ['.soundswitch', '.autorefreshlbtxt']
        for css in csses:
            elem = self.driver.find_element_by_css_selector(css)
            if elem.is_selected():
                elem.click()
                time.sleep(0.1)

    def _parse(self, div, tag):
        """解析单个消息内容"""
        # 编号、日期(20180901)、时间('22:31:44')、概要
        ps = div.find_elements_by_tag_name('p')
        return (
            div.get_attribute('data-id'),
            div.get_attribute('data-time'),
            ps[0].text,
            ps[1].text,
            TOPIC_MAPS[tag],
        )

    @property
    def live_data(self):
        """最新消息，默认滚动三次"""
        res = []
        for tag in TOPIC_MAPS.keys():
            res.extend(self._get_topic_news(tag, times=3))
        return res

    def _get_topic_news(self, tag, times):
        """获取分类消息"""
        url = self.url_fmt.format(tag)
        self.driver.get(url)
        # 每个栏目都需要关闭
        self._turn_off()
        div_css = 'div.bd_i'
        for i in range(times):
            if (i+1) % 100 == 0:
                time.sleep(0.5)
            self.scrolling()
            time.sleep(0.25)
            logger.info(f'当前栏目：{TOPIC_MAPS[tag]} 第{i+1:>4}页')
        # 滚动完成后，一次性读取div元素
        divs = self.driver.find_elements_by_css_selector(div_css)
        return [self._parse(div, tag) for div in divs]

    def history_news(self, times):
        """历史财经新闻"""
        res = []
        for tag in TOPIC_MAPS.keys():
            logger.info(f'当前栏目：{TOPIC_MAPS[tag]}')
            res.extend(self._get_topic_news(tag, times))
        return res
