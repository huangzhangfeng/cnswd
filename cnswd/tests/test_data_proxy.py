import unittest
from parameterized import parameterized
from pandas.testing import assert_frame_equal

import os
import time
import pandas as pd
from cnswd.constants import MARKET_START
from cnswd.data_proxy import DataProxy, last_modified_time,next_update_time


def fake_fetch(*args, **kwargs):
    # 模拟下载数据
    return pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})


def _make_time_str(add_seconds=1):
    time_ = (pd.Timestamp('now') + pd.Timedelta(seconds=add_seconds)).time()
    time_str = '{}:{}:{}'.format(time_.hour, time_.minute, time_.second)
    return time_str


class DataproxyTestCase(unittest.TestCase):
    def setUp(self):
        self.sleep_seconds = sleep_seconds = 1
        time_str = _make_time_str(sleep_seconds)
        kwargs = {'code': '000001', 'start': '2017-11-10',
                  'end': '2017-11-21', 'is_index': False}
        history_data_reader = DataProxy(fake_fetch, time_str, freq='S')
        self.kwargs = kwargs
        self.reader = history_data_reader

    def test_auto_refresh(self):
        """测试数据过期后自动刷新网络数据"""
        local_path = self.reader.get_cache_file_path(**self.kwargs)
        if os.path.exists(local_path):
            os.remove(local_path)
        # 读取数据后，此时存储在本地文件中
        df_1 = self.reader.read(**self.kwargs)
        time_1 = last_modified_time(local_path)
        # 确保过期
        time.sleep(self.sleep_seconds + 0.01)

        # 再次读取时，数据已经过期。重新下载后，本地文件的时间发生变动
        df_2 = self.reader.read(**self.kwargs)
        time_2 = last_modified_time(local_path)

        delta = time_2 - time_1

        # self.assertNotEqual(time_2, time_1)
        self.assertGreaterEqual(delta.seconds, self.sleep_seconds)

        assert_frame_equal(df_1, df_2)

    def test_cache_file_name(self):
        # 使用函数默认值时，尽管意义相同，结果一样，hash表示不一样
        # 最终导致存储路径发生变化而重复下载
        # 使用数据代理类时，要注意保持函数参数写法的一致性
        same_kwargs = {'code': '000001',
                       'start': '2017-11-10', 'end': '2017-11-21'}
        path_1 = self.reader.get_cache_file_path(**self.kwargs)
        path_2 = self.reader.get_cache_file_path(**same_kwargs)

        self.assertNotEqual(path_1, path_2)

        df_1 = self.reader.read(**self.kwargs)
        df_2 = self.reader.read(**same_kwargs)

        assert_frame_equal(df_1, df_2)

    def test_need_refresh(self):
        """测试刷新判断"""
        proxy = DataProxy(fake_fetch, freq='H')
        local_path = self.reader.get_cache_file_path(**self.kwargs)
        if os.path.exists(local_path):
            os.remove(local_path)
        # 确保已经下载
        proxy.read(**self.kwargs)
        now = pd.Timestamp('now', tz='Asia/Shanghai') + pd.Timedelta(minutes=3)
        nd = proxy.need_refresh(now, **self.kwargs)
        self.assertFalse(nd)

    # visual studio 中无法完成测试
    # 在测试文件目录下，使用命令行`python -m unittest -v`
    @parameterized.expand([
        ('D',  pd.Timestamp('2018-8-29', tz='Asia/Shanghai'),  18,    0,  pd.Timestamp('2018-8-30 18', tz='Asia/Shanghai')),
        ('D',  pd.Timestamp('2018-8-29', tz='Asia/Shanghai'),    9,  30,  pd.Timestamp('2018-8-30 09:30', tz='Asia/Shanghai')),
        ('W', pd.Timestamp('2018-8-29', tz='Asia/Shanghai'),    0,   0,   pd.Timestamp('2018-9-3', tz='Asia/Shanghai')),
        ('M', pd.Timestamp('2018-8-29', tz='Asia/Shanghai'),    0,   0,   pd.Timestamp('2018-9-1', tz='Asia/Shanghai')),
        ('Q',  pd.Timestamp('2018-8-29', tz='Asia/Shanghai'),   0,   0,   pd.Timestamp('2018-10-1', tz='Asia/Shanghai')),
        ('Q',  None,                                                                      0,   0,  MARKET_START),
    ])
    def test_next_update_time(self, freq, input_,hour, minute, expected):
        self.assertEqual(next_update_time(input_, freq, hour, minute),expected)
