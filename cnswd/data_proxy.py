"""
某些网络数据在一定周期内不会发生变化。如股票代码列表。如果在
同一个会话中，使用funtools的cache机制，可以避免多次下载同
一数据。但计划任务程序中，由于每次运行会话不同，如果需要使
用网络数据，就需要多次从网络下载。数据代理类使用本地文件持
久化存储，在特点时点自动更新，确保数据时效性，同时减少网络
下载，提高运行效率。

数据代理主要用于计划任务程序。适用于每天变动，但在24小时内，
数据一直为静态的网络数据采集。如股票列表等。

也可用于频繁访问，但单次查询需要较长时间的数据提取。

读取代理数据，直接使用类的`read`方法，少数需要进一步转换。

好处：
    1、避免当天重复下载同一网络数据
    2、时间点数据统一

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import pickle
import pandas as pd
from pandas.tseries.offsets import BDay, Week, MonthBegin, QuarterBegin, Hour, Minute, Second
from hashlib import md5
from six import iteritems
import logbook

from cnswd.constants import MARKET_START
from cnswd.utils import data_root

logger = logbook.Logger(__name__)

TEMP_DIR = data_root('webcache')

DEFAULT_TIME_STR = '18:00:00'  # 网站更新数据时间
DEFAULT_FREQ = 'D'


def hash_args(*args, **kwargs):
    """Define a unique string for any set of representable args."""
    arg_string = '_'.join([str(arg) for arg in args])
    kwarg_string = '_'.join([str(key) + '=' + str(value)
                             for key, value in iteritems(kwargs)])
    combined = ':'.join([arg_string, kwarg_string])
    hasher = md5()
    hasher.update(combined.encode('utf-8'))
    return hasher.hexdigest()


def last_modified_time(path):
    """
    Get the last modified time of path as a Timestamp.

    Notes:
    ------
        舍弃秒以下单位的数字
    """
    return pd.Timestamp(int(os.path.getmtime(path)), unit='s', tz='Asia/Shanghai')


def next_update_time(last_updated, freq='D', hour=18, minute=0, second=0):
    """计算下次更新时间
    说明：
        'S'：移动到下一秒
        'm'：移动到下一分钟
        'H'：移动到下一小时
        'D'：移动到下一天
        'W'：移动到下周一
        'M'：移动到下月第一天
        'Q'：下一季度的第一天
        将时间调整到指定的hour和minute
    """
    if pd.isnull(last_updated):
        return MARKET_START
    if freq == 'S':
        off = Second()
        return last_updated + off
    elif freq == 'm':
        off = Minute()
        return last_updated + off
    elif freq == 'H':
        off = Hour()
        return last_updated + off
    elif freq == 'D':
        d = BDay(n=1, normalize=True)
        res = last_updated + d
        return res.replace(hour=hour, minute=minute, second=second)
    elif freq == 'W':
        w = Week(normalize=True, weekday=0)
        res = last_updated + w
        return res.replace(hour=hour, minute=minute, second=second)
    elif freq == 'M':
        m = MonthBegin(n=1, normalize=True)
        res = last_updated + m
        return res.replace(hour=hour, minute=minute, second=second)
    elif freq == 'Q':
        q = QuarterBegin(normalize=True, startingMonth=1)
        res = last_updated + q
        return res.replace(hour=hour, minute=minute, second=second)
    else:
        raise TypeError('不能识别的周期类型，仅接受{}'.format(
            ('S', 'm', 'H', 'D', 'W', 'M', 'Q')))


class DataProxy(object):

    def __init__(self, fetch_fun, time_str=None, freq=None):
        self._fetch_fun = fetch_fun
        if time_str:
            self._time_str = time_str
        else:
            self._time_str = DEFAULT_TIME_STR
        if freq:
            self._freq = freq
        else:
            self._freq = DEFAULT_FREQ
        # 验证
        self._validate()
        self._ensure_root_dir()

    def _validate(self):
        assert isinstance(self._freq, str), 'freq必须是str实例'
        assert isinstance(self._time_str, str), 'time_str必须是str实例'
        assert ':' in self._time_str, 'time_str要包含":"字符'
        parts = self._time_str.split(':')
        assert len(parts) == 3, '时间字符串格式为"小时:分钟:秒"'
        assert hasattr(self._fetch_fun, '__call__'), '{}必须是函数'.format(
            self._fetch_fun)

    def _ensure_root_dir(self):
        """确保函数根目录存在"""
        subdir = os.path.join(TEMP_DIR, self._fetch_fun.__name__)
        if not os.path.exists(subdir):
            os.makedirs(subdir)
        return subdir

    def get_cache_file_path(self, *args, **kwargs):
        """获取本地缓存文件路径"""
        name = '{}'.format(hash_args(*args, **kwargs))
        subdir = self._ensure_root_dir()
        file_path = os.path.join(TEMP_DIR, subdir, name)
        return file_path

    def need_refresh(self, now, *args, **kwargs):
        if now.tz is None:
            now = now.tz_localize('Asia/Shanghai')
        file_path = self.get_cache_file_path(*args, **kwargs)
        if not os.path.exists(file_path):
            return True
        else:
            last_time = last_modified_time(file_path)
            next_time = self.expiration
            # 如now介于二者之间，则无需要刷新
            return not (last_time < now < next_time)

    @property
    def expiration(self):
        """将时间字符串转换为时间戳"""
        parts = self._time_str.split(':')
        hour = int(parts[0])
        minute = int(parts[1])
        second = int(parts[2])
        now = pd.Timestamp('now', tz='Asia/Shanghai').normalize()
        next_time = next_update_time(now, self._freq, hour, minute)
        next_time = next_time.replace(hour=hour, minute=minute, second=second)
        return next_time

    def read(self, *args, **kwargs):
        """读取网页数据。如果存在本地数据，使用缓存；否则从网页下载。"""
        now = pd.Timestamp('now', tz='Asia/Shanghai')
        file_path = self.get_cache_file_path(*args, **kwargs)
        download_from_web = self.need_refresh(now, *args, **kwargs)
        if download_from_web:
            try:
                data = self._fetch_fun(*args, **kwargs)
                with open(file_path, 'wb') as f:
                    pickle.dump(data, f)
            except Exception as e:
                raise e
        with open(file_path, 'rb') as f:
            return pickle.load(f)
