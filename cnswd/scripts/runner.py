import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import logbook

from cnswd.utils import loop_codes

max_worker = int(os.cpu_count()/2)

logger = logbook.Logger('runner')


def batch_codes(iterable):
    """切分可迭代对象，返回长度cpu_count()/2批次列表"""
    batch_num = math.ceil(len(iterable) / max_worker)
    return loop_codes(iterable, batch_num)


class TryToCompleted(object):
    """多线程尝试直至完成"""

    def __init__(self, func, iterable, kws={}, retry_times=30, sleep=3):
        self._func = func
        self._iterable = iterable
        self._kws = kws
        self._retry_times = retry_times
        self._sleep = sleep

    def run(self):
        """内部运行过程，直至完成"""
        t_start = time.time()
        retry = []
        for i in range(self._retry_times):
            if i == 0:
                to_do = batch_codes(self._iterable)
            else:
                # 此时只需要处理上次异常部分
                if len(retry) == 0:
                    break
                to_do = batch_codes(retry)
                retry = []
            start = time.time()
            with ThreadPoolExecutor(max_worker) as executor:
                future_to_codes = {executor.submit(
                    self._func, codes, **self._kws): codes for codes in to_do}
                for future in as_completed(future_to_codes):
                    codes = future_to_codes[future]
                    try:
                        future.result()
                    except Exception as e:
                        retry.extend(codes)
                        logger.error('%s' % (e,))
            logger.notice(
                f'第{i+1}次尝试，用时：{(time.time() - start):.2f}秒，剩余{len(retry)}')
            time.sleep(self._sleep+i)
        if len(retry):
            logger.warn(f'经过{self._retry_times}次尝试，以下尚未完成：{sorted(retry)}')
        logger.notice(f'总用时：{(time.time() - t_start):.2f}秒')
