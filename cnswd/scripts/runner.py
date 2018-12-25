import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import logbook

from cnswd.utils import loop_codes

max_worker = int(os.cpu_count()/2)

logger = logbook.Logger('runner')


def batch_codes(iterable):
    """
    切分可迭代对象，返回长度max_worker*4批次列表

    说明：
        1. 初始化浏览器很耗时，且占有大量内存
        2. 切分目标主要是平衡速度与稳定性
    """
    min_batch_num = max_worker * 4
    batch_num = max(min_batch_num, math.ceil(len(iterable) / max_worker / 4))
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
        completed = []
        retry = {} # 记录重试次数
        for i in range(self._retry_times):
            if i == 0:
                to_do = batch_codes(self._iterable)
            else:
                # 此时只需要处理上次异常部分
                if len(to_do) == 0:
                    break
                to_do = batch_codes(to_do)
            start = time.time()
            with ThreadPoolExecutor(max_worker) as executor:
                future_to_codes = {executor.submit(
                    self._func, codes, **self._kws): codes for codes in to_do}
                for future in as_completed(future_to_codes):
                    codes = future_to_codes[future]
                    try:
                        future.result()
                        completed.extend(codes)                   
                    except Exception as e:
                        for code in codes:
                            num = retry.get(code, 0)
                            retry[code] = num + 1
                        logger.error('%s' % (e,))
            # 如果重试三次依然不成功，则忽略
            to_do = [c for c in retry.keys() if retry[c] <= 3 and c not in set(completed)]
            logger.notice(
                f'第{i+1}次尝试，用时：{(time.time() - start):.2f}秒，剩余：{to_do}')
            time.sleep(self._sleep)
        end = set(self._iterable) - set(completed)
        if len(end):
            logger.warn(f'经过{i+1}次尝试，以下尚未完成：{sorted(end)}')
        logger.notice(f'总用时：{(time.time() - t_start):.2f}秒')


