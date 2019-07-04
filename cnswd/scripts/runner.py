# 废弃
import math
import os
import time
import multiprocessing as mp
from selenium.common.exceptions import TimeoutException
from cnswd.websource.exceptions import RetryException
import logbook


max_worker = 1 # max(1, int(os.cpu_count()/2))
logger = logbook.Logger('runner')


class TryToCompleted(object):
    """多进程多次尝试直至完成任务"""

    def __init__(self, func, iterable, before=(), end=(), retry_times=20, sleep=3):
        self._func = func
        self._iterable = iterable
        self._before = before
        self._end = end
        self._retry_times = retry_times
        self._sleep = sleep

    def __call__(self):
        mp.freeze_support()
        t_start = time.time()
        # 执行前置任务
        if self._before:
            for f in self._before:
                f()
        logger.info('Creating pool with %d processes\n' % max_worker)
        with mp.Manager() as manager:
            d = manager.dict({x: None for x in self._iterable})
            for i in range(self._retry_times):
                to_do = [l for l, s in d.items() if s is None]
                if len(to_do) == 0:
                    break
                logger.info(f"第{i+1}次尝试\n")
                with mp.Pool(max_worker) as pool:
                    tasks = [(self._do_task, (l, d))
                             for l in to_do]
                    pool.map(self._executestar, tasks)
                    self._report(d, i)
        # 执行后置任务
        if self._end:
            for f in self._end:
                f()
        logger.notice(f'总用时：{(time.time() - t_start):.2f}秒')

    def _executestar(self, args):
        return self._execute(*args)

    def _execute(self, func, args):
        func(*args)

    def _do_task(self, one, d):
        """执行单一任务"""
        try:
            # 正常执行，在共享字典`d`标注状态`完成`
            self._func(one)
            d[one] = True
        except (RetryException, TimeoutException):
            # 超时或者重试异常执行，在共享字典`d`标注状态`None`
            d[one] = None
            time.sleep(self._sleep)
        except Exception as e:
            # 其他异常，在共享字典`d`标注状态`e`
            d[one] = f"{e}"

    def _report(self, d, i):
        """报告执行状态"""
        msg = f"第{i+1}次尝试结果：\n"
        for k, v in d.items():
            if v is None:
                msg += f'项目：{k} 需要重试 \n'
            elif v == True:
                msg += f'项目：{k} 已经完成 \n'
            else:
                msg += f'项目：{k} {d[k]} \n'
        print(msg)
