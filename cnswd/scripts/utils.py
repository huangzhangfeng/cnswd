"""

脚本辅助模块

"""
from __future__ import absolute_import, division, print_function

import os
import shutil
import psutil
import pandas as pd

from cnswd.constants import DB_DIR_NAME, DB_NAME, ROOT_DIR_NAME
from cnswd.sql.backup import Base as BackupBase
from cnswd.sql.base import db_path, get_engine
from cnswd.sql.info import Base as InfoBase
from cnswd.sql.szsh import Base as szshBase
from cnswd.sql.data_browse import Base as SZXBase
from cnswd.sql.thematic_statistics import Base as TSBase
from cnswd.utils import data_root


def create_tables(db_dir_name=DB_DIR_NAME, rewrite=False):
    """初始化表"""
    path = db_path(db_dir_name)
    if rewrite:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
    engine = get_engine(db_dir_name, echo=True)
    if db_dir_name.startswith('dataBrowse'):
        SZXBase.metadata.create_all(engine)
    elif db_dir_name.startswith('info'):
        InfoBase.metadata.create_all(engine)
    elif db_dir_name.startswith('szsh'):
        szshBase.metadata.create_all(engine)
    elif db_dir_name.startswith('thematicStatistics'):
        TSBase.metadata.create_all(engine)
    else:
        raise ValueError(f'不支持{db_dir_name}')


def is_trading_time():
    """判断当前是否为交易时段"""
    now = pd.Timestamp('now')
    if now.weekday() in (5, 6):
        return False
    current_time = now.time()
    am_start = pd.Timestamp('9:30').time()
    am_end = pd.Timestamp('11:30').time()
    pm_start = pd.Timestamp('13:00').time()
    pm_end = pd.Timestamp('15:00').time()
    is_am = am_start <= current_time <= am_end
    is_pm = pm_start <= current_time <= pm_end
    return is_am or is_pm


def remove_temp_files():
    """删除日志、缓存文件"""
    dirs = ['geckordriver', 'webcache', 'download']
    for d in dirs:
        path = data_root(d)
        try:
            shutil.rmtree(path)
        except PermissionError:
            # 可能后台正在使用中，忽略
            pass
        # 然后再创建该目录
        data_root(d)


def find_procs_by_name(name):
    "Return a list of processes matching 'name'."
    ls = []
    for p in psutil.process_iter(attrs=['name']):
        if p.info['name'] == name:
            ls.append(p)
    return ls


def kill_firefox():
    """杀死可能残留的进程"""
    for e in ['geckodriver.exe','firefox.exe']:
        for p in find_procs_by_name(e):
            try:
                p.kill()
            except psutil.NoSuchProcess:
                pass