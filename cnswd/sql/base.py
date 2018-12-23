import enum
import os
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from ..constants import DB_DIR_NAME, DB_NAME
from ..utils import data_root


def db_path(db_dir_name, db_name=DB_NAME, path_str=None):
    """数据库路径"""
    if path_str:
        return path_str
    db_dir = data_root(db_dir_name)
    return os.path.join(db_dir, db_name)


def get_engine(db_dir_name, echo=False, path_str=None):
    """数据库引擎"""
    if path_str:
        path = path_str
    else:
        path = db_path(db_dir_name)
    engine = create_engine('sqlite:///' + path, echo=echo)
    return engine


def get_session(db_dir_name, echo=False, path_str=None):
    """数据库会话对象"""
    engine = get_engine(db_dir_name, echo=echo, path_str=path_str)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


@contextmanager
def session_scope(db_dir_name, echo=False, path_str=None):
    """提供一系列操作事务范围"""
    session = get_session(db_dir_name, echo, path_str)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
