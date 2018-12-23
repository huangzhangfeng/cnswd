"""

刷新暂停上市、终止上市清单

"""
import logbook

from cnswd.sql.base import get_engine, get_session
from cnswd.sql.szx import Delisting, Suspend
from cnswd.websource.szx.thematic_statistics import ThematicStatistics

db_dir_name = 'szx'
logger = logbook.Logger('深证信')


def delete_data():
    """删除表数据"""
    session = get_session(db_dir_name)
    num = session.query(Suspend).delete(False)
    logger.notice(f"删除 表:{Suspend.__tablename__} {num}行")
    num = session.query(Delisting).delete(False)
    logger.notice(f"删除 表:{Delisting.__tablename__} {num}行")
    session.commit()
    session.close()


def insert_data():
    engine = get_engine(db_dir_name)
    with ThematicStatistics(True) as api:
        s = api.query('12.2')
        s.sort_values(['上市代码', '暂停上市日期'], inplace=True)
        s.drop_duplicates(['上市代码'], 'last', inplace=True)
        s['上市代码'] = s['上市代码'].map(lambda x: str(x).zfill(6))
        s.to_sql(Suspend.__tablename__, engine, if_exists='append', index=False)
        api.logger.notice(f"添加{Suspend.__tablename__} {len(s)}行")
        d = api.query('12.3')
        d.sort_values(['上市代码', '终止上市日期'], inplace=True)
        d.drop_duplicates(['上市代码'], 'last', inplace=True)
        d['上市代码'] = d['上市代码'].map(lambda x: str(x).zfill(6))
        d.to_sql(Delisting.__tablename__, engine, if_exists='append', index=False)
        api.logger.notice(f"添加{Delisting.__tablename__} {len(d)}行")


def refresh():
    """刷新暂停上市、终止上市清单"""
    delete_data()
    insert_data()
