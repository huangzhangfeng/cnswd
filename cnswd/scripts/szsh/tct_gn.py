"""
腾讯概念股票列表(覆盖式更新)
"""
import logbook
from cnswd.sql.base import get_engine, get_session
from cnswd.websource.tencent import fetch_concept_stocks
from cnswd.sql.szsh import TCTGN


logger = logbook.Logger('腾讯概念')
db_dir_name = 'szsh'


def delete_all():
    """删除腾讯股票概念列表数据"""
    session = get_session(db_dir_name)
    num = session.query(TCTGN).delete(False)
    logger.notice(f"删除 表:{TCTGN.__tablename__} {num}行")
    session.commit()
    session.close()


def refresh():
    """采用覆盖式更新腾讯股票概念列表"""
    delete_all()
    engine = get_engine(db_dir_name)
    df = fetch_concept_stocks()
    tab = TCTGN.__tablename__
    df.rename(columns={'item_id':'概念id','item_name':'概念简称','code':'股票代码'}, inplace=True)
    df.to_sql(tab, engine, if_exists='append', index=False)
    logger.notice(f"表{tab} 更新{len(df)}行")
