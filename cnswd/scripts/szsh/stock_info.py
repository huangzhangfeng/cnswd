"""
股票简要信息

覆盖式更新
"""
import pandas as pd
from cnswd.websource.szse import (fetch_companys_info,
                                  fetch_delisting_stocks,
                                  fetch_suspend_stocks)
from cnswd.websource.sse import SSEPage
from cnswd.sql.szsh import Stock, Delisting, Suspend
from cnswd.sql.base import get_engine, get_session
import time
import logbook


logger = logbook.Logger('数据库')
db_dir_name = 'szsh'


def stock_list(sse_api):
    """正常在市股票(深交所、上交所)"""
    szse = fetch_companys_info()
    a = sse_api.get_stock_list_a()
    a['公司代码'] = a['公司代码'].map(str)
    a['A股代码'] = a['A股代码'].map(str)
    b = sse_api.get_stock_list_b()
    b['公司代码'] = b['公司代码'].map(str)
    b['B股代码'] = b['B股代码'].map(str)
    b.drop('公司简称', axis=1, inplace=True)
    sse = a.set_index('公司代码').join(b.set_index('公司代码'), sort=True)
    sse.reset_index(inplace=True)
    df = pd.concat([szse[sse.columns], sse])
    df['A股上市日期'] = pd.to_datetime(df['A股上市日期'], errors='coerce')
    df['B股上市日期'] = pd.to_datetime(df['B股上市日期'], errors='coerce')
    return df.drop_duplicates('公司代码')


def delisting(sse_api):
    """合并深交所、上交所退市股票"""
    szse = fetch_delisting_stocks()
    sse = sse_api.get_delisting()
    sse.rename(columns={'原公司代码': '证券代码', '原公司简称': '证券简称'}, inplace=True)
    df = pd.concat([szse, sse[szse.columns]])
    df['上市日期'] = pd.to_datetime(df['上市日期'], errors='coerce')
    df['终止上市日期'] = pd.to_datetime(df['终止上市日期'], errors='coerce')
    return df


def suspend(sse_api):
    """合并深交所、上交所暂停上市股票"""
    szse = fetch_suspend_stocks()
    sse = sse_api.get_suspend()
    sse.rename(columns={'公司代码': '证券代码', '公司简称': '证券简称'}, inplace=True)
    df = pd.concat([szse, sse[szse.columns]])
    df['上市日期'] = pd.to_datetime(df['上市日期'], errors='coerce')
    df['暂停上市日期'] = pd.to_datetime(df['暂停上市日期'], errors='coerce')
    return df


def delete_all():
    """删除已有股票基本信息数据"""
    session = get_session(db_dir_name)
    num = session.query(Stock).delete(False)
    logger.notice(f"删除 表:{Stock.__tablename__} {num}行")
    num = session.query(Delisting).delete(False)
    logger.notice(f"删除 表:{Delisting.__tablename__} {num}行")
    num = session.query(Suspend).delete(False)
    logger.notice(f"删除 表:{Suspend.__tablename__} {num}行")
    session.commit()
    session.close()


def refresh():
    """采用覆盖式更新"""
    # 在写入前删除
    delete_all()
    engine = get_engine(db_dir_name)
    with SSEPage() as sse_api:
        for get_data, tab in zip((stock_list, delisting, suspend), ('stocks', 'delistings', 'suspends')):
            try:
                df = get_data(sse_api)
                df.to_sql(tab, engine, if_exists='append', index=False)
                logger.notice(f"表{tab} 更新{len(df)}行")
            except Exception as e:
                logger.error(f'{tab}数据刷新失败 {e!r}')
            time.sleep(0.5)