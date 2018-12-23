"""
处理国库券数据（供算法分析使用）
"""
from datetime import timedelta
from sqlalchemy import func
import pandas as pd
import logbook

from cnswd.websource.treasuries import fetch_treasury_data_from, download_last_year
from cnswd.sql.base import get_session
from cnswd.sql.szsh import Treasury


logger = logbook.Logger('国库券')
db_dir_name = 'szsh'


def get_start(sess):
    """获取开始日期"""
    last_date = sess.query(func.max(Treasury.date)).scalar()
    if last_date is not None:
        return last_date + timedelta(days=1)
    return last_date


def insert(sess, df):
    """插入数据到数据库"""
    objs = []
    for d, row in df.iterrows():
        t = Treasury(date=d.date(),
                     m0=row['m0'],
                     m1=row['m1'],
                     m2=row['m2'],                
                     m3=row['m3'],
                     m6=row['m6'],
                     m9=row['m9'],                     
                     y1=row['y1'],
                     y3=row['y3'],
                     y5=row['y5'],
                     y7=row['y7'],                     
                     y10=row['y10'],
                     y15=row['y15'],                    
                     y20=row['y20'],
                     y30=row['y30'],
                     y40=row['y40'],
                     y50=row['y50'])
        objs.append(t)
    sess.add_all(objs)
    sess.commit()
    logger.info('新增{}行'.format(len(objs)))


def refresh_treasury():
    """刷新国库券利率数据"""
    # 首先下载最新一期的数据
    download_last_year()
    sess = get_session(db_dir_name)
    start = get_start(sess)
    if start is None:
        df = fetch_treasury_data_from()
    elif start > pd.Timestamp('today').date():
        return
    else:
        # 读取自开始日期的数据
        df = fetch_treasury_data_from(start)
    insert(sess, df)
    sess.close()
