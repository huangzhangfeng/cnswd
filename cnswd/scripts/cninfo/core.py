"""

深证信数据搜索刷新脚本

"""
import logbook
import numpy as np
import pandas as pd
from pandas.tseries.offsets import BDay, QuarterBegin, YearBegin

from sqlalchemy import func

from cnswd.utils import loop_period_by
from cnswd.sql.base import get_engine, get_session
from cnswd.sql.data_browse import Classification, ClassificationBom, StockInfo
from cnswd.websource.cninfo.data_browse import DataBrowser
from cnswd.websource.cninfo.constants import DB_NAME, DB_DATE_FREQ, TS_NAME, TS_DATE_FREQ
from cnswd.websource.cninfo.data_browse import DataBrowser
from cnswd.websource.cninfo.thematic_statistics import ThematicStatistics
from .base import DB_DATE_FIELD, DB_MODEL_MAPS, TS_DATE_FIELD, TS_MODEL_MAPS
from .units import fixed_data


logger = logbook.Logger('数据刷新')


def _get_model_map(db_name):
    if db_name == 'db':
        return DB_MODEL_MAPS
    elif db_name == 'ts':
        return TS_MODEL_MAPS


def _get_class(db_name, level):
    return _get_model_map(db_name)[level]


def _get_date_freq_map(db_name):
    if db_name == 'db':
        return DB_DATE_FREQ
    elif db_name == 'ts':
        return TS_DATE_FREQ


def _get_field_map(db_name):
    if db_name == 'db':
        return DB_DATE_FIELD
    elif db_name == 'ts':
        return TS_DATE_FIELD


def _get_session(db_name):
    if db_name == 'db':
        return get_session('dataBrowse')
    elif db_name == 'ts':
        return get_session('thematicStatistics')


def _get_engine(db_name):
    if db_name == 'db':
        return get_engine('dataBrowse')
    elif db_name == 'ts':
        return get_engine('thematicStatistics')


def _get_freq(level, db_name):
    d_maps = _get_date_freq_map(db_name)
    freq_str = d_maps[level][0]
    if freq_str is None:
        return None
    else:
        return freq_str[0]


def _compute_start(end_dates, level, db_name):
    """计算项目开始日期"""
    freq = _get_freq(level, db_name)
    assert freq in ('Y', 'Q', 'M', 'W', 'D', 'B')
    if freq is None:
        return None
    # B -> D
    if freq == 'B':
        freq = 'D'
    today = pd.Timestamp('today')
    if len(end_dates) == 1:
        e_1, e_2 = pd.Timestamp(end_dates[0][0]), pd.Timestamp(end_dates[0][0])
    else:
        e_1, e_2 = pd.Timestamp(end_dates[0][0]), pd.Timestamp(end_dates[1][0])
    t_delta_2 = pd.Timedelta(
        2, unit=freq) if freq != 'Q' else pd.Timedelta(2*3, unit='M')
    t_delta_1 = pd.Timedelta(
        1, unit=freq) if freq != 'Q' else pd.Timedelta(1*3, unit='M')
    # 如大于二个周期，表明提取的网络数据为历史数据，不可能再更新
    # 此时只需在最后一日后加1天，即为开始日期
    if today - e_2 > t_delta_2:
        return pd.Timestamp(e_1) + pd.Timedelta(days=1)
    else:
        # 超出一个周期，使用最后日期
        if today - e_2 > t_delta_1:
            start = e_1
        else:
            start = e_2
    return start.normalize()


def get_start(level, db_name):
    """返回项目的开始日期"""
    freq = _get_freq(level, db_name)
    if freq is None:
        return None
    session = _get_session(db_name)
    class_ = _get_class(db_name, level)
    f_maps = _get_field_map(db_name)
    date_field = f_maps[level][0]
    default_date = f_maps[level][1]
    expr = getattr(class_, date_field)
    # 降序排列
    # 指定表的最后二项日期(唯一)
    end_dates = session.query(expr).order_by(expr.desc()).distinct()[:2]
    session.close()
    # 为空返回默认值
    if not end_dates:
        start_date = pd.Timestamp(default_date).normalize()
    else:
        start_date = _compute_start(end_dates, level, db_name)
    return start_date


def _replace(level, df, db_name):
    # 如新数据为空，则跳过
    if df.empty:
        return
    class_ = _get_model_map(db_name)[level]
    # 删除旧数据
    delete_data_of(class_, _get_session(db_name))
    df = fixed_data(df, level, db_name)
    engine = _get_engine(db_name)
    if_exists = 'replace'
    table_name = class_.__tablename__
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
    logger.notice(f"更新 数据库 {db_name} 表 {table_name}, 共 {len(df)} 条记录")


def _save_to_sql(level, df, db_name):
    class_ = _get_class(db_name, level)
    table_name = class_.__tablename__
    engine = _get_engine(db_name)
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    logger.notice(f"{db_name} {table_name}, 添加 {len(df)} 条记录")


def _add(level, df, db_name):
    df = fixed_data(df, level, db_name)
    _save_to_sql(level, df, db_name)


def _add_or_replace(level, df, db_name):
    freq = _get_freq(level, db_name)
    if freq is None:
        _replace(level, df, db_name)
    else:
        _add(level, df, db_name)


def _delete_recent_data(level, start, db_name):
    """删除指定项目的近期数据"""
    # 删除最近的日线数据
    # 融资融券数据导致不一致，需要清理旧数据
    class_ = _get_class(db_name, level)
    f_maps = _get_field_map(db_name)
    expr = getattr(class_, f_maps[level][0])
    table_name = class_.__tablename__
    session = _get_session(db_name)
    num = session.query(class_).filter(
        expr >= start).delete(False)
    st = start.strftime(r'%Y-%m-%d')
    msg = f"删除数据库 {db_name} 表:{table_name} 自{st}开始 {num}行"
    logger.notice(msg)
    session.commit()
    session.close()


def _refresh_data(level, db_name, start=None, end=None):
    """刷新层级项目数据"""
    # 在开始之前，删除可能重复的数据
    if start is not None:
        _delete_recent_data(level, start, db_name)
    if db_name == 'db':
        api_class_ = DataBrowser
    elif db_name == 'ts':
        api_class_ = ThematicStatistics
    with api_class_(True) as api:
        df = api.get_data(level, start, end)
        if not df.empty:
            _add_or_replace(level, df, db_name)


def refresh_data(level, db_name, end=None):
    """初始化数据搜索、专题统计数据"""
    if end is None:
        end = pd.Timestamp('now').normalize()
    freq = _get_freq(level, db_name)
    f_maps = _get_field_map(db_name)
    default_date = f_maps[level][1]
    if freq is None:
        # 一次执行，无需循环
        _refresh_data(level, db_name)
    else:
        start = get_start(level, db_name)
        # 按年循环
        default_date = pd.Timestamp(default_date)
        start_date = start if start > default_date else default_date
        ps = loop_period_by(start_date, pd.Timestamp(end), 'Y')
        for s, e in ps:
            _refresh_data(level, db_name, s, e)


def update_classify_bom():
    """更新分类BOM表"""
    table = ClassificationBom.__tablename__
    engine = get_engine('dataBrowse')
    with DataBrowser(True) as api:
        bom = api.classify_bom
        bom.to_sql(table, engine, if_exists='replace')
        api.logger.info(f'表：{table} 更新 {len(bom):>4}行')


def update_stock_classify(n):
    """更新第n组股票分类树信息"""
    table = Classification.__tablename__
    with DataBrowser(True) as api:
        df = api.get_classify_tree(n)
        if not df.empty:
            df.to_sql(table, get_engine('dataBrowse'),
                      if_exists='append', index=False)
            api.logger.info(f'表：{table} 添加 第{n}层分类 {len(df):>4}行')


def delete_data_of(class_, session, code=None):
    """删除表数据"""
    if code is None:
        num = session.query(class_).delete(False)
    else:
        num = session.query(class_).filter(class_.证券代码 == code).delete(False)
    logger.notice(f"删除数据 表:{class_.__tablename__} {num}行")
    session.commit()
    session.close()


def before_update_stock_classify():
    """更新股票分类前，删除已经存储的本地数据"""
    session = get_session('dataBrowse')
    delete_data_of(Classification, session)
