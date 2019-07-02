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


def _get_date_start(level, db_name):
    """日数据类型的开始时间"""
    session = _get_session(db_name)
    class_ = _get_class(db_name, level)
    f_maps = _get_field_map(db_name)
    date_field = f_maps[level][0]
    default_date = f_maps[level][1]
    expr = getattr(class_, date_field)
    end_date = session.query(func.max(expr)).scalar()
    session.close()
    if end_date is None:
        start_date = pd.Timestamp(default_date)
    else:
        start_date = pd.Timestamp(end_date)
    return start_date.normalize()


def get_start(level, db_name):
    """
    获取项目开始刷新数据的日期

    逻辑
    ----
    1. 查询层级本地数据库最后更新日期;
    2. 不存在即返回默认开始日期;
    3. 如存在
        1. 网络数据可按期间设定的项目，以本地最大日期的次日作为开始日期
        1. 网络数据按季度设定的项目，自当日回溯至上一个季度首日为开始日期
        2. 网络数据按年度设定的项目，自当日回溯至上一个年度首日为开始日期

    说明：
    ----
    1. 静态数据返回`None`，如股票信息、IPO等项目
    """
    freq = _get_freq(level, db_name)
    today = pd.Timestamp('today')
    if freq is None:
        return None
    end_date = _get_date_start(level, db_name)
    if freq == 'B':
        # 股票日线数据中融资融券数据时间有延迟，后溯1天
        start_date = end_date - pd.Timedelta(1, unit='D')
    elif freq == 'W':
        start_date = end_date - pd.Timedelta(1, unit=freq)
    elif freq == 'D':
        start_date = end_date + pd.Timedelta(1, unit=freq)
    elif freq == 'M':
        if (today - end_date) > pd.Timedelta(1, 'M'):
            start_date = end_date + pd.Timedelta(1, 'D')
        else:
            start_date = end_date - pd.Timedelta(1, unit=freq)
    elif freq == 'Q':
        # 为确保数据完整，须在本地数据库最大日期后溯2个季度
        # 自当前季度倒退2个季度，选择季初日期
        # 如当前日期 2019-09-30 则开始日期为 2019-04-01
        # 如当前日期 2019-06-30 则开始日期为 2019-01-01
        if (today - end_date) > pd.Timedelta(6, 'M'):
            start_date = end_date + pd.Timedelta(1, 'D')
        elif (today - end_date) > pd.Timedelta(3, 'M'):
            start_date = QuarterBegin(
                -1, normalize=True, startingMonth=1).apply(end_date)
        else:
            start_date = QuarterBegin(
                -2, normalize=True, startingMonth=1).apply(end_date)
    elif freq == 'Y':
        # 为确保数据完整，须在本地数据库最大日期后溯2年
        # 自当前年度倒退2年，选择年初日期
        if (today - end_date) > pd.Timedelta(2, freq):
            start_date = end_date + pd.Timedelta(1, 'D')
        if (today - end_date) > pd.Timedelta(1, freq):
            start_date = YearBegin(-1, normalize=True, month=1).apply(end_date)
        else:
            start_date = YearBegin(-2, normalize=True, month=1).apply(end_date)
    else:
        raise ValueError(f'{level} 更新频率字符值错误')
    return start_date.normalize()


def _replace(level, df, db_name):
    # 如新数据为空，则跳过
    if df.empty:
        return
    class_ = _get_model_map(db_name)[level]
    # 删除旧数据
    delete_data_of(class_, _get_session(db_name))
    df = fixed_data(df, level, db_name)
    if db_name == 'db':
        df['last_refresh_time'] = pd.Timestamp('now')
    engine = _get_engine(db_name)
    if_exists = 'replace'
    table_name = class_.__tablename__
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
    logger.notice(f"更新 数据库 {db_name} 表{table_name}, 共 {len(df)} 条记录")


def _save_to_sql(level, df, db_name):
    class_ = _get_class(db_name, level)
    table_name = class_.__tablename__
    item = DB_NAME[level] if db_name == 'db' else TS_NAME[level]
    engine = _get_engine(db_name)
    if db_name == 'db':
        df['last_refresh_time'] = pd.Timestamp('now')
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    logger.notice(f"{db_name} {item}, 添加 {len(df)} 条记录")


def _add(level, df, db_name):
    if df.empty:
        return
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


def refresh_data(level, db_name):
    """刷新层级项目数据"""
    start = get_start(level, db_name)
    # 在开始之前，删除可能重复的数据
    if start is not None:
        _delete_recent_data(level, start, db_name)
    if db_name == 'db':
        api_class_ = DataBrowser
    elif db_name == 'ts':
        api_class_ = ThematicStatistics
    with api_class_(True) as api:
        df = api.get_data(level, start)
        _add_or_replace(level, df, db_name)


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


def init_cninfo_data(level, db_name):
    """初始化数据搜索、专题统计数据"""
    if db_name == 'db':
        api_class_ = DataBrowser
    elif db_name == 'ts':
        api_class_ = ThematicStatistics
    freq = _get_freq(level, db_name)
    f_maps = _get_field_map(db_name)
    # date_field = f_maps[level][0]
    default_date = f_maps[level][1]
    with api_class_(True) as api:
        if freq is None:
            df = api.get_data(level)
            # 一次性完成
            _replace(level, df, db_name)
        else:
            start = get_start(level, db_name)
            # 如果时间跨度太大，不一定能够一次性完成导入；且容易造成内存溢出
            # 按年循环
            default_date = pd.Timestamp(default_date)
            start_date = start if start > default_date else default_date
            ps = loop_period_by(start_date, pd.Timestamp('now'), 'Y')
            for s, e in ps:
                df = api.get_data(level, s, e)
                _add(level, df, db_name)