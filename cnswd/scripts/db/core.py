"""

深证信数据搜索刷新脚本

"""
import time
from functools import lru_cache
import logbook
import pandas as pd
from selenium.common.exceptions import (ElementNotInteractableException,
                                        NoSuchElementException,
                                        TimeoutException)
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from cnswd.sql.base import get_engine, get_session
from cnswd.sql.szx import Classification, ClassificationBom, StockInfo
from cnswd.utils import ensure_list, loop_codes, loop_period_by
from cnswd.websource.szx.data_browse import LEVEL_MAPS, DataBrowser

from .base import DATE_MAPS, MODEL_MAPS
from .utils import fixed_data


db_dir_name = 'dataBrowse'
logger = logbook.Logger('数据搜索')
B_2007_LEVELS = ('8.3.4', '8.3.5', '8.3.6')


def default_ordered_levels():
    """默认项目排序"""
    y = ['6.1']
    s_e = ['2.1', '2.2', '2.3', '2.4', '3.1',
           '4.1'] + ['7.1', '7.2', '7.3', '7.4']
    y_q = ['2.5', '5.1'] + [x for x in MODEL_MAPS.keys() if x.startswith('8')
                            and (x not in B_2007_LEVELS)]
    return ['1.1', '7.5'] + y + s_e + y_q


def get_bank_stock():
    """金融行业股票代码列表"""
    session = get_session(db_dir_name)
    codes = session.query(
        StockInfo.证券代码
    ).filter(
        StockInfo.证监会一级行业名称 == '金融业'
    ).all()
    session.close()
    return [x[0] for x in codes]


def delete_data_of(class_, code=None):
    """删除表数据"""
    session = get_session(db_dir_name)
    if code is None:
        num = session.query(class_).delete(False)
    else:
        num = session.query(class_).filter(class_.证券代码 == code).delete(False)
    logger.notice(f"删除 表:{class_.__tablename__} {num}行")
    session.commit()
    session.close()


def _ipo_date(session, code):
    class_ = MODEL_MAPS['1.1']
    return session.query(class_.上市日期).filter(class_.证券代码 == code).one_or_none()


def _get_start_date(level, code, offset=1):
    """开始刷新时间"""
    session = get_session(db_dir_name)
    class_ = MODEL_MAPS[level]
    expr = getattr(class_, DATE_MAPS[level][0])
    cond = class_.证券代码 == code
    t_end_date = session.query(func.max(expr)).filter(cond).scalar()
    if t_end_date is None:
        ipo = _ipo_date(session, code)
        if ipo:
            res = ipo[0]
        else:
            res = pd.Timestamp(DATE_MAPS[level][2])
    else:
        # 调整天数
        res = t_end_date + pd.Timedelta(days=offset)
    session.close()
    return res


def _save_to_sql(level, df, code):
    class_ = MODEL_MAPS[level]
    table_name = class_.__tablename__
    item = LEVEL_MAPS[level][0]
    engine = get_engine(db_dir_name)
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    logger.notice(f"{item}, 股票代码 {code} 添加 {len(df)} 条记录")


def _delete_recent_quotes(start, code):
    # 删除最近的日线数据
    # 融资融券数据导致不一致，需要清理旧数据
    start = start - pd.Timedelta(days=7)
    class_ = MODEL_MAPS['3.1']
    expr = getattr(class_, DATE_MAPS['3.1'][0])
    table_name = class_.__tablename__
    session = get_session(db_dir_name)
    num = session.query(class_).filter(
        expr >= start, class_.证券代码 == code).delete(False)
    logger.notice(f"删除 表:{table_name} 股票代码：{code} {num}行")
    session.commit()
    session.close()
    return start


def _loop_by(api, level, code):
    start = _get_start_date(level, code)
    if level == '3.1':
        start = _delete_recent_quotes(start, code)
    today = pd.Timestamp('today')
    freq = DATE_MAPS[level][1]
    # 单只股票，期间 = 数据库最后一日 ~ 今日
    if freq == 'D':
        t1 = start.strftime(r'%Y-%m-%d')
        t2 = today.strftime(r'%Y-%m-%d')
        df = api.query(level, code, t1, t2)
        if df.empty:
            return
        df = fixed_data(df, level)
        _save_to_sql(level, df, code)
    else:
        if level == '3.1':
            ps = loop_period_by(start, today, freq, False)
        else:
            ps = loop_period_by(start, today, freq)
        for s, e in ps:
            if freq == 'Y':
                t1 = str(e.year)
                t2 = None
            elif freq == 'Q':
                t1 = str(e.year)
                t2 = e.quarter
            elif freq == 'M':
                t1 = s.strftime(r'%Y-%m-%d')
                t2 = e.strftime(r'%Y-%m-%d')
            df = api.query(level, code, t1, t2)
            if df.empty:
                continue
            df = fixed_data(df, level)
            _save_to_sql(level, df, code)
            del df


def _replace(api, level, code):
    # 删除旧数据
    delete_data_of(MODEL_MAPS[level], code)
    df = api.query(level, code)
    if df.empty:
        return
    df = fixed_data(df, level)
    engine = get_engine(db_dir_name)
    if_exists = 'append'
    table_name = MODEL_MAPS[level].__tablename__
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
    api.logger.notice(f"更新 表{table_name}, 股票代码 {code} 共 {len(df)} 条记录")


def _refresh(api, level, code):
    """刷新项目数据
    
    Arguments:
        api {api} -- api
        level {str} -- 项目
        codes {str} -- 股票代码列表
        b {int} -- 批号
    """
    if DATE_MAPS[level][1] is None:
        _replace(api, level, code)
    else:
        _loop_by(api, level, code)


def _valid_level(to_do):
    for l in to_do:
        if l not in MODEL_MAPS.keys():
            raise ValueError(f'不支持数据项目"{l}"')


def _ipo(api, codes):
    codes = sorted(codes)
    dfs = []
    for code in codes:
        df = api.query('7.5', code)
        if not df.empty:
            dfs.append(df)
    to_add = pd.concat(dfs)
    engine = get_engine(db_dir_name)
    table_name = MODEL_MAPS['7.5'].__tablename__
    to_add = fixed_data(to_add, '7.5')
    to_add.to_sql(table_name, con=engine, if_exists='replace', index=False)
    api.logger.notice(f"{'更新'} 表{table_name}, 共 {len(to_add)} 条记录")


@lru_cache(None)
def _existed_code(level):
    class_ = MODEL_MAPS[level]
    session = get_session(db_dir_name)
    res = session.query(class_.证券代码).all()
    session.close()
    return [x[0] for x in res]


def refresh(update=False, levles=None, retry=3):
    """专题统计项目数据刷新"""
    if levles is None or len(levles) == 0:
        to_do = default_ordered_levels()
    else:
        to_do = ensure_list(levles)
        _valid_level(to_do)
    done = {}
    api = DataBrowser(True)
    codes = api.get_all_codes()
    b_codes = get_bank_stock()
    # 测试代码
    # codes = ['000001', '000002', '000007', '300467', '300483', '603633', '603636',
    #          '000333', '600017', '600645', '603999']
    for code in codes:
        for i in range(retry):
            for level in to_do:
                key = ','.join([level, code])
                # 本地数据库已经存在的股票代码，其基本信息、IPO表如不需要更新，则跳过
                if not update and level in ('1.1', '7.5') and code in _existed_code(level):
                    done[key] = True
                if level in B_2007_LEVELS and code not in b_codes:
                    done[key] = True
                if done.get(key):
                    # api.logger.info(f'跳过：{LEVEL_MAPS[level][0]} {code}')
                    continue
                api.logger.notice(f'第{i+1}次尝试：{LEVEL_MAPS[level][0]}')
                try:
                    _refresh(api, level, code)
                    done[key] = True
                except IntegrityError as e:
                    if level.startswith('8.') or level == '6.1':
                        done[key] = True
                    else:
                        done[key] = False
                        api.logger.notice(f'{LEVEL_MAPS[level][0]} {code} \n {e!r}')
                        api.driver.quit()
                        time.sleep(5)
                        api = DataBrowser(True)                        
                except (ElementNotInteractableException, NoSuchElementException, TimeoutException, ValueError, ConnectionError) as e:
                    api.logger.notice(
                        f'{LEVEL_MAPS[level][0]} {code} \n {e!r}')
                    done[key] = False
                    api.driver.quit()
                    time.sleep(5)
                    api = DataBrowser(True)
    api.driver.quit()


def _update_classify_bom(api):
    table = ClassificationBom.__tablename__
    engine = get_engine(db_dir_name)
    bom = api.classify_bom
    bom.to_sql(table, engine, if_exists='replace')
    api.logger.info(f'表：{table} 更新 {len(bom):>4}行')


def _update_stock_classify(api):
    """更新股票分类信息"""
    table = Classification.__tablename__
    levels = []
    for nth in (1, 2, 3, 4, 5, 6):
        levels.extend(api.get_levels_for(nth))
    # CDR当前不可用
    levels = [x for x in levels if x not in (
        '6.6', '6.9') and api._is_end_level(x)]
    done = []
    dfs = []
    for i in range(10):
        to_do = sorted(list(set(levels) - set(done)))
        if len(to_do) == 0:
            break
        api.logger.notice(f'第{i+1}次尝试，剩余分类层级数量:{len(to_do)}')
        for level in sorted(to_do):
            try:
                df = api.get_classify_stock(level)
                dfs.append(df)
                done.append(level)
            except Exception as e:
                api.logger.notice(f"{e!r}")
        time.sleep(0.5)

    # 完整下载后，才删除旧数据
    delete_data_of(Classification)

    for df in dfs:
        if not df.empty:
            df.to_sql(table, get_engine(db_dir_name),
                      if_exists='append', index=False)
            api.logger.info(
                f'表：{table} 添加{len(df):>4}行')


def update_stock_classify():
    """刷新股票分类信息"""
    with DataBrowser(True) as api:
        _update_classify_bom(api)
        _update_stock_classify(api)
