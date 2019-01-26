"""只应用到深证信
"""


import pandas as pd
from sqlalchemy import func, text
from sqlalchemy.sql import exists

from cnswd.data_proxy import next_update_time
from cnswd.sql.base import db_path, session_scope, get_session
from cnswd.sql.szx import (ActualController, AdditionalStockImplementation,
                           AdditionalStockPlan, CompanyShareChange, Dividend,
                           ExecutivesShareChange, FinancialIndicatorRanking,
                           InvestmentRating, PerformanceForecaste,
                           PeriodlyBalanceSheet, PeriodlyBalanceSheet2007,
                           PeriodlyCashFlowStatement,
                           PeriodlyCashFlowStatement2007,
                           PeriodlyFinancialIndicator, PeriodlyIncomeStatement,
                           PeriodlyIncomeStatement2007,
                           QuarterlyCashFlowStatement,
                           QuarterlyFinancialIndicator,
                           QuarterlyIncomeStatement, Quote,
                           ShareholderShareChange, ShareholdingConcentration,
                           SharePlacementImplementation, SharePlacementPlan,
                           Classification,
                           StockInfo, IPO, TtmCashFlowStatement, TtmIncomeStatement)
from cnswd.utils import sanitize_dates, MARKET_START
from cnswd.websource.szx.data_browse import LEVEL_MAPS
from cnswd.websource.szx.ops import convert_to_item, item_to_level

DT_FMT = r'%Y-%m-%d %H:%M:%S.%f'

MODEL_MAPS = {
    '1.1':   StockInfo,
    '2.1':   ActualController,
    '2.2':   CompanyShareChange,
    '2.3':   ExecutivesShareChange,
    '2.4':   ShareholderShareChange,
    '2.5':   ShareholdingConcentration,
    '3.1':   Quote,
    '4.1':   InvestmentRating,
    '5.1':   PerformanceForecaste,
    '6.1':   Dividend,
    '7.1':   AdditionalStockPlan,
    '7.2':   AdditionalStockImplementation,
    '7.3':   SharePlacementPlan,
    '7.4':   SharePlacementImplementation,
    '7.5':   IPO,
    '8.1.1': TtmIncomeStatement,
    '8.1.2': TtmCashFlowStatement,
    '8.2.1': QuarterlyIncomeStatement,
    '8.2.2': QuarterlyCashFlowStatement,
    '8.2.3': QuarterlyFinancialIndicator,
    '8.3.1': PeriodlyBalanceSheet,
    '8.3.2': PeriodlyIncomeStatement,
    '8.3.3': PeriodlyCashFlowStatement,
    '8.3.4': PeriodlyBalanceSheet2007,
    '8.3.5': PeriodlyIncomeStatement2007,
    '8.3.6': PeriodlyCashFlowStatement2007,
    '8.4.1': PeriodlyFinancialIndicator,
    '8.4.2': FinancialIndicatorRanking,
}

# 第一项为数据库字段名称，第二项为更新频率，第三项为原始列名称
DATE_MAPS = {
    # '1.1':   StockInfo,
    '2.1':   ('变动日期', 'D'),
    '2.2':   ('变动日期', 'D'),
    '2.3':   ('截止日期', 'D'),
    '2.4':   ('增减持截止日', 'D', '增（减）持截止日'),
    '2.5':   ('截止日期', 'Q'),
    '3.1':   ('交易日期', 'D'),
    '4.1':   ('发布日期', 'D'),
    '5.1':   ('报告年度', 'Q'),
    '6.1':   ('分红年度', 'Q'),
    '7.1':   ('公告日期', 'D'),
    '7.2':   ('公告日期', 'D'),
    '7.3':   ('公告日期', 'D'),
    '7.4':   ('公告日期', 'D'),
    # '7.5':   IPO,
    '8.1.1': ('报告年度', 'Q'),
    '8.1.2': ('报告年度', 'Q'),
    '8.2.1': ('报告年度', 'Q'),
    '8.2.2': ('报告年度', 'Q'),
    '8.2.3': ('报告年度', 'Q'),
    '8.3.1': ('报告年度', 'Q'),
    '8.3.2': ('报告年度', 'Q'),
    '8.3.3': ('报告年度', 'Q'),
    '8.3.4': ('报告年度', 'Q'),
    '8.3.5': ('报告年度', 'Q'),
    '8.3.6': ('报告年度', 'Q'),
    '8.4.1': ('报告年度', 'Q'),
    '8.4.2': ('报告期', 'Q'),
}


def get_all_stock(session=None):
    """所有股票代码"""
    if session is None:
        session = get_session('szx')
    codes = session.query(
        StockInfo.股票代码
    ).all()
    res = [x[0] for x in codes]
    session.close()
    return res

def get_bank_stock(session):
    """金融行业股票代码列表"""
    codes = session.query(
        StockInfo.股票代码
    ).filter(
        StockInfo.证监会一级行业名称 == '金融业'
    ).all()
    return [x[0] for x in codes]


def get_ipo_date(session, code):
    """股票上市日期"""
    return session.query(
        StockInfo.上市日期
    ).filter(
        StockInfo.股票代码 == code,
        func.date(StockInfo.上市日期) >= pd.Timestamp('1900-01-01').date()
    ).scalar()


def has_any_row_data(session, item, code, start, end):
    """查询股票期间是否存在任意行数据"""
    level = item_to_level(convert_to_item(item, LEVEL_MAPS), LEVEL_MAPS)
    obj = MODEL_MAPS[level]
    date_col_name = DATE_MAPS[level][0]
    q = session.query(obj).filter(
        obj.股票代码 == code
    ).filter(
        text(
            "{0}>=:start and {0}<=:end".format(date_col_name)
        ).params(start=start.strftime(DT_FMT), end=end.strftime(DT_FMT))
    )
    return session.query(q.exists()).scalar()


def has_data(session, code, level):
    """查询项目中指定代码是否存在数据"""
    if level == '1.1':
        obj = StockInfo
    elif level == '7.5':
        obj = IPO
    q = session.query(obj).filter(
        obj.股票代码 == code
    )
    return session.query(q.exists()).scalar()


def get_start_date(session, item, code):
    """股票项目数据开始刷新日期"""
    level = item_to_level(convert_to_item(item, LEVEL_MAPS), LEVEL_MAPS)
    obj = MODEL_MAPS[level]
    date_col_name = DATE_MAPS[level][0]
    freq = DATE_MAPS[level][1]
    d = getattr(obj, date_col_name)
    res = session.query(func.max(d)).filter(
        obj.股票代码 == code
    ).scalar()
    if res is None:
        if level in ('2.1', '2.2', '2.3', '2.4'):
            return MARKET_START.date()
        else:
            return get_ipo_date(session, code).date()
    else:
        try:
            start, _ = sanitize_dates(res, None)
            return next_update_time(start, freq, hour=0).date()
        except ValueError:
            # 当开始日期大于当日，返回明日
            return (pd.Timestamp('today') + pd.Timedelta(days=1)).date()


def get_s_and_d():
    """获取股票暂停及终止上市日期"""
    db_dir_name = 'szx'
    with session_scope(db_dir_name) as sess:
        p1 = sess.query(
            Suspend.上市代码,
            func.date(Suspend.暂停上市日期)
        ).all()
        p2 = sess.query(
            Delisting.上市代码,
            func.date(Delisting.终止上市日期)
        ).all()
    df = pd.DataFrame.from_records(
        p1+p2, columns=['股票代码', '日期'])
    # df.sort_index(inplace=True)
    df.drop_duplicates(['股票代码', '日期'], keep='last', inplace=True)
    return df.set_index('股票代码')


def _get_quote_start_date():
    """获取股票日线开始日期
    
    Returns:
        DataFrame -- 以股票代码为Index，日期单列DataFrame
    """
    db_dir_name = 'szx'
    codes = get_all_stock()
    with session_scope(db_dir_name) as sess:
        res = sess.query(
            Quote.股票代码,
            func.max(Quote.交易日期)
        ).group_by(
            Quote.股票代码
        ).all()
        tmp = []
        for code, date in res:
            if date is None:
                start = get_ipo_date(sess, code).date()
            else:
                start = date + pd.Timedelta(days=1)
            tmp.append((code, start))
        # 已经存在日线代码
        q_codes = [cd[0] for cd in tmp]
        for code in codes:
            if code not in q_codes:
                tmp.append((code, get_ipo_date(sess, code).date()))
        return pd.DataFrame.from_records(tmp, columns=['股票代码', '开始日期']).set_index('股票代码')


def get_quote_start_date():
    """获取股票日线开始日期
    
    Returns:
        DataFrame -- 以股票代码为Index，日期单列DataFrame

    说明：
        如暂停上市、终止上市，则开始日期设置为次日。值为Timestamp对象。
    """
    df = _get_quote_start_date()
    sd = get_s_and_d()
    for code, d in sd.iterrows():
        try:
            dt = df.at[code, '开始日期']
            if pd.Timestamp(dt).date() >= pd.Timestamp(d['日期']).date():
                df.loc[code, '开始日期'] = (pd.Timestamp(
                    'today')+pd.Timedelta(days=1)).normalize()
        except KeyError:
            pass
    return df


def query_stock_by_level(level, exact=False):
    """按分类层级查询过代码
    
    Arguments:
        level {str} -- 分类层级
    
    Keyword Arguments:
        exact {bool} -- 是否精确匹配 (default: {False})
    
    Returns:
        list -- 分类层级股票代码列表
    """
    db_dir_name = 'szx'
    with session_scope(db_dir_name) as sess:
        if exact:
            cond = Classification.分类层级 == level
        else:
            cond = Classification.分类层级.startswith(level)
        q = sess.query(
            Classification.股票代码
        ).filter(
            cond
        ).all()
        return [x[0] for x in q]
