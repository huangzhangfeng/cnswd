# from __future__ import absolute_import
# from __future__ import division
# from __future__ import print_function

# import time

# import logbook
# import pandas as pd
# from logbook.more import ColorizedStderrHandler
# from sqlalchemy import func
# from sqlalchemy.orm import sessionmaker
# from cnswd.websource.juchao import fetch_delisting_stocks
# from cnswd.sql.base import db_path, get_engine, get_session
# from cnswd.sql.szx import IPO, Classification, Quote, StockInfo
# from cnswd.utils import ensure_list, loop_codes, loop_period_by
# from cnswd.websource import helper
# from cnswd.websource.cninfo import CodeApi, WebApi
# from cnswd.data_proxy import DataProxy
# from cnswd.scripts.szx.base import (
#     get_bank_stock, 
#     get_IPO_date, 
#     has_any_row_data, 
#     has_data,
#     last_date
# )
# from cnswd.scripts.szx.sql import _to_sql_table, write_to_sql

# EXCHANGES = ('深圳证券交易所', '上海证券交易所')
# # 以下代码无效
# INVALID_CODES = ('002257', '002525', '002710', '002720',)
# EXCLUDES = ('1.1', '7.5')               # 排除静态数据
# B_LEVELS = ('8.3.4', '8.3.5', '8.3.6')  # 金融2007版本
# FIRST_DAY = pd.Timestamp(pd.Timestamp('today').year, 1, 1)
# START_CHECK = (pd.Timestamp('today') - pd.Timedelta(days=30)).date()

# log = logbook.Logger('深证信')


# def get_request_levels(levels):
#     """请求项目层级列表"""
#     to_append = set(helper.SZX_DATA_ITEMS.keys()).difference(EXCLUDES)
#     if levels is None:
#         return to_append
#     else:
#         return set(ensure_list(levels)).intersection(to_append)


# def update_stockinfo(web_df, code, session):
#     """在数据库更新`stockinfo`对象"""
#     # 一段时期后，`stockinfo`属性可能发生变化，鉴于其为根元素，不可删除
#     # 采用覆盖式更新
#     pass


# def _update_classifications_for_level(api, sess, engine, level):
#     """写入层级分类数据，返回完成状态"""
#     q = sess.query(Classification).filter(Classification.分类层级 == level)
#     if sess.query(q.exists()).scalar():
#         log.info('{} 已经存在'.format(level))
#         return True
#     try:
#         df = helper.get_classify_table(api.wait, api.browser, level)
#         if len(df):
#             df['分类层级'] = level
#             _to_sql_table(df, 'classifications', engine,
#                         index_label='股票代码', if_exists='append')
#         return True
#     except:
#         return False
    

# def update_classifications(rewrite=False):
#     """更新所有分类信息"""
#     sess = get_session('szx')
#     engine = get_engine('szx')
#     api = WebApi()
#     if rewrite:
#         rows = sess.query(Classification).delete()
#         sess.commit()
#         log.info('删除{}行'.format(rows))
#         # for df in api.yield_total_classify_table():
#         #     if len(df):
#         #         _to_sql_table(df, 'classifications', engine,
#         #                       index_label='股票代码', if_exists='append')

#     api._load_page()
#     levels = []
#     for nth in (1, 2, 3, 4, 5, 6):
#         levels.extend(api._get_levels(nth))
#     failed = []
#     for level in levels:
#         if not _update_classifications_for_level(api, sess, engine, level):
#             for _ in range(3):
#                 log.info('分类层级{}失败，第{}次再尝试'.format(level, _ + 1))
#                 res = _update_classifications_for_level(api, sess, engine, level)
#                 if res:
#                     break
#             failed.append(level)
#     log.info('完成。失败分类层级：{}'.format(failed))
#     sess.close()
#     api.browser.quit()


# def append_quote_data(engine, api, code, start, end, dates):
#     """追加期间行情数据"""
#     level = '3.1'
#     ipo = dates.at[code, '上市日期']
#     ps = loop_period_by(start, end, freq='Q')
#     # 季度循环
#     for s, e in ps:
#         # 如在结束日期后才上市，无数据可导入
#         if ipo > e:
#             continue
#         df = api.get_quote(code, s, e, loop_ps=False)
#         write_to_sql(engine, df, level)


# def append_other_data(engine, api, level, code, start, end, dates):
#     """其他数据"""
#     s = dates.at[code, '上市日期']
#     if s > end:
#         return
#     df = api.get_data(level, code, start, end)
#     write_to_sql(engine, df, level)


# def append_bank_data(engine, api, level, code, start, end, dates):
#     """追加期间金融行业2007版本数据"""
#     s = dates.at[code, '上市日期']
#     if s > end:
#         return
#     df = api.get_data(level, code, start, end)
#     write_to_sql(engine, df, level)


# def _basic(session, level, codes):
#     failed = []
#     with WebApi(True) as api:
#         for code in codes:
#             if not has_data(session, code, level):
#                 try:
#                     df = api.get_data(level, code)
#                     write_to_sql(session.bind.engine, df, level)
#                 except Exception:
#                     failed.append(code)
#     return failed


# def _delist_codes(delisted, session):
#     """预先处理退市代码"""
#     for code, row in delisted.iterrows():
#         try:
#             si = StockInfo()
#             si.股票代码 = code
#             si.股票简称 = row['股票简称']
#             si.摘牌日期 = pd.Timestamp(row['终止上市日期'])
#             si.上市状态 = '已经退市'
#             session.add(si)
#             session.commit()
#         except:
#             session.rollback()


# def basic(levels, include_classify):
#     """初始化股票基本信息"""
#     db_dir_name = 'szx'
#     delisted = fetch_delisting_stocks()
#     with CodeApi() as api:
#         data_proxy = DataProxy(api.read_data, '08:00:00')
#         df = data_proxy.read(input_exchanges=EXCHANGES)
#         web_codes = sorted([x[:6] for x in df['股票代码'].values])
#         # 排除已经退市股票代码
#         web_codes = [x for x in web_codes if x not in delisted.index]
#         # 排除无效代码
#         web_codes = [x for x in web_codes if x not in INVALID_CODES]
#     session = get_session(db_dir_name)
#     _delist_codes(delisted, session)
#     for level in levels:
#         codes = web_codes
#         for _ in range(1, 4):
#             log.info('第{}次尝试{}'.format(_, level))
#             codes = _basic(session, level, codes)
#             if len(codes) == 0:
#                 break
#             else:
#                 log.notice('失败代码共{}个,前10：{}'.format(len(codes), codes[:10]))
#             log.notice('休眠10秒')
#             time.sleep(10)
#     if include_classify:
#         # 更新分类
#         update_classifications()
#     session.close()


# def append(levels, start, end):
#     """追加历史数据"""
#     db_dir_name = 'szx'
#     engine = get_engine(db_dir_name)
#     request_levels = get_request_levels(levels)
#     ps = sorted(loop_period_by(start, end, freq='Y'), reverse=True)
#     session = get_session(db_dir_name)
#     # 使用数据库存储的代码
#     dates = get_IPO_date(session)
#     b_codes = get_bank_stock(session)
#     with WebApi(True) as api:
#         codes = dates.index
#         for level in request_levels:
#             if level in B_LEVELS:
#                 continue
#             for code in codes:
#                 for s, e in ps:
#                     # 如果股票在此期间存在数据，则跳出，防止重复
#                     if has_any_row_data(session, level, code, s, e):
#                         continue
#                     if level == '3.1':
#                         append_quote_data(engine, api, code, s, e, dates)
#                     else:
#                         append_other_data(engine, api, level, code, s, e, dates)

#         # 金融类2007版本单列
#         for level in request_levels:
#             if level in B_LEVELS:
#                 for code in b_codes:
#                     for s, e in ps:
#                         if has_any_row_data(session, level, code, s, e):
#                             continue
#                         append_bank_data(engine, api, level, code, s, e, dates)
#     session.close()


# def _delete():
#     """删除不完整的数据"""
#     db_dir_name = 'szx'
#     session = get_session(db_dir_name)
#     start = session.query(
#         func.min(Quote.交易日期)
#     ).filter(
#         Quote.股票代码 == '000001',
#     ).filter(
#         func.date(Quote.交易日期) >= START_CHECK,
#     ).filter(
#         Quote.本日融资余额.is_(None),
#     ).one_or_none()
#     # 如果存在，证明数据不完整，删除改日之后的数据
#     # 限定融资融券类的股票
#     if start[0] is not None:
#         margin_codes = session.query(
#             Quote.股票代码.distinct()
#         ).filter(
#             Quote.本日融资余额.isnot(None)
#         ).all()
#         for code in margin_codes:
#             rows = session.query(Quote).filter(func.date(Quote.交易日期) >= start[0].date(),
#                 Quote.股票代码 == code[0]).delete(False)
#             session.commit()
#             log.info('删除代码：{} 不完整数据 {} 行'.format(code[0], rows))
#     session.close()


# def stock_daily():
#     """刷新行情数据"""
#     # 因为数据不同步，导致部分行数据不完整，比如融资融券数据。在刷新之前，删除不完整的数据
#     _delete()
#     db_dir_name = 'szx'
#     level = '3.1'
#     engine = get_engine(db_dir_name)
#     session = get_session(db_dir_name)
#     # 使用数据库存储的代码
#     dates = get_IPO_date(session)
#     codes = dates.index
#     with WebApi(True) as api:
#         for code in codes:
#             start = last_date(session, level, code)
#             if start:
#                 s = start + pd.Timedelta(days=1)
#             else:
#                 s = dates.at[code, '上市日期']
#             append_quote_data(engine, api, code, s, None, dates)
#     session.close()
