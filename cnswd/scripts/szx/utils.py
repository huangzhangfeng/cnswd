# """

# 列名称、数据类型辅助转换工具

# 规则：
#     1. 列名称 (上限) -> _上限_ -> _上限
# """
# import re
# import pandas as pd

# DATE_COL_PAT = re.compile('日$|日期$|年度$|期$')
# UNIT_PAT = re.compile(r'\(*.?元|股|%|‰\)$')


# def _get_uit(x):
#     """列名称解析数量单位"""
#     if '%' in x:
#         return 0.01
#     elif '‰' in x:
#         return 0.001
#     elif '万元' in x:
#         return 10000
#     elif '万股' in x:
#         return 10000
#     elif '亿元' in x:
#         return 100000000
#     elif '亿股' in x:
#         return 100000000
#     elif '股' in x:
#         return 1
#     elif '元' in x:
#         return 1
#     return None


# def _zfill(df):
#     if '证券代码' in df.columns:
#         df['证券代码'] = df['证券代码'].map(lambda x: str(x).zfill(6))
#     if '基金代码' in df.columns:
#         df['基金代码'] = df['基金代码'].map(lambda x: str(x).zfill(6))
#     return df


# def _to_date(df):
#     """转换日期"""
#     for col in df.columns:
#         if re.search(DATE_COL_PAT, col):
#             df[col] = pd.to_datetime(
#                 df[col], infer_datetime_format=True, errors='coerce')
#     return df


# def _convert_to_numeric(df):
#     # 转换数据
#     for col in df.columns:
#         if '(' in col:
#             unit = _get_uit(col)
#             if unit:
#                 df[col] = pd.to_numeric(df[col], errors='coerce') * unit
#     return df


# def _normalize_col_name(col):
#     """
#     规范列名称

#     案例：
#         1. 总市值（静态）(亿元) -> 总市值_静态
#         2. 静态市盈率（加权平均） -> 静态市盈率_加权平均
#     """
#     res = ''
#     if re.search(UNIT_PAT, col):
#         res = col.split('(')[0]
#     else:
#         res = col
#     if '%' in res:
#         res = res.replace('%','')
#     res = res.replace('（', '_').replace('）', '')
#     return res



# def _fix_col_name(df):
#     """修复数据框的列名称"""
#     new_names = []
#     for col in df.columns:
#         res = _normalize_col_name(col)
#         new_names.append(res)
#     df.columns = new_names
#     return df


# def fixed_data(df):
#     df = _zfill(df)
#     df = _to_date(df)
#     df = _convert_to_numeric(df)
#     # 最后修复列名称
#     df = _fix_col_name(df)
#     return df
