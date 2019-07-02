"""

列名称、数据类型、单位辅助转换工具

列名称规则：
    1. 去除前导大写字符； 如"一、营业总收入" -> "营业总收入"
                          "（一）基本每股收益" -> "基本每股收益"
    2. 列名称 (上限) -> 列名称_上限_ -> 列名称_上限
    3. 去除名称中的单位；如"分配股本基数（董 ）万股" -> "分配股本基数_董"
    4. 名称中含"："，转换为"_"；如"其中：营业收入" -> "其中_营业收入"

"""
import re
import pandas as pd
from .base import DB_DATE_FIELD, TS_DATE_FIELD

DATE_COL_PAT = re.compile('时间$|日$|日A$|日B$|日期$|年度$|报告期$')
UNIT_PAT = re.compile(r'[）(（]?(单位：)?(\w*[币股元%‰])[）)]?$')
CODE_PAT = re.compile(r'([.-]\w{1,3}$)')
# 去除前导数字
PREFIX_PAT = re.compile(r"^\d、|^[（]?[一二三四五六七八九].*?[、）]|^[(]\d[)]")
MID_PAT = re.compile(r"([)）]$|\b[（）()、：:-])")
# 尾部单位
SUFFIX_PAT = re.compile(r'[）(（]?(单位：)?(\w*[^美][股元%‰])[）)]?$')
FIN_PAT = re.compile(r"(_{1,})$")

UNIT_MAPS = {
    '%': 0.01,
    '‰': 0.001,
    '元': 1.0,
    '人民币': 1.0,
    '港币': 1.0,
    '美元': 1.0,
    '股': 1,
    '万元': 10000.0,
    '万股': 10000,
    '亿股': 100000000,
    '亿元': 100000000.0,
}


def get_unit_dict(df):
    units = {}
    for c in df.columns:
        f = UNIT_PAT.findall(c)
        if len(f) == 1:
            units[c] = UNIT_MAPS[f[0][1]]
        elif len(f) > 1:
            raise ValueError(f"找到结果{f}")
    return units


def _fix_code(df):
    """修复代码"""
    cols = ['证券代码', '股票代码', '上市代码', '转板代码', '基金代码']
    # 股票行数数据 代码 000001-SZE

    def f(x):
        if isinstance(x, str):
            return CODE_PAT.sub('', x)
        try:
            return str(int(x)).zfill(6)
        except Exception:
            return x
    for c in cols:
        if c in df.columns:
            df[c] = df[c].map(f)
    return df


def _fix_date(df):
    """修复日期"""
    for col in df.columns:
        if re.search(DATE_COL_PAT, col):
            df[col] = pd.to_datetime(
                df[col], infer_datetime_format=True, errors='coerce')
    return df


def db_3_1(df):
    df.rename(columns={"5日平均（MA5）": "MA5",
                       "10日平均（MA10）": "MA10",
                       "30日平均（MA30）": "MA30",
                       "120日均价": "MA120",
                       "EV/EBITDA": "EV_EBITDA",
                       #    "股票代码": "证券代码",
                       #    "股票简称": "证券简称",
                       "52周均价（360日）均价": "MA360"},
              inplace=True)
    return df


def db_6_1(df):
    df.rename(columns={"分配股本基数（股 ）万股": "分配股本基数(万股)"},
              inplace=True)
    # 派息日(A) -> 派息日_A
    df['派息日(A)'] = pd.to_datetime(
        df['派息日(A)'], infer_datetime_format=True, errors='coerce')
    df['派息日(B)'] = pd.to_datetime(
        df['派息日(B)'], infer_datetime_format=True, errors='coerce')
    return df


def db_8_2_3(df):
    df.rename(columns={"基本获利能力(EBIT)": "基本获利能力_EBIT"},
              inplace=True)
    df = _fix_col_name(df)
    return df


def db_8_3_1(df):
    dorp_cols = [
        '以公允价值计量且其变动计入当期损益的金融资产(20190322弃用)',
        '以公允价值计量且其变动计入当期损益的金融负债（20190322弃用）'
    ]
    # 如按解析规则 "所有者权益（或股东权益）合计" -> 所有者权益_股东权益_合计
    for c in dorp_cols:
        if c in df.columns:
            df.drop(columns=c, inplace=True)
    df.rename(columns={"所有者权益（或股东权益）合计": "所有者权益或股东权益合计",
                       "实收资本（或股本）": "实收资本或股本",
                       "负债和所有者（或股东权益）合计": "负债和所有者或股东权益合计"},
              inplace=True)
    return df


def db_8_3_2(df):
    if "基它收入" in df.columns:
        df.rename(columns={"基它收入": "其他收入"},
                  inplace=True)
    if "其它收入" in df.columns:
        df.rename(columns={"其它收入": "其他收入"},
                  inplace=True)
    return df


def db_8_3_4(df):
    df.rename(columns={"实收资本（或股本）": "实收资本或股本",
                       "所有者权益（或股东权益）合计": "所有者权益或股东权益合计",
                       "股票代码": "证券代码",
                       "股票简称": "证券简称",
                       "负债和所有者权益（或股东权益）总计": "负债和所有者权益或股东权益总计"},
              inplace=True)
    return df


def db_8_4_1(df):
    drops = ['扣除非经常性损益后的净利润(2007版)', '非经常性损益合计(2007版)']
    for c in drops:
        if c in df.columns:
            df.drop(columns=c, inplace=True)
    return df


def db_8_4_2(df):
    pre = ''
    for col in df.columns:
        if col[:4] in ('行业均值', '行业排名'):
            df.rename(columns={col: "{}_{}".format(
                pre, col[:4])}, inplace=True)
        else:
            pre = col
    return df


# 以下部分处理 -----专题统计-----


def ts_10_3(df):
    df.rename(columns={"序号": "议案序号"},
              inplace=True)
    return df


def _factory(level, db_name):
    assert db_name in ('db', 'ts'), '只支持数据查询与专题统计'
    if db_name == 'db':
        assert level in DB_DATE_FIELD.keys(
        ), f'输入层级错误，有效层级为{DB_DATE_FIELD.keys()}'
        if level == '3.1':
            return db_3_1
        elif level == '6.1':
            return db_6_1
        elif level == '8.2.3':
            return db_8_2_3
        elif level == '8.3.1':
            return db_8_3_1
        elif level == '8.3.2':
            return db_8_3_2
        elif level == '8.3.4':
            return db_8_3_4
        elif level == '8.4.1':
            return db_8_4_1
        elif level == '8.4.2':
            return db_8_4_2
    elif db_name == 'ts':
        assert level in TS_DATE_FIELD.keys(
        ), f'输入层级错误，有效层级为{TS_DATE_FIELD.keys()}'
        if level == '10.3':
            return ts_10_3
    return lambda x: x


def _special_fix(df, level, db_name):
    """针对特定项目的特殊处理"""
    func = _factory(level, db_name)
    df = func(df)
    return df


def _fix_num_unit(df):
    """修复列数量单位"""
    units = get_unit_dict(df)
    for col, unit in units.items():
        if not pd.api.types.is_numeric_dtype(df[col]):
            raise TypeError(f'应为数字类型。列"{col}"实际为"{df[col].dtype}"')
        df[col] = df[col] * unit
    return df


def _remove_prefix_num(x):
    """去除列名称中的前导数字部分"""
    return PREFIX_PAT.sub('', x)


def _remove_suffix_unit(x):
    """去除列名称中的尾部单位部分"""
    return SUFFIX_PAT.sub('', x)


def _fix_col_name(df):
    """修复列名称"""
    # 更名
    if ("股票代码" in df.columns) and ("股票简称" in df.columns):
        df.rename(columns={"股票代码": "证券代码",
                           "股票简称": "证券简称"},
                  inplace=True)
    origin = df.columns

    def f(x):
        x = _remove_prefix_num(x)
        x = _remove_suffix_unit(x)
        x = MID_PAT.sub('_', x)
        x = x.replace('Ａ', 'A')
        x = x.replace('Ｂ', 'B')
        x = x.replace('Ｈ', 'H')
        x = FIN_PAT.sub('', x.strip())
        return x
    df.columns = map(f, origin)
    return df


def fixed_data(input_df, level, db_name):
    """修复日期、股票代码、数量单位及规范列名称"""
    # 避免原地修改
    df = input_df.copy()
    df = _special_fix(df, level, db_name)
    df = _fix_code(df)
    df = _fix_date(df)
    df = _fix_num_unit(df)
    df = _fix_col_name(df)
    return df
