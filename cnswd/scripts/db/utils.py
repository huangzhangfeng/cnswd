"""

列名称、数据类型辅助转换工具

规则：
    1. 列名称 (上限) -> 列名称_上限_ -> 列名称_上限
    2. 分配股本基数（董 ）万股 -> 分配股本基数_董
"""
import re
import pandas as pd


DATE_COL_PAT = re.compile('时间$|日$|日A$|日B$|日期$|年度$|报告期$')
UNIT_PAT = re.compile(r'\(*.?元|股|%|‰\)$')


def _get_uit(x):
    """列名称解析数量单位"""
    if '%' in x:
        return 0.01
    elif '‰' in x:
        return 0.001
    elif '万元' in x:
        return 10000
    elif '万股' in x:
        return 10000
    elif '亿元' in x:
        return 100000000
    elif '亿股' in x:
        return 100000000
    elif '股' in x:
        return 1
    elif '元' in x:
        return 1
    return None


def _zfill(df):
    if '证券代码' in df.columns:
        df['证券代码'] = df['证券代码'].map(lambda x: str(x).zfill(6))
    if '股票代码' in df.columns:
        df['股票代码'] = df['股票代码'].map(lambda x: str(x)[:6].zfill(6))
    return df


def _to_date(df):
    """转换日期"""
    for col in df.columns:
        if re.search(DATE_COL_PAT, col):
            df[col] = pd.to_datetime(
                df[col], infer_datetime_format=True, errors='coerce')
    return df


def _fix_unit(df, unit, exclude=()):
    """修复数据框列单位
    
    Arguments:
        df {数据框} -- 要修复的数据框
    
    Keyword Arguments:
        unit {dict或float或None} -- 指定单位 (default: {{}})
        exclude {tuple} -- 排除列名称 (default: {()})
    """
    for col in df.columns:
        if unit is None:
            u = _get_uit(col)
            if u:
                df[col] = df[col] * u
        elif pd.api.types.is_numeric_dtype(df[col]) and col not in exclude:
            if isinstance(unit, dict):
                u = unit.get(col, 1)
            elif isinstance(unit, (float, int)):
                u = unit
            else:
                u = 1
            df[col] = df[col] * u
    return df


def _normalize_col_name(col):
    """
    规范列名称

    案例：
        1. 总市值（静态）(亿元) -> 总市值_静态
        2. 静态市盈率（加权平均） -> 静态市盈率_加权平均
    """
    res = ''
    if re.search(UNIT_PAT, col):
        res = col.split('(')[0]
    else:
        res = col
    if '%' in res:
        res = res.replace('%', '')
    if '‰' in res:
        res = res.replace('‰', '')
    if '万股' in res:
        res = res.replace('万股', '')
    res = res.replace('（', '_').replace('）', '')
    res = res.replace('(', '_').replace(')', '')
    res = res.replace('：', '_')
    res = res.replace(':', '_')
    res = res.replace('/', '_')
    res = res.replace('Ａ', 'A')
    res = res.replace('Ｂ', 'B')
    res = res.replace('Ｈ', 'H')
    res = res.replace(' ', '')
    res = res.replace('，', '_')
    res = res.replace('、', '_')
    res = res.replace('-', '_')
    # 删除尾部`_`
    if res[-1] == '_':
        res = res[:-1]
    return res.strip()


def _fix_col_name(df):
    """修复数据框的列名称"""
    new_names = []
    for col in df.columns:
        res = _normalize_col_name(col)
        new_names.append(res)
    df.columns = new_names
    return df


def t_1_1(df):
    df['注册资本'] = df['注册资本'] * 10000
    df['邮编'] = df['邮编'].map(lambda x: str(x).zfill(6))
    return df


def t_2_1(df):
    df['控股数量'] = df['控股数量(万股)'] * 10000
    del df['控股数量(万股)']
    return df


def t_2_2(df):
    df = _fix_col_name(df)
    exclude = ('最新记录标识',)
    df = _fix_unit(df, 10000, exclude)
    return df


def t_2_3(df):
    # 首先修复数量单位
    df = _fix_unit(df, None)
    # 然后才能修复列名称
    df = _fix_col_name(df)
    return df


def t_2_4(df):
    df = _fix_col_name(df)
    return df


def t_2_5(df):
    df = _fix_col_name(df)
    return df


def t_3_1(df):
    df.rename(columns={"5日平均（MA5）": "MA5",
                       "10日平均（MA10）": "MA10",
                       "30日平均（MA30）": "MA30",
                       "120日均价": "MA120",
                       "股票代码": "证券代码",
                       "股票简称": "证券简称",
                       "52周均价（360日）均价": "MA360"},
              inplace=True)
    df = _fix_col_name(df)
    return df


def t_4_1(df):
    df = _fix_col_name(df)
    return df


def t_5_1(df):
    return df


def t_6_1(df):
    df.rename(columns={"股票代码": "证券代码",
                       "股票简称": "证券简称",
                       "分配股本基数（董 ）万股": "分配股本基数_董",
                       "分配股本基数（股 ）万股": "分配股本基数",
                       "派息金额(人民币 万元)": "派息金额_人民币"},
              inplace=True)
    # 首先修复数量单位
    units = {'送股数量(万股)': 10000, '转增数量(万股)': 10000,
             '派息金额(人民币 万元)': 10000, '送转前总股本(万股)': 10000, '送转后总股本(万股)': 10000,
             '送转前流通股本(万股)': 10000, '送转后流通股本(万股)': 10000, '分配股本基数（董 ）万股': 10000,
             '分配股本基数（股 ）万股': 10000}
    df = _fix_unit(df, units)
    # 然后才能修复列名称
    df = _fix_col_name(df)
    return df


def t_7_1(df):
    return df


def t_7_2(df):
    df = _fix_col_name(df)
    return df


def t_7_3(df):
    df.rename(columns={"配股比例（股）": "配股比例"},
              inplace=True)
    df = _fix_col_name(df)
    return df


def t_7_4(df):
    df = _fix_col_name(df)
    return df


def t_7_5(df):
    df.rename(columns={"实际募资净额(人民币)": "实际募资净额_人民币",
                       "实际募资净额(外币)": "实际募资净额_外币"},
              inplace=True)
    df = _fix_col_name(df)
    return df


def t_8_1_1(df):
    df.rename(columns={"三、营业利润": "营业利润",
                       "四、利润总额": "利润总额",
                       "五、净利润": "净利润",
                       "六、每股收益：": "每股收益",
                       "（一）基本每股收益": "基本每股收益",
                       "一、营业总收入": "营业总收入",
                       "二、营业总成本": "营业总成本",
                       "七、其他综合收益": "其他综合收益",
                       "八、综合收益总额": "综合收益总额"},
              inplace=True)
    df = _fix_col_name(df)
    return df


def t_8_1_2(df):
    df.rename(columns={"四、汇率变动对现金的影响": "汇率变动对现金的影响",
                       "四(2)、其他原因对现金的影响": "其他原因对现金的影响",
                       "五、现金及现金等价物净增加额": "现金及现金等价物净增加额",
                       "1、将净利润调节为经营活动现金流量：": "将净利润调节为经营活动现金流量",
                       "2、不涉及现金收支的重大投资和筹资活动：": "不涉及现金收支的重大投资和筹资活动",
                       "3、现金及现金等价物净变动情况：": "现金及现金等价物净变动情况"},
              inplace=True)
    df = _fix_col_name(df)
    return df


def t_8_2_1(df):
    df.rename(columns={"三、营业利润": "营业利润",
                       "四、利润总额": "利润总额",
                       "五、净利润": "净利润",
                       "六、每股收益：": "每股收益",
                       "（一）基本每股收益": "基本每股收益",
                       "一、营业总收入": "营业总收入",
                       "二、营业总成本": "营业总成本",
                       "七、其他综合收益": "其他综合收益",
                       "八、综合收益总额": "综合收益总额"},
              inplace=True)
    df = _fix_col_name(df)
    return df


def t_8_2_2(df):
    df.rename(columns={"四、汇率变动对现金的影响": "汇率变动对现金的影响",
                       "四(2)、其他原因对现金的影响": "其他原因对现金的影响",
                       "五、现金及现金等价物净增加额": "现金及现金等价物净增加额",
                       "1、将净利润调节为经营活动现金流量：": "将净利润调节为经营活动现金流量",
                       "2、不涉及现金收支的重大投资和筹资活动：": "不涉及现金收支的重大投资和筹资活动",
                       "3、现金及现金等价物净变动情况：": "现金及现金等价物净变动情况"},
              inplace=True)
    df = _fix_col_name(df)
    return df


def t_8_2_3(df):
    df.rename(columns={"基本获利能力(EBIT)": "基本获利能力_EBIT"},
              inplace=True)
    df = _fix_col_name(df)
    return df


def t_8_3_1(df):
    df.rename(columns={"所有者权益（或股东权益）合计": "所有者权益或股东权益合计",
                       "实收资本（或股本）": "实收资本或股本",
                       "负债和所有者（或股东权益）合计": "负债和所有者或股东权益合计"},
              inplace=True)
    df = _fix_col_name(df)
    return df


def t_8_3_2(df):
    df.rename(columns={"一、营业总收入": "营业总收入",
                       "二、营业总成本": "营业总成本",
                       "基它收入": "其他收入",
                       "三、营业利润": "营业利润",
                       "四、利润总额": "利润总额",
                       "五、净利润": "净利润",
                       "（一）基本每股收益": "基本每股收益",
                       "（二）稀释每股收益": "稀释每股收益",
                       "七、其他综合收益": "其他综合收益",
                       "八、综合收益总额": "综合收益总额"},
              inplace=True)
    df = _fix_col_name(df)
    return df


def t_8_3_3(df):
    df.rename(columns={"四、汇率变动对现金的影响": "汇率变动对现金的影响",
                       "四(2)、其他原因对现金的影响": "其他原因对现金的影响",
                       "五、现金及现金等价物净增加额": "现金及现金等价物净增加额"},
              inplace=True)
    df = _fix_col_name(df)
    return df


def t_8_3_4(df):
    df.rename(columns={"实收资本（或股本）": "实收资本或股本",
                       "所有者权益（或股东权益）合计": "所有者权益或股东权益合计",
                       "股票代码": "证券代码",
                       "股票简称": "证券简称",
                       "负债和所有者权益（或股东权益）总计": "负债和所有者权益或股东权益总计"},
              inplace=True)
    df = _fix_col_name(df)
    return df


def t_8_3_5(df):
    df.rename(columns={"一、营业收入": "营业收入",
                       "二、营业支出": "营业支出",
                       "三、营业利润": "营业利润",
                       "四、利润总额": "利润总额",
                       "股票代码": "证券代码",
                       "股票简称": "证券简称",
                       "五、净利润": "净利润",
                       "（一）归属于母公司所有者的净利润": "归属于母公司所有者的净利润",
                       "（二）少数股东损益": "少数股东损益",
                       "（一）基本每股收益": "基本每股收益",
                       "（二）稀释每股收益": "稀释每股收益",
                       "七、其他综合收益": "其他综合收益",
                       '八、综合收益总额': '综合收益总额',
                       "其它收益": "其他收益"},
              inplace=True)
    df = _fix_col_name(df)
    return df


def t_8_3_6(df):
    df.rename(columns={"四、汇率变动对现金的影响": "汇率变动对现金的影响",
                       "股票代码": "证券代码",
                       "股票简称": "证券简称",
                       "四(2)、其他原因对现金的影响": "其他原因对现金的影响",
                       "五、现金及现金等价物净增加额": "现金及现金等价物净增加额",
                       "3、现金及现金等价物净变动情况：": "现金及现金等价物净变动情况"},
              inplace=True)
    df = _fix_col_name(df)
    return df


def t_8_4_1(df):
    df.rename(columns={"净资产收益率(扣除非经常性损益)": "净资产收益率_扣除非经常性损益",
                       "净资产收益率-加权(扣除非经常性损益)": "净资产收益率_加权_扣除非经常性损益",
                       "净资产收益率.1": "净资产收益率_1"},
              inplace=True)
    df = _fix_col_name(df)
    return df


def t_8_4_2(df):
    df.rename(columns={"股票代码": "证券代码",
                       "股票简称": "证券简称"},
              inplace=True)
    pre = ''
    for col in df.columns:
        if col[:4] in ('行业均值', '行业排名'):
            df.rename(columns={col: "{}_{}".format(
                pre, col[:4])}, inplace=True)
        else:
            pre = col
    return df


def _factory(level):
    if level == '1.1':
        return t_1_1
    elif level == '2.1':
        return t_2_1
    elif level == '2.2':
        return t_2_2
    elif level == '2.3':
        return t_2_3
    elif level == '2.4':
        return t_2_4
    elif level == '2.5':
        return t_2_5
    elif level == '3.1':
        return t_3_1
    elif level == '4.1':
        return t_4_1
    elif level == '5.1':
        return t_5_1
    elif level == '6.1':
        return t_6_1
    elif level == '7.1':
        return t_7_1
    elif level == '7.2':
        return t_7_2
    elif level == '7.3':
        return t_7_3
    elif level == '7.4':
        return t_7_4
    elif level == '7.5':
        return t_7_5
    elif level == '8.1.1':
        return t_8_1_1
    elif level == '8.1.2':
        return t_8_1_2
    elif level == '8.2.1':
        return t_8_2_1
    elif level == '8.2.2':
        return t_8_2_2
    elif level == '8.2.3':
        return t_8_2_3
    elif level == '8.3.1':
        return t_8_3_1
    elif level == '8.3.2':
        return t_8_3_2
    elif level == '8.3.3':
        return t_8_3_3
    elif level == '8.3.4':
        return t_8_3_4
    elif level == '8.3.5':
        return t_8_3_5
    elif level == '8.3.6':
        return t_8_3_6
    elif level == '8.4.1':
        return t_8_4_1
    elif level == '8.4.2':
        return t_8_4_2
    raise ValueError(f"不支持{level}")


def _fix(df, level):
    func = _factory(level)
    df = func(df)
    return df


def fixed_data(df, level):
    df = _zfill(df)
    df = _to_date(df)
    df = _fix(df, level)
    return df
