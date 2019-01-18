"""
用于整理深证信数据服务平台存入数据库
    1. 代码可能为1，或者带后缀，统一为6位代码
    2. 转换日期时间类型
    3. 文本转换为数字类型

列名称转换规则
    1. 小写数字开头，移除
    2. 大写数字开头，保留
    3. 其中：　－> 其中_
    4. 加：　-> 加_
    5. 减：　-> 减_
    6. / -> 移除
    7. () -> 移除
    8. 类似　５日MA5 -> MA5 
"""
import re

import logbook
import pandas as pd
from sqlalchemy.types import String

from cnswd.sql.szx import Classification
from cnswd.websource.szx import DataBrowser as WebApi
from cnswd.websource.szx.data_browse import LEVEL_MAPS
from cnswd.websource.szx.ops import convert_to_item, item_to_level
from .base import MODEL_MAPS

logger = logbook.Logger('数据库')


# 列名称尾部符合搜索条件的，视同为日期列
DATE_COL_PAT = re.compile('时间$|日$|日A$|日B$|日期$|年度$|报告期$')
U_NUM_B = re.compile(r'^([一二三四五六七八九])[、:：]')
NUM_B = re.compile(r'^(\d){1}、')
IN_B = re.compile(r'^其中[：:]')
S_B = re.compile(r'^减[：:]')
A_B = re.compile(r'^加[：:]')
S_E = re.compile(r'_$')
M_E = re.compile(r'[：:]$')

keys = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I']
replace_dict = {str(x): '{}_'.format(keys[x-1]) for x in range(1, 10)}

u_keys = ['一', '二', '三', '四', '五', '六', '七', '八', '九']
u_replace_dict = {x: '{}_'.format(x) for x in u_keys}


def _replace(m):
    return replace_dict[m.group(1)]


def _u_replace(m):
    return u_replace_dict[m.group(1)]


def _get_uit(x):
    """列名称解析数量单位"""
    if '%' in x:
        return 0.01
    if '‰' in x:
        return 0.001
    if '万元' in x:
        return 10000
    elif '万股' in x:
        return 10000
    elif '亿元' in x:
        return 100000000
    elif '亿股' in x:
        return 100000000
    return 1


def _fix_num(s):
    """修复序列数量"""
    if pd.api.types.is_numeric_dtype(s):
        x = s.name
        u = _get_uit(x)
        return s * u
    else:
        return s


def _fix_num_by(s, unit=10000, exclude=()):
    if s.name in exclude:
        return s
    if pd.api.types.is_numeric_dtype(s):
        return s * unit
    else:
        return s


def _convert_to_numeric(s, include=(), exclude=()):
    assert not (include and exclude), '限定包含与不包含，只能二者选其一'
    # 文本类型转换为数字
    if re.search(DATE_COL_PAT, s.name):
        # 不处理日期类的列
        return s
    if pd.api.types.is_string_dtype(s):
        if include:
            if s.name in include:
                return pd.to_numeric(s, errors='coerce')
        if exclude:
            if s.name not in exclude:
                return pd.to_numeric(s, errors='coerce')
    return s


def _fix_col_name(col):
    """修复列名称"""
    # 必须先修复数据，然后再修复名称！！！
    # 替换
    if col.strip() == "净资产收益率.1":
        return "净资产收益率_1"
    if col.strip() == '5日平均（MA5）':
        return 'MA5'
    if col.strip() == '10日平均（MA10）':
        return 'MA10'
    if col.strip() == '30日平均（MA30）':
        return 'MA30'
    if col.strip() == '120日均价':
        return 'MA120'
    if col.strip() == '52周均价（360日）均价':
        return 'MA360'
    if col.strip() == 'Ａ股户数':
        return 'A股户数'
    # 字符编码问题
    if col.strip() == 'Ａ股户数':
        return 'A股户数'
    if col.strip() == 'Ｂ股户数':
        return 'B股户数'
    if col.strip() == 'Ｈ股户数':
        return 'H股户数'
    # 数字开头的列(带`、`，类似`1、` -> `A_`, `2、` -> `B_`)
    col = re.sub(NUM_B, _replace, col, 1)
    col = re.sub(U_NUM_B, _u_replace, col, 1)

    col = re.sub(IN_B, '其中_', col, 1)
    col = re.sub(A_B, '加_', col, 1)
    col = re.sub(S_B, '减_', col, 1)

    # 去除尾部无效字符
    col = re.sub(M_E, '', col, 1)
    col = re.sub(S_E, '', col, 1)
    # 替换
    col = col.replace('基它', '其他')
    col = col.replace('其它', '其他')

    col = col.replace('-', '_')
    col = col.replace('、', '_')
    col = col.replace('：', '_')
    col = col.replace('，', '_')
    col = col.replace('/', '_')
    # 去除
    col = col.replace('(%)', '')  # 注意`%`与`(%)`顺序
    col = col.replace('(‰)', '')
    col = col.replace('(股)', '')
    col = col.replace('(元)', '')
    col = col.replace('(万元)', '')
    col = col.replace('(万股)', '')
    col = col.replace('(', '')
    col = col.replace(')', '')
    col = col.replace('（', '')
    col = col.replace('）', '')
    col = col.replace(' ', '')
    col = col.strip()
    return col


def _to_date(df, col):
    """转换日期"""
    df[col] = pd.to_datetime(
        df[col], infer_datetime_format=True, errors='coerce')
    return df


def _to_code(x):
    """转换股票代码"""
    return str(x).zfill(6)


def _to_sql_table(df, level, engine, index_label, if_exists='append', dtype={}, save=True):
    """写入sql数据库中的表
    
    Arguments:
        df {pd.DataFrame} -- 数据框对象
        level {str} -- 写入的表名称
        index_label {str or sequence} -- 索引列
        save {bool} -- 是否存储，默认为真
            为观察存入数据库前的结果，当设定为假时，返回修改列名称、数据类型后的DataFrame对象

    Keyword Arguments:
        if_exists {str} -- 如果表存在的情形下，如何操作 (default: {'append'})
        dtype {dict} -- 转换数据类型字典 (default: {{}})
    """
    if '股票代码' in df.columns:
        dtype['股票代码'] = String(6)
    if '股票简称' in df.columns:
        dtype['股票简称'] = String(10)
    if index_label:
        df.set_index(index_label, inplace=True)
    # 修复数量单位
    df = df.apply(_fix_num)
    # 修复列名称
    df.columns = df.columns.map(_fix_col_name)
    # 修复列名称后，再进行日期列的处理！！！
    for col in df.columns:
        if re.search(DATE_COL_PAT, col):
            df = _to_date(df, col)
    table_name = MODEL_MAPS[level].__tablename__
    if save:
        df.to_sql(table_name, con=engine, if_exists=if_exists,
                  index=True, index_label=index_label, dtype=dtype)
        logger.info(f'表 {table_name} 添加{df.shape[0]:>4} 行')
    else:
        return df


def t_1_1(table, engine, save):
    """添加股票基本资料到数据库中的stock_infos表"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称",
                       "证券类别": "股票类别"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(_convert_to_numeric, include=('注册资本',))
    # 改为元为单位
    df['注册资本'] = df['注册资本'] * 10000
    return _to_sql_table(df, '1.1', engine, '股票代码', save=save)


def t_2_1(table, engine, save):
    """添加公司股东实际控制人到数据库中的actual_controllers表"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    return _to_sql_table(df, '2.1', engine, '股票代码', save=save)


def t_2_2(table, engine, save):
    """公司股本变动"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(_convert_to_numeric, exclude=(
        '股票代码', '股票简称', '机构名称', '变动原因',))
    # 数量*10000
    df = df.apply(_fix_num_by, exclude=('变动原因编码', '最新记录标识'))
    return _to_sql_table(df, '2.2', engine, '股票代码', save=save)


def t_2_3(table, engine, save):
    """上市公司高管持股变动"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(_convert_to_numeric, include=('期末市值(万元)',))
    return _to_sql_table(df, '2.3', engine, '股票代码', save=save)


def t_2_4(table, engine, save):
    """添加股东增（减）持情况到数据库中的shareholder_share_changes表"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    # df['增（减）持截止日'] = pd.to_datetime(df['增（减）持截止日'], 'coerce')
    df = df.apply(_convert_to_numeric, include=('增（减）持价格上限', '变动后占比',))
    return _to_sql_table(df, '2.4', engine, '股票代码', save=save)


def t_2_5(table, engine, save):
    """持股集中度"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(_convert_to_numeric, include=('Ａ股户数', 'Ｂ股户数', 'Ｈ股户数',
                                                '股东持股数量', '股东持股比例', '股东持股比例比上报告期增减'))
    return _to_sql_table(df, '2.5', engine, '股票代码', save=save)


def t_3_1(table, engine, save):
    """添加行情数据到数据库中的quotes表"""
    if table.empty:
        return
    df = table.copy()
    df['股票代码'] = df['股票代码'].str.slice(0, 6)
    df = df.apply(_convert_to_numeric, exclude=(
        '股票代码', '股票简称', '交易日期', '交易所'

    ))
    df['涨跌幅'] = df['涨跌幅'] / 100.
    return _to_sql_table(df, '3.1', engine, '股票代码', save=save)


def t_4_1(table, engine, save):
    """投资评级"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df = df.apply(_convert_to_numeric, include=('目标价格（下限）', '目标价格（上限）'))
    df['股票代码'] = df['股票代码'].map(_to_code)
    return _to_sql_table(df, '4.1', engine, '股票代码', save=save)


def t_5_1(table, engine, save):
    """上市公司业绩预告"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(_convert_to_numeric, include=(
        '本期净利润下限', '本期净利润上限', '本期净利润增减幅下限', '本期净利润增减幅上限'))
    return _to_sql_table(df, '5.1', engine, '股票代码', save=save)


def t_6_1(table, engine, save=True):
    """分红指标"""
    if table.empty:
        return
    df = table.copy()
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(
        _convert_to_numeric,
        include=('分配股本基数（实施）', '送股比例', '转增比例',
                 '派息比例(人民币)', '派息比例（美元）', '派息比例（港币）',
                 '送股数量(万股)', '转增数量(万股)',
                 '派息金额(人民币 万元)', '送转前总股本(万股)',
                 '送转后总股本(万股)', '送转前流通股本(万股)', '送转后流通股本(万股)',
                 '分配股本基数（董 ）万股', '分配股本基数（股 ）万股', '汇率',)
    )
    return _to_sql_table(df, '6.1', engine, '股票代码', save=save)


def t_7_1(table, engine, save=True):
    """公司增发股票预案"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    return _to_sql_table(df, '7.1', engine, '股票代码', save=save)


def t_7_2(table, engine, save=True):
    """公司增发股票实施方案"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(
        _convert_to_numeric,
        include=('竞价上限', '竞价下限', '老股东配售价格',
                 '老股东配售比例', '加权发行市盈率', '摊薄发行市盈率',
                 '发行前每股净资产', '发行后每股净资产',
                 '实际募资净额（外币）', '发行费用总额', '承销费用', '每股发行费用',
                 '发行前总股本', '发行后总股本', '发行前流通股本', '发行后流通股本',
                 '网上申购上限', '网上发行中签率', '网下发行中签率', '起额认购倍数',
                 '网上发行数量', '老股东配售数量', '网下配售数量', '承销余额',
                 '回拨数量', '网上有效申购股数', '网上有效申购资金', '网上有效申购户数',
                 '老股东有效申购股数', '老股东有效申购户数', '老股东有效申购资金',
                 '网下有效申购户数', '网下有效申购股数', '网下有效申购资金',
                 '一般法人申购数量', '一般法人配售数量', '一般法人配售户数',
                 '证券基金申购数量', '证券基金配售数量', '证券基金配售户数',
                 '战略投资者申购数量', '战略投资者配售数量', '战略投资者配售户数',
                 '预计发行股数', '网上预设发行数量比例', '网下预设发行数量比例',
                 '原股东最多认购数占发行数比例', '网下申购数量下限',
                 '网下申购定金', 'A类投资者网下申购中签比例', 'B类投资者网下申购中签比例',
                 '网下申购数量上限', 'A类投资者认购下限', 'B类投资者认购下限',
                 '网上申购数量下限', '发行后每股收益',)
    )
    return _to_sql_table(df, '7.2', engine, '股票代码', save=save)


def t_7_3(table, engine, save=True):
    """公司配股预案"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(
        _convert_to_numeric,
        include=('预计配股价格上限', '预计配股价格下限')
    )
    return _to_sql_table(df, '7.3', engine, '股票代码', save=save)


def t_7_4(table, engine, save=True):
    """公司配股实施方案"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(
        _convert_to_numeric,
        include=('承销余额', '国家股实配数量', '法人股实配数量', '职工股实配数量',
                 '转配股实配数量', '其他股份实配数量', '可转配股数量', '公众获转配数量',
                 '每股配权转让费',)
    )
    return _to_sql_table(df, '7.4', engine, '股票代码', save=save)


def t_7_5(table, engine, save=True):
    """公司首发股票"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(
        _convert_to_numeric,
        exclude=('股票代码', '股票简称', '机构名称',
                 '股票类别编码', '股票类别', '外币币种编码', '外币币种',
                 '发行定价方式', '发行对象', '发行方式编码', '发行方式',
                 '承销方式编码', '承销方式', '发行地区',
                 '发行地区编码', '主要发起人', '分配承诺',
                 '招股意向书网址', '记录标识')
    )
    return _to_sql_table(df, '7.5', engine, '股票代码', save=save)


def t_8_1_1(table, engine, save=True):
    """个股TTM财务利润表"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(
        _convert_to_numeric,
        exclude=('股票代码', '股票简称', '合并类型')
    )
    return _to_sql_table(df, '8.1.1', engine, '股票代码', save=save)


def t_8_1_2(table, engine, save=True):
    """个股TTM现金流量表"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(
        _convert_to_numeric,
        exclude=('股票代码', '股票简称', '合并类型编码', '合并类型',)
    )
    return _to_sql_table(df, '8.1.2', engine, '股票代码', save=save)


def t_8_2_1(table, engine, save=True):
    """个股单季财务利润表"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(
        _convert_to_numeric,
        exclude=('股票代码', '股票简称', '合并类型', )
    )
    return _to_sql_table(df, '8.2.1', engine, '股票代码', save=save)


def t_8_2_2(table, engine, save=True):
    """个股单季现金流量表"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(
        _convert_to_numeric,
        exclude=('股票代码', '股票简称', '合并类型', )
    )
    return _to_sql_table(df, '8.2.2', engine, '股票代码', save=save)


def t_8_2_3(table, engine, save=True):
    """个股单季财务指标"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(
        _convert_to_numeric,
        exclude=('股票代码', '股票简称', '合并类型', )
    )
    return _to_sql_table(df, '8.2.3', engine, '股票代码', save=save)


def t_8_3_1(table, engine, save=True):
    """个股报告期资产负债表"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(
        _convert_to_numeric,
        exclude=('股票代码', '股票简称', '机构名称', '合并类型', '报表来源', )
    )
    return _to_sql_table(df, '8.3.1', engine, '股票代码', save=save)


def t_8_3_2(table, engine, save=True):
    """8.3.2 个股报告期利润表"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(
        _convert_to_numeric,
        exclude=('股票代码', '股票简称', '机构名称', '合并类型', '报表来源', '备注')
    )
    return _to_sql_table(df, '8.3.2', engine, '股票代码', save=save)


def t_8_3_3(table, engine, save=True):
    """个股报告期现金表"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(
        _convert_to_numeric,
        exclude=('股票代码', '股票简称', '机构名称', '合并类型', '报表来源', )
    )
    return _to_sql_table(df, '8.3.3', engine, '股票代码', save=save)


def t_8_3_4(table, engine, save=True):
    """金融类资产负债表2007版"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称",
                       },
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(
        _convert_to_numeric,
        exclude=('股票代码', '股票简称', '机构名称', '合并类型', '报表来源', )
    )
    return _to_sql_table(df, '8.3.4', engine, '股票代码', save=save)


def t_8_3_5(table, engine, save=True):
    """金融类利润表2007版"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(
        _convert_to_numeric,
        exclude=('股票代码', '股票简称', '机构名称', '合并类型', '报表来源', )
    )
    return _to_sql_table(df, '8.3.5', engine, '股票代码', save=save)


def t_8_3_6(table, engine, save=True):
    """金融类现金流量表2007版"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(
        _convert_to_numeric,
        exclude=('股票代码', '股票简称', '机构名称', '合并类型', '报表来源', )
    )
    return _to_sql_table(df, '8.3.6', engine, '股票代码', save=save)


def t_8_4_1(table, engine, save=True):
    """个股报告期指标表"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    df = df.apply(
        _convert_to_numeric,
        exclude=('股票代码', '股票简称', '机构名称', '合并类型', '数据来源',)
    )
    return _to_sql_table(df, '8.4.1', engine, '股票代码', save=save)


def t_8_4_2(table, engine, save=True):
    """财务指标行业排名"""
    if table.empty:
        return
    df = table.copy()
    df.rename(columns={"证券代码": "股票代码",
                       "证券简称": "股票简称"},
              inplace=True)
    df['股票代码'] = df['股票代码'].map(_to_code)
    pre = ''
    for col in df.columns:
        if col[:4] in ('行业均值', '行业排名'):
            df.rename(columns={col: "{}_{}".format(
                pre, col[:4])}, inplace=True)
        else:
            pre = col

    df = df.apply(
        _convert_to_numeric,
        exclude=('股票代码', '股票简称', '报告期', '行业ID', '级别说明', '行业名称',)
    )
    return _to_sql_table(df, '8.4.2', engine, '股票代码', save=save)


def write_to_sql(engine, df, input_item, save=True):
    """写入指定数据类型的数据框到数据库"""
    item = convert_to_item(input_item, LEVEL_MAPS)
    level = item_to_level(item, LEVEL_MAPS)
    if level == '1.1':
        return t_1_1(df, engine, save)
    elif level == '2.1':
        return t_2_1(df, engine, save)
    elif level == '2.2':
        return t_2_2(df, engine, save)
    elif level == '2.3':
        return t_2_3(df, engine, save)
    elif level == '2.4':
        return t_2_4(df, engine, save)
    elif level == '2.5':
        return t_2_5(df, engine, save)
    elif level == '3.1':
        return t_3_1(df, engine, save)
    elif level == '4.1':
        return t_4_1(df, engine, save)
    elif level == '5.1':
        return t_5_1(df, engine, save)
    elif level == '6.1':
        return t_6_1(df, engine, save)
    elif level == '7.1':
        return t_7_1(df, engine, save)
    elif level == '7.2':
        return t_7_2(df, engine, save)
    elif level == '7.3':
        return t_7_3(df, engine, save)
    elif level == '7.4':
        return t_7_4(df, engine, save)
    elif level == '7.5':
        return t_7_5(df, engine, save)
    elif level == '8.1.1':
        return t_8_1_1(df, engine, save)
    elif level == '8.1.2':
        return t_8_1_2(df, engine, save)
    elif level == '8.2.1':
        return t_8_2_1(df, engine, save)
    elif level == '8.2.2':
        return t_8_2_2(df, engine, save)
    elif level == '8.2.3':
        return t_8_2_3(df, engine, save)
    elif level == '8.3.1':
        return t_8_3_1(df, engine, save)
    elif level == '8.3.2':
        return t_8_3_2(df, engine, save)
    elif level == '8.3.3':
        return t_8_3_3(df, engine, save)
    elif level == '8.3.4':
        return t_8_3_4(df, engine, save)
    elif level == '8.3.5':
        return t_8_3_5(df, engine, save)
    elif level == '8.3.6':
        return t_8_3_6(df, engine, save)
    elif level == '8.4.1':
        return t_8_4_1(df, engine, save)
    elif level == '8.4.2':
        return t_8_4_2(df, engine, save)
    else:
        raise NotImplementedError('无法写入数据项目为“{}”的数据'.format(item))
