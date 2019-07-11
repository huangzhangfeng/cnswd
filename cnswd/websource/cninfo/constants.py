TIMEOUT = 20             # 标准等待时间，单位：秒
# 轮询时间缩短
POLL_FREQUENCY = 0.3

HOME_URL_FMT = 'http://webapi.cninfo.com.cn/#/{}'

API_MAPS = {
    5:  ('个股API', 'dataDownload'),
    6:  ('行情中心', 'marketData'),
    7:  ('数据搜索', 'dataBrowse'),
    8:  ('专题统计', 'thematicStatistics'),
    9:  ('公司快照', 'company'),
    10:  ('公告定制', 'notice'),
}


DB_NAME = {
    '1.1':    '基本资料',
    '2.1':    '公司股东实际控制人',
    '2.2':    '公司股本变动',
    '2.3':    '上市公司高管持股变动',
    '2.4':    '股东增（减）持情况',
    '2.5':    '持股集中度',
    '3.1':    '行情数据',
    '4.1':    '投资评级',
    '5.1':    '上市公司业绩预告',
    '6.1':    '分红指标',
    '7.1':    '公司增发股票预案',
    '7.2':    '公司增发股票实施方案',
    '7.3':    '公司配股预案',
    '7.4':    '公司配股实施方案',
    '7.5':    '公司首发股票',
    '8.1.1':  '个股TTM财务利润表',
    '8.1.2':  '个股TTM现金流量表',
    '8.2.1':  '个股单季财务利润表',
    '8.2.2':  '个股单季现金流量表',
    '8.2.3':  '个股单季财务指标',
    '8.3.1':  '个股报告期资产负债表',
    '8.3.2':  '个股报告期利润表',
    '8.3.3':  '个股报告期现金表',
    '8.3.4':  '金融类资产负债表2007版',
    '8.3.5':  '金融类利润表2007版',
    '8.3.6':  '金融类现金流量表2007版',
    '8.4.1':  '个股报告期指标表',
    '8.4.2':  '财务指标行业排名',
}

DB_CSS = {
    '1.1':   (None, None),
    '2.1':   ('input.date:nth-child(1)', 'input.form-control:nth-child(2)'),
    '2.2':   ('input.form-control:nth-child(1)', 'input.date:nth-child(2)'),
    '2.3':   ('input.form-control:nth-child(1)', 'input.date:nth-child(2)'),
    '2.4':   ('input.form-control:nth-child(1)', 'input.date:nth-child(2)'),
    '2.5':   ('#se1_sele', '.condition2 > select:nth-child(2)'),
    '3.1':   ('input.form-control:nth-child(1)', 'input.date:nth-child(2)'),
    '4.1':   ('input.form-control:nth-child(1)', 'input.date:nth-child(2)'),
    '5.1':   ('#se1_sele', '.condition2 > select:nth-child(2)'),
    '6.1':   ('#se2_sele', None),
    '7.1':   ('input.form-control:nth-child(1)', 'input.date:nth-child(2)'),
    '7.2':   ('input.date:nth-child(1)', 'input.form-control:nth-child(2)'),
    '7.3':   ('input.form-control:nth-child(1)', 'input.date:nth-child(2)'),
    '7.4':   ('input.form-control:nth-child(1)', 'input.date:nth-child(2)'),
    '7.5':   (None, None),
    '8.1.1': ('#se1_sele', '.condition2 > select:nth-child(2)'),
    '8.1.2': ('#se1_sele', '.condition2 > select:nth-child(2)'),
    '8.2.1': ('#se1_sele', '.condition2 > select:nth-child(2)'),
    '8.2.2': ('#se1_sele', '.condition2 > select:nth-child(2)'),
    '8.2.3': ('#se1_sele', '.condition2 > select:nth-child(2)'),
    '8.3.1': ('#se1_sele', '.condition2 > select:nth-child(2)'),
    '8.3.2': ('#se1_sele', '.condition2 > select:nth-child(2)'),
    '8.3.3': ('#se1_sele', '.condition2 > select:nth-child(2)'),
    '8.3.4': ('#se1_sele', '.condition2 > select:nth-child(2)'),
    '8.3.5': ('#se1_sele', '.condition2 > select:nth-child(2)'),
    '8.3.6': ('#se1_sele', '.condition2 > select:nth-child(2)'),
    '8.4.1': ('#se1_sele', '.condition2 > select:nth-child(2)'),
    '8.4.2': ('#se1_sele', '.condition2 > select:nth-child(2)'),
}

# 日期包含二部分
# 1. 循环与格式符号
# 2. 是否排除未来日期

# 循环与格式符号说明
# 第一个代表周期，第二个字符代表格式
# QD -> 按季度循环，以日期格式表达
# QQ -> 按季度循环，t1 按年份数 t2 按季度
# 以月度循环都不能排除未来日期
# 例如当日为 xx-05 查询xx月，表达为`xx-01 ~ xx-30`，结果为空集
DB_DATE_FREQ = {
    '1.1':    (None, None),
    '2.1':    ('QD', True),
    '2.2':    ('QD', True),
    '2.3':    ('MD', True),  # 数据库与网络数据 同一期间，居然数据不同。怀疑日期字段设置问题
    '2.4':    ('QD', True),
    '2.5':    ('QQ', True),
    '3.1':    ('BD', True),  # 日常刷新使用`B`
    '4.1':    ('MD', True),
    '5.1':    ('QQ', False),
    '6.1':    ('YY', False),
    '7.1':    ('QD', False),
    '7.2':    ('QD', False),
    '7.3':    ('QD', False),
    '7.4':    ('QD', False),
    '7.5':    (None, None),
    '8.1.1':  ('QQ', True),
    '8.1.2':  ('QQ', True),
    '8.2.1':  ('QQ', True),
    '8.2.2':  ('QQ', True),
    '8.2.3':  ('QQ', True),
    '8.3.1':  ('QQ', True),
    '8.3.2':  ('QQ', True),
    '8.3.3':  ('QQ', True),
    '8.3.4':  ('QQ', True),
    '8.3.5':  ('QQ', True),
    '8.3.6':  ('QQ', True),
    '8.4.1':  ('QQ', True),
    '8.4.2':  ('QQ', True),
}

# 专题统计映射参数： 名称 t1_css t2_css 可选css
TS_NAME = {
    # 1 新股数据
    # '1.1':      '新股申购',
    # '1.2':      '新股发行',
    # '1.3':      '新股过会',
    # 2 发行筹资
    '2.1':      '首发审核',
    '2.2':      '首发筹资',
    '2.3':      '增发筹资',
    '2.4':      '配股筹资',
    '2.5':      '公司债或可转债',
    # 3 股东股本
    '3.1':      '解禁报表明细',
    '3.2':      '按天减持明细',
    '3.3':      '按天增持明细',
    # '3.4':      '减持汇总统计',
    # '3.5':      '增持汇总统计',
    '3.6':      '股本情况',
    # '3.7':      '高管持股变动明细',
    # '3.8':      '高管持股变动汇总',
    # '3.9':      '实际控制人持股变动',
    # '3.10':     '股东人数及持股集中度',
    # 4 业绩与分红
    # '4.1':      '业绩预告',
    # '4.2':      '预告业绩扭亏个股',
    # '4.3':      '预告业绩大幅下降个股',
    # '4.4':      '预告业绩大幅上升个股',
    # '4.5':      '地区分红明细',
    # '4.6':      '行业分红明细',
    # 5 公司治理
    '5.1':      '资产重组',
    '5.2':      '债务重组',
    '5.3':      '吸收合并',
    '5.4':      '股权变更',
    # '5.5':      '对外担保',
    # '5.6':      '公司诉讼',
    # 6 财务报表
    # '6.1':      '个股主要指标',
    # '6.2':      '分地区财务指标',
    # '6.3':      '分行业财务指标',
    # '6.4':      '分市场财务指标',
    # 7 行业分析
    '7.1':      '行业市盈率',
    # 8 评级预测
    # '8.1':      '投资评级',
    # 9 市场交易
    '9.1':      '大宗交易报表',
    # '9.2':      '融资融券明细',
    # 10 信息提示
    '10.1':     '股东大会召开情况',
    '10.2':     '股东大会相关事项变动',
    '10.3':     '股东大会议案表',
    '10.4':     '停复牌',
    '10.5':     '市场公开信息汇总',
    '10.6':     '拟上市公司清单',
    '10.7':     '暂停上市公司清单',
    '10.8':     '终止上市公司清单',
    # 11 基金报表
    '11.1':     '基金净值增长率',
}

# t1_css t2_css option_css
TS_CSS = {
    # 1 新股数据
    '1.1':      (None, None, None),
    '1.2':      (None, None, None),  # 选择默认值
    '1.3':      (None, None, None),
    # 2 发行筹资
    '2.1':      ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', '.condition6 > select:nth-child(2)'), # 全部
    '2.2':      ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', None),
    '2.3':      ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', None),
    '2.4':      ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', None),
    '2.5':      ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', '.condition6 > select:nth-child(2)'), # 逐项
    # 3 股东股本
    '3.1':      ('#fBDatepair > input:nth-child(1)', None, None),
    '3.2':      ('#fBDatepair > input:nth-child(1)', None, None),
    '3.3':      ('#fBDatepair > input:nth-child(1)', None, None),
    '3.4':      ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', None),
    '3.5':      ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', None),
    '3.6':      (None, None, '.condition6 > select:nth-child(2)'), # 全部
    '3.7':      ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', '.condition6 > select:nth-child(2)'), # 逐项
    '3.8':      ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', '.condition6 > select:nth-child(2)'), # 逐项
    # '3.9':      (None, None, '.condition6 > select:nth-child(2)'), # 全部
    '3.10':     ('#seee1_sele', '.condition2 > select:nth-child(2)', None),
    # 4 业绩与分红
    '4.1':      ('#seee1_sele', '.condition2 > select:nth-child(2)', None),
    '4.2':      ('#seee1_sele', '.condition2 > select:nth-child(2)', None),
    '4.3':      ('#seee1_sele', '.condition2 > select:nth-child(2)', None),
    '4.4':      ('#seee1_sele', '.condition2 > select:nth-child(2)', None),
    '4.5':      ('#seee1_sele', None, '.condition6 > select:nth-child(2)'),  # 逐项
    '4.6':      ('#seee1_sele', None, '.condition6 > select:nth-child(2)'),  # 逐项
    # 5 公司治理
    '5.1':      ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', '.condition6 > select:nth-child(2)'), # 全部
    '5.2':      ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', None),
    '5.3':      ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', None),
    '5.4':      ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', '.condition6 > select:nth-child(2)'), # 全部
    '5.5':      ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', '.condition6 > select:nth-child(2)'), # 全部
    '5.6':      ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', '.condition6 > select:nth-child(2)'), # 全部
    # 6 财务报表
    '6.1':      ('#seee1_sele', '.condition2 > select:nth-child(2)', None),
    '6.2':      ('#seee1_sele', '.condition2 > select:nth-child(2)','.condition6 > select:nth-child(2)'), # 逐项
    '6.3':      ('#seee1_sele', '.condition2 > select:nth-child(2)','.condition6 > select:nth-child(2)'), # 逐项
    '6.4':      ('#seee1_sele', '.condition2 > select:nth-child(2)', None),
    # 7 行业分析
    '7.1':      ('#fBDatepair > input:nth-child(1)', None, '.condition6 > select:nth-child(2)'),  # 逐项
    # 8 评级预测
    '8.1':      ('#fBDatepair > input:nth-child(1)', None, None),
    # 9 市场交易
    '9.1':      ('#fBDatepair > input:nth-child(1)', None, None),
    '9.2':      ('#fBDatepair > input:nth-child(1)', None, None),
    # 10 信息提示
    '10.1':     ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', None),
    '10.2':     ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', None),
    '10.3':     ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', None),
    '10.4':     ('#dBDatepair > input:nth-child(1)', 'input.form-control:nth-child(2)', None),
    '10.5':     ('#fBDatepair > input:nth-child(1)', None, None),
    '10.6':     (None, None, None),
    '10.7':     (None, None, None),
    '10.8':     (None, None, None),
    # 11 基金报表
    '11.1':     ('#fBDatepair > input:nth-child(1)', None, '.condition6 > select:nth-child(2)'),
}

# 期间相关
# 循环符号说明
# 第一个代码周期，第二个字符代码格式
# QD -> 按季度循环，以日期格式表达
# QQ -> 按季度循环，t1 按年份数 t2 按季度
TS_DATE_FREQ = {
    # 1 新股数据
    '1.1':      (None, None),
    '1.2':      (None, None),
    '1.3':      (None, None),
    # 2 发行筹资
    '2.1':      ('MD', False),
    '2.2':      ('MD', False),
    '2.3':      ('MD', False),
    '2.4':      ('MD', False),
    '2.5':      ('MD', False),
    # 3 股东股本
    '3.1':      ('DD', False),
    '3.2':      ('DD', False),
    '3.3':      ('DD', False),
    '3.4':      ('MD', False),
    '3.5':      ('MD', False),
    '3.6':      (None, None),
    # '3.7':      ('截止日期',False), # 网上API查询逻辑存在错误
    # '3.8':      ('截止日期',False), # 网上API查询逻辑存在错误
    # '3.9':      (None, None),
    '3.10':     ('QQ', True),
    # 4 业绩与分红
    # '4.1':      ('QQ', True),
    '4.2':      ('QQ', True),
    '4.3':      ('QQ', True),
    '4.4':      ('QQ', True),
    '4.5':      ('YY', True),
    '4.6':      ('YY', True),
    # 5 公司治理
    '5.1':      ('MD', False),
    '5.2':      ('MD', False),
    '5.3':      ('MD', False),
    '5.4':      ('MD', False),
    # '5.5':      '对外担保',
    # '5.6':      '公司诉讼',
    # 6 财务报表
    '6.1':      ('QQ', True),
    '6.2':      ('QQ', True),
    '6.3':      ('QQ', True),
    '6.4':      ('QQ', True),
    # 7 行业分析
    '7.1':      ('BD', True),
    # 8 评级预测
    '8.1':      ('DD', True),
    # 9 市场交易
    '9.1':      ('DD', True),
    '9.2':      ('BD', True),
    # 10 信息提示
    '10.1':     ('MD', True),
    '10.2':     ('MD', True),
    '10.3':     ('MD', True),
    '10.4':     ('MD', True),
    '10.5':     ('DD', True),
    '10.6':     (None, None),
    '10.7':     (None, None),
    '10.8':     (None, None),
    # 11 基金报表
    '11.1':     ('BD', True),
}
