"""

深证信数据基础模块

"""
from cnswd.constants import MARKET_START
from cnswd.sql.data_browse import (
    IPO, 
    ActualController,
    AdditionalStockImplementation, 
    AdditionalStockPlan,
    Classification, 
    CompanyShareChange, 
    Dividend,
    ExecutivesShareChange, 
    FinancialIndicatorRanking,
    InvestmentRating, 
    PerformanceForecaste,
    PeriodlyBalanceSheet, 
    PeriodlyBalanceSheet2007,
    PeriodlyCashFlowStatement,
    PeriodlyCashFlowStatement2007,
    PeriodlyFinancialIndicator, 
    PeriodlyIncomeStatement,
    PeriodlyIncomeStatement2007,
    QuarterlyCashFlowStatement,
    QuarterlyFinancialIndicator,
    QuarterlyIncomeStatement, 
    Quote,
    ShareholderShareChange, 
    ShareholdingConcentration,
    SharePlacementImplementation, 
    SharePlacementPlan,
    StockInfo, 
    TtmCashFlowStatement, 
    TtmIncomeStatement
)

from cnswd.sql.thematic_statistics import (
    IPOAudit,
    IpoFinancing,
    Raise,
    Placement,
    Bond,
    Deregulation,
    Underweight,
    Overweight,
    Equity,
    # ActualController,
    # Concentration,
    # PerformanceForecast,
    Reorganization,
    DebtRestructuring,
    Merger,
    ShareVariation,
    Pe,
    BigDeal,
    Meeting,
    MeetingChange,
    Motion,
    SuspensionAndResumption,
    MarketPublicInformation,
    Proposed,
    Suspend,
    Delisted,
    Rnav,
)

DB_MODEL_MAPS = {
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

default_start_date = MARKET_START.tz_localize(None).strftime(r'%Y-%m-%d')
# 第一项为数据库字段名称，第二项为默认开始刷新日期
DB_DATE_FIELD = {
    '1.1':   (None,             None),
    '2.1':   ('变动日期',        '2006-12-31'),
    '2.2':   ('变动日期',        default_start_date),
    '2.3':   ('截止日期',        '2004-11-29'),
    '2.4':   ('增_减_持截止日',  '2006-09-30'),
    '2.5':   ('截止日期',        '1997-06-30'),
    '3.1':   ('交易日期',        default_start_date),
    '4.1':   ('发布日期',        '2003-01-02'),
    '5.1':   ('报告年度',        '2001-06-30'),
    '6.1':   ('分红年度',        default_start_date),
    '7.1':   ('公告日期',        '1996-11-29'),
    '7.2':   ('公告日期',        '1996-11-29'),
    '7.3':   ('公告日期',        '1993-03-13'),
    '7.4':   ('公告日期',        '1993-03-13'),
    '7.5':   (None,              None),
    '8.1.1': ('报告年度',        default_start_date),
    '8.1.2': ('报告年度',        default_start_date),
    '8.2.1': ('报告年度',        default_start_date),
    '8.2.2': ('报告年度',        default_start_date),
    '8.2.3': ('报告年度',        default_start_date),
    '8.3.1': ('报告年度',        default_start_date),
    '8.3.2': ('报告年度',        default_start_date),
    '8.3.3': ('报告年度',        default_start_date),
    '8.3.4': ('报告年度',       '2006-03-31'),
    '8.3.5': ('报告年度',       '2006-03-31'),
    '8.3.6': ('报告年度',       '2006-03-31'),
    '8.4.1': ('报告年度',       '1991-12-31'),
    '8.4.2': ('报告期',         '1991-12-31'),
}

TS_MODEL_MAPS = {
    # 1 新股数据
    # '1.1':      '新股申购',
    # '1.2':      '新股发行',
    # '1.3':      '新股过会',
    # 2 发行筹资
    '2.1':      IPOAudit,
    '2.2':      IpoFinancing,
    '2.3':      Raise,
    '2.4':      Placement,
    '2.5':      Bond,
    # 3 股东股本
    '3.1':      Deregulation,
    '3.2':      Underweight,
    '3.3':      Overweight,
    # '3.4':      '减持汇总统计',
    # '3.5':      '增持汇总统计',
    '3.6':      Equity,
    # '3.7':      '高管持股变动明细',
    # '3.8':      '高管持股变动汇总',
    # '3.9':      ActualController,
    # '3.10':     Concentration,
    # 4 业绩与分红
    # '4.1':      PerformanceForecast,
    # '4.2':      '预告业绩扭亏个股',
    # '4.3':      '预告业绩大幅下降个股',
    # '4.4':      '预告业绩大幅上升个股',
    # '4.5':      '地区分红明细',
    # '4.6':      '行业分红明细',
    # 5 公司治理
    '5.1':      Reorganization,
    '5.2':      DebtRestructuring,
    '5.3':      Merger,
    '5.4':      ShareVariation,
    # '5.5':      '对外担保',
    # '5.6':      '公司诉讼',
    # 6 财务报表
    # '6.1':      '个股主要指标',
    # '6.2':      '分地区财务指标',
    # '6.3':      '分行业财务指标',
    # '6.4':      '分市场财务指标',
    # 7 行业分析
    '7.1':      Pe,
    # 8 评级预测
    # '8.1':      '投资评级',
    # 9 市场交易
    '9.1':      BigDeal,
    # '9.2':      '融资融券明细',
    # 10 信息提示
    '10.1':     Meeting,
    '10.2':     MeetingChange,
    '10.3':     Motion,
    '10.4':     SuspensionAndResumption,
    '10.5':     MarketPublicInformation,
    '10.6':     Proposed,
    '10.7':     Suspend,
    '10.8':     Delisted,
    # 11 基金报表
    '11.1':     Rnav,
}


# 日期字段名称 默认开始日期

TS_DATE_FIELD = {
    # 1 新股数据
    # '1.1':      (None, None),
    # '1.2':      (None, None),
    # '1.3':      (None, None),
    # 2 发行筹资
    '2.1':      ('公告日期', '2012-02-01'),
    '2.2':      ('上市日期', '1990-12-10'),
    '2.3':      ('发行日期', '1998-06-26'),
    '2.4':      ('配股日期', '1991-06-01'),
    '2.5':      ('发行起始日', '1992-11-19'),
    # 3 股东股本
    '3.1':      ('实际解除限售日期', '2000-09-01'),
    '3.2':      ('公告日期', '2007-01-01'),
    '3.3':      ('公告日期', '2007-01-01'),
    # '3.4':      ('公告日期', '2007-09-12'),
    # '3.5':      ('公告日期', '2007-09-12'),
    '3.6':      (None, None),
    # '3.7':      ('截止日期','2009-08-19'), # 网上API查询逻辑存在错误
    # '3.8':      ('截止日期','2009-08-19'), # 网上API查询逻辑存在错误
    # '3.9':      (None, None),
    # '3.10':     ('变动日期', '2000-12-31'),
    # 4 业绩与分红
    # '4.1':      ('报告年度', '2000-12-31'),
    # '4.2':      ('报告年度', '2000-12-31'),
    # '4.3':      ('报告年度', '2000-12-31'),
    # '4.4':      ('报告年度', '2000-12-31'),
    # '4.5':      ('当前年度', '2000-12-31'),
    # '4.6':      ('当前年度', '2000-12-31'),
    # 5 公司治理
    '5.1':      ('公告日期', '2006-04-04'),
    '5.2':      ('公告日期', '2001-07-20'),
    '5.3':      ('公告日期', '1998-10-29'),
    '5.4':      ('公告日期', '1999-08-27'),
    # '5.5':      ('公告日期','2006-04-04'), # 区间统计
    # '5.6':      ('公告日期','2006-04-04'), # 区间统计
    # 6 财务报表
    # '6.1':      '个股主要指标',
    # '6.2':      '分地区财务指标',
    # '6.3':      '分行业财务指标',
    # '6.4':      '分市场财务指标',
    # 7 行业分析
    '7.1':      ('变动日期','2013-06-13'),
    # 8 评级预测
    # '8.1':      '投资评级',
    # 9 市场交易
    '9.1':      ('交易日期','2007-09-26'),
    # '9.2':      '融资融券明细',
    # 10 信息提示
    '10.1':     ('公告日期', '2008-12-31'),
    '10.2':     ('变动日期', '2008-12-31'),
    '10.3':     ('变动日期', '2008-12-31'),
    '10.4':     ('停牌时间', '2002-12-02'),
    '10.5':     ('交易日期', '2006-07-03'),
    '10.6':     (None, None),
    '10.7':     (None, None),
    '10.8':     (None, None),
    # 11 基金报表
    '11.1':     ('净值日期', '2011-12-31'),
}
