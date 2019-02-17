"""

深证信数据搜索基础模块

"""
from cnswd.constants import MARKET_START
from cnswd.sql.szx import (IPO, ActualController,
                           AdditionalStockImplementation, AdditionalStockPlan,
                           Classification, CompanyShareChange, Dividend,
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
                           StockInfo, TtmCashFlowStatement, TtmIncomeStatement)
from cnswd.utils import sanitize_dates


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

default_start_date = MARKET_START.tz_localize(None).strftime(r'%Y-%m-%d')
# 第一项为数据库字段名称，第二项为更新频率，第三项为默认开始刷新日期
DATE_MAPS = {
    '1.1':   (None, None, None),
    '2.1':   ('变动日期', 'D', '2006-12-31'),
    '2.2':   ('变动日期', 'D', default_start_date),
    '2.3':   ('公告日期', 'D', '2004-11-29'),
    '2.4':   ('增_减持截止日', 'D', '2006-09-30'),
    '2.5':   ('截止日期', 'Q', '1997-06-30'),
    '3.1':   ('交易日期', 'M', default_start_date),
    '4.1':   ('发布日期', 'D', '2003-01-02'),
    '5.1':   ('报告年度', 'Q',  default_start_date),
    '6.1':   ('分红年度', 'Y', default_start_date),
    '7.1':   ('公告日期', 'D', '1996-11-29'),
    '7.2':   ('公告日期', 'D', '1996-11-29'),
    '7.3':   ('公告日期', 'D', '1993-03-13'),
    '7.4':   ('公告日期', 'D', '1993-03-13'),
    '7.5':   (None, None, None),
    '8.1.1': ('报告年度', 'Q', default_start_date),
    '8.1.2': ('报告年度', 'Q',  default_start_date),
    '8.2.1': ('报告年度', 'Q',  default_start_date),
    '8.2.2': ('报告年度', 'Q',  default_start_date),
    '8.2.3': ('报告年度', 'Q',  default_start_date),
    '8.3.1': ('报告年度', 'Q', default_start_date),
    '8.3.2': ('报告年度', 'Q', default_start_date),
    '8.3.3': ('报告年度', 'Q', default_start_date),
    '8.3.4': ('报告年度', 'Q', '2006-03-31'),
    '8.3.5': ('报告年度', 'Q', '2006-03-31'),
    '8.3.6': ('报告年度', 'Q', '2006-03-31'),
    '8.4.1': ('报告年度', 'Q', '2000-06-30'),
    '8.4.2': ('报告期', 'Q', '1991-12-31'),
}
