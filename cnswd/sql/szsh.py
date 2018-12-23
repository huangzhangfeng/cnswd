"""
深交所、上交所数据模型
"""
from sqlalchemy import (BigInteger, Boolean, Column, Date, DateTime, Enum,
                        Float, ForeignKey, Integer, SmallInteger, String, Text,
                        Time, func)
from sqlalchemy.ext.declarative import declarative_base
from .common import CommonMixin

# 增强基本类
Base = declarative_base(cls=CommonMixin)


class Stock(Base):
    """股票基本资料"""
    公司代码 = Column(String(6), primary_key=True, index=True)
    公司简称 = Column(String(10))
    A股代码 = Column(String(6))
    A股简称 = Column(String(10))
    A股上市日期 = Column(DateTime)
    A股总股本 = Column(Integer)
    A股流通股本 = Column(Integer)
    B股代码 = Column(String(6))
    B股简称 = Column(String(10))
    B股上市日期 = Column(DateTime)
    B股总股本 = Column(Integer)
    B股流通股本 = Column(Integer)

    def __repr__(self):
        return "<Stock(公司代码='%s')>" % self.公司代码

    @property
    def exchange(self):
        first_letter = self.公司代码[0]
        if first_letter in ('6', ):
            return '上交所'
        elif first_letter in ('0', '3'):
            return '深交所'

    @property
    def plate(self):
        if self.公司代码[:3] == '002':
            return '中小板'
        elif self.公司代码[0] in ('3', ):
            return '创业板'
        else:
            return '主板A'


class Delisting(Base):
    """股票基本资料"""
    证券代码 = Column(String(6), primary_key=True, index=True)
    证券简称 = Column(String(10))
    上市日期 = Column(DateTime)
    终止上市日期 = Column(DateTime)


class Suspend(Base):
    """股票基本资料"""
    证券代码 = Column(String(6), primary_key=True, index=True)
    证券简称 = Column(String(10))
    上市日期 = Column(DateTime)
    暂停上市日期 = Column(DateTime)


class CJMX(Base):
    """成交明细"""
    __table_args__ = {'sqlite_autoincrement': True}
    序号 = Column(Integer, primary_key=True)
    股票代码 = Column(String(6), index=True)
    成交时间 = Column(DateTime, index=True)
    成交价 = Column(Float)
    价格变动 = Column(Float)
    成交量 = Column(BigInteger)
    成交额 = Column(Float)
    性质 = Column(Text)


class TradingCalendar(Base):
    """交易日期"""
    日期 = Column(DateTime, unique=True, primary_key=True, index=True)
    交易日 = Column(Boolean)

    def __repr__(self):
        return "<TradingCalendar(date='%s')>" % self.日期


class StockDaily(Base):
    """股票日线数据"""
    股票代码 = Column(String(6), primary_key=True, index=True)
    日期 = Column(DateTime, primary_key=True, index=True)
    名称 = Column(String, nullable=False)
    开盘价 = Column(Float)
    最高价 = Column(Float)
    最低价 = Column(Float)
    收盘价 = Column(Float)
    成交量 = Column(Integer)
    成交金额 = Column(Float)
    换手率 = Column(Float)
    前收盘 = Column(Float)
    涨跌额 = Column(Float)
    涨跌幅 = Column(Float)
    总市值 = Column(Float)
    流通市值 = Column(Float)
    成交笔数 = Column(Float)


class LiveQuote(Base):
    """股票报价（五档）"""
    __table_args__ = {'sqlite_autoincrement': True}
    序号 = Column(Integer, primary_key=True, autoincrement=True)
    股票代码 = Column(String(6), index=True)
    时间 = Column(DateTime, index=True)
    股票简称 = Column(String)
    开盘 = Column(Float)
    前收盘 = Column(Float)
    现价 = Column(Float)
    最高 = Column(Float)
    最低 = Column(Float)
    竞买价 = Column(Float)
    竞卖价 = Column(Float)
    成交量 = Column(Integer)  # 累计
    成交额 = Column(Float)    # 累计
    买1量 = Column(Integer, name='买1量')
    买1价 = Column(Float, name='买1价')
    买2量 = Column(Integer, name='买2量')
    买2价 = Column(Float, name='买2价')
    买3量 = Column(Integer, name='买3量')
    买3价 = Column(Float, name='买3价')
    买4量 = Column(Integer, name='买4量')
    买4价 = Column(Float, name='买4价')
    买5量 = Column(Integer, name='买5量')
    买5价 = Column(Float, name='买5价')
    卖1量 = Column(Integer, name='卖1量')
    卖1价 = Column(Float, name='卖1价')
    卖2量 = Column(Integer, name='卖2量')
    卖2价 = Column(Float, name='卖2价')
    卖3量 = Column(Integer, name='卖3量')
    卖3价 = Column(Float, name='卖3价')
    卖4量 = Column(Integer, name='卖4量')
    卖4价 = Column(Float, name='卖4价')
    卖5量 = Column(Integer, name='卖5量')
    卖5价 = Column(Float, name='卖5价')


class THSGN(Base):
    """同花顺概念成分表"""
    概念 = Column(String, primary_key=True, index=True)
    概念编码 = Column(String(6))
    页码 = Column(Integer)
    股票代码 = Column(String(6), primary_key=True, index=True)


class TCTGN(Base):
    """腾讯股票概念成分表"""
    概念id = Column(String, primary_key=True, index=True)
    概念简称 = Column(String(10))
    股票代码 = Column(String(6), primary_key=True, index=True)


class Treasury(Base):
    """国债资金成本"""
    date = Column(Date, primary_key=True)
    m0 = Column(Float)
    m1 = Column(Float)
    m2 = Column(Float)
    m3 = Column(Float)
    m6 = Column(Float)
    m9 = Column(Float)
    y1 = Column(Float)
    y3 = Column(Float)
    y5 = Column(Float)
    y7 = Column(Float)
    y10 = Column(Float)
    y15 = Column(Float)
    y20 = Column(Float)
    y30 = Column(Float)
    y40 = Column(Float)
    y50 = Column(Float)


class IndexDaily(Base):
    """指数日线交易数据"""
    指数代码 = Column(String, primary_key=True, index=True)
    日期 = Column(Date, primary_key=True, index=True)
    开盘价 = Column(Float)
    最高价 = Column(Float)
    最低价 = Column(Float)
    收盘价 = Column(Float)
    成交量 = Column(Float)
    成交额 = Column(Float)
    涨跌幅 = Column(Float)
