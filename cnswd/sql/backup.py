"""
实时报价、成交明细数据量巨大，数据库只保留最近30天的数据，历史数据存储在备份数据库
"""

from sqlalchemy import (BigInteger, Boolean, Column, Date, DateTime, Enum,
                        Float, ForeignKey, Integer, SmallInteger, String, Text,
                        Time, func)
from sqlalchemy.ext.declarative import declarative_base

from .common import CommonMixin

# 增强基本类
Base = declarative_base(cls=CommonMixin)


class CJMX(Base):
    """成交明细"""
    __table_args__ = {'sqlite_autoincrement': True}
    序号 = Column(Integer, primary_key=True, autoincrement=True)
    页码 = Column(Integer)
    股票代码 = Column(String(6), index=True)
    成交时间 = Column(DateTime, index=True)
    成交价 = Column(Float)
    价格变动 = Column(Float)
    成交量 = Column(BigInteger)
    成交额 = Column(Float)
    性质 = Column(Text)


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
