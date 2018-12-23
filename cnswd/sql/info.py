"""
财经消息
公司公告
"""
import datetime
from sqlalchemy import (BigInteger, Boolean, Column, Date, DateTime, Enum,
                        Float, ForeignKey, Integer, SmallInteger, String, Text,
                        Time, func)
from sqlalchemy.orm import relationship

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class EconomicNews(Base):
    """财经新闻"""
    __tablename__ = 'economic_newses'
    序号 = Column(Integer, primary_key=True, index=True)
    时间 = Column(DateTime, index=True)
    分类 = Column(String(10))
    概要 = Column(Text, nullable=False)


class Disclosure(Base):
    """公司公告"""
    __tablename__ = 'disclosures'
    序号 = Column(Integer, primary_key=True)
    股票代码 = Column(String(6), index=True)
    股票简称 = Column(Text)
    公告标题 = Column(Text, nullable=False)
    公告时间 = Column(DateTime, index=True)
    下载网址 = Column(Text)
