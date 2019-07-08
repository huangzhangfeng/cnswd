"""

专题统计模型

"""
from sqlalchemy import (BigInteger, Boolean, Column, Date, DateTime, Float,
                        ForeignKey, Integer, String, Text)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


# class Subscription(Base):
#     """ 新股申购 1.1"""
#     __tablename__ = 'subscription'
#     证券代码 = Column(Text, nullable=False, index=True)
#     证券简称 = Column(Text)
#     申购代码 = Column(Text)
#     申购日期 = Column(DateTime, nullable=False, index=True)
#     发行价 = Column(Float)
#     上网发行数量 = Column(BigInteger)  # 转换为整数


# class Issued(Base):
#     """ 新股发行 1.2"""
#     __tablename__ = 'issued'
#     证券代码 = Column(Text, nullable=False, index=True)
#     证券简称 = Column(Text)
#     上市日期 = Column(DateTime, nullable=False, index=True)
#     申购日期 = Column(DateTime, nullable=False, index=True)
#     发行价 = Column(Float)
#     总发行数量 = Column(BigInteger)  # 转换为整数
#     发行市盈率 = Column(Float)
#     上网发行中签率 = Column(Float)
#     摇号结果公告日 = Column(DateTime, nullable=False, index=True)
#     中签公告日 = Column(DateTime, nullable=False, index=True)
#     中签缴款日 = Column(DateTime, nullable=False, index=True)


# class Approved(Base):
#     """ 新股过会 1.3"""
#     __tablename__ = 'approved'
#     公司名称 = Column(Text, nullable=False, index=True)
#     上会日期 = Column(DateTime, nullable=False, index=True)
#     审核类型 = Column(Text)
#     审议内容 = Column(Text)
#     审核结果 = Column(Text)
#     审核公告日 = Column(DateTime, nullable=False, index=True)


class IPOAudit(Base):
    """首发审核 2.1"""
    __tablename__ = 'ipo_audit'
    __table_args__ = {'sqlite_autoincrement': True}

    序号 = Column(Integer, primary_key=True)
    机构名称 = Column(Text)
    公告日期 = Column(DateTime, index=True)
    截止日期 = Column(DateTime, index=True)
    注册地 = Column(Text)
    所属行业或领域 = Column(Text)
    拟上市板块 = Column(Text)
    保荐机构 = Column(Text)
    保荐代表人 = Column(Text)
    会计师事务所 = Column(Text)
    签字会计师 = Column(Text)
    律师事务所 = Column(Text)
    签字律师 = Column(Text)
    审核进度 = Column(Text)


class IpoFinancing(Base):
    """首发筹资 2.2"""
    __tablename__ = 'ipo_financing'
    __table_args__ = {'sqlite_autoincrement': True}

    序号 = Column(Integer, primary_key=True)
    证券代码 = Column(Text, index=True)
    证券简称 = Column(Text)
    行业名称 = Column(Text)
    上市日期 = Column(DateTime, index=True)
    募资总额 = Column(Float)
    承销收入 = Column(Float)
    每股净资产 = Column(Float)
    首日收盘价 = Column(Float)
    市净率 = Column(Float)
    发行价 = Column(Float)
    首日涨跌幅 = Column(Float)


class Raise(Base):
    """增发筹资 2.3"""
    __tablename__ = 'raise'
    __table_args__ = {'sqlite_autoincrement': True}

    序号 = Column(Integer, primary_key=True)
    证券代码 = Column(Text, nullable=False)
    证券简称 = Column(Text)
    发行日期 = Column(DateTime, nullable=False, index=True)
    公告日期 = Column(DateTime, nullable=False, index=True)
    总发行数量 = Column(Float)
    实际募资总额 = Column(Float)
    发行方式 = Column(Text)


class Placement(Base):
    """配股筹资 2.4"""
    __tablename__ = 'placement'
    __table_args__ = {'sqlite_autoincrement': True}

    序号 = Column(Integer, primary_key=True)
    证券代码 = Column(Text, nullable=False, index=True)
    证券简称 = Column(Text)
    配股日期 = Column(DateTime, nullable=False, index=True)
    实际配股数量 = Column(BigInteger)
    实际募资总额 = Column(Float)


class Bond(Base):
    """公司债或可转债 2.5"""
    __tablename__ = 'bond'

    证券代码 = Column(Text, primary_key=True, nullable=False)
    证券简称 = Column(Text)
    公司名称 = Column(Text)
    上市日期 = Column(DateTime, nullable=False)
    发行起始日 = Column(DateTime, primary_key=True, nullable=False)
    债券类型 = Column(Text)
    实际发行总量 = Column(Float)


class Deregulation(Base):
    """解禁报表明细 3.1"""
    __tablename__ = 'deregulation'
    __table_args__ = {'sqlite_autoincrement': True}
    序号 = Column(Integer, primary_key=True)
    证券代码 = Column(Text, index=True, nullable=False)
    证券简称 = Column(Text)
    公告日期 = Column(DateTime, index=True, nullable=False)
    实际解除限售日期 = Column(DateTime, index=True, nullable=False)
    实际解除限售数量 = Column(BigInteger)
    实际解除限售比例 = Column(Float)
    实际可流通数量 = Column(BigInteger)


class Underweight(Base):
    """按天减持明细 3.2"""
    __tablename__ = 'underweight'
    __table_args__ = {'sqlite_autoincrement': True}
    序号 = Column(Integer, primary_key=True)
    公告日期 = Column(DateTime, index=True)
    证券代码 = Column(Text, index=True)
    证券简称 = Column(Text)
    减持日期 = Column(DateTime, index=True)
    股东名称 = Column(Text)
    减持数量 = Column(BigInteger)  # 转换为整数
    减持比例 = Column(Float)
    减持价格 = Column(Float)


class Overweight(Base):
    """增持明细 3.3"""
    __tablename__ = 'overweight'
    __table_args__ = {'sqlite_autoincrement': True}
    序号 = Column(Integer, primary_key=True)
    公告日期 = Column(DateTime, index=True)
    证券代码 = Column(Text, index=True)
    证券简称 = Column(Text)
    增持日期 = Column(DateTime, index=True)
    股东名称 = Column(Text)
    增持数量 = Column(BigInteger)
    增持比例 = Column(Float)
    增持价格 = Column(Float)


class Equity(Base):
    """股本情况 3.6"""
    __tablename__ = 'equity'

    证券代码 = Column(Text, primary_key=True, nullable=False)
    证券简称 = Column(Text)
    交易市场 = Column(Text)
    公告日期 = Column(DateTime, nullable=False, index=True)
    变动日期 = Column(DateTime, index=True)
    变动原因 = Column(Text)
    总股本 = Column(BigInteger)
    已流通股份 = Column(Float)
    已流通比例 = Column(Float)
    流通受限股份 = Column(BigInteger)


# class ActualController(Base):
#     """实际控制人持股变动 3.9"""
#     __tablename__ = 'actual_controller'

#     证券代码 = Column(Text, primary_key=True, nullable=False)
#     证券简称 = Column(Text)
#     变动日期 = Column(DateTime, primary_key=True, nullable=False)
#     实际控制人名称 = Column(Text, primary_key=True, nullable=False)
#     控股数量 = Column(BigInteger)
#     控股比例 = Column(Float)
#     直接控制人名称 = Column(Text)


# class Concentration(Base):
#     """股东人数及持股集中度 3.10"""
#     __tablename__ = 'concentration'

#     证券代码 = Column(Text, primary_key=True, nullable=False)
#     证券简称 = Column(Text)
#     变动日期 = Column(DateTime, primary_key=True, nullable=False)
#     本期股东人数 = Column(BigInteger)
#     上期股东人数 = Column(BigInteger)
#     股东人数增幅 = Column(Float)
#     本期人均持股数量 = Column(BigInteger)
#     上期人均持股数量 = Column(BigInteger)
#     人均持股数量增幅 = Column(Float)


# class PerformanceForecast(Base):
#     """业绩预告 4.1"""
#     __tablename__ = 'performance_forecast'

#     公告日期 = Column(DateTime, primary_key=True, nullable=False)
#     证券代码 = Column(Text, primary_key=True, nullable=False)
#     证券简称 = Column(Text)
#     申万二级行业 = Column(Text)
#     报告年度 = Column(DateTime, primary_key=True, nullable=False)
#     业绩类型 = Column(Text)
#     业绩预告内容 = Column(Text)
#     业绩变化原因 = Column(Text)
#     净利润增长幅上限 = Column(Float)
#     净利润增长幅下限 = Column(Float)


class Reorganization(Base):
    """资产重组 5.1"""
    __tablename__ = 'reorganization'
    __table_args__ = {'sqlite_autoincrement': True}

    序号 = Column(Integer, primary_key=True)
    证券代码 = Column(Text, nullable=False)
    证券简称 = Column(Text)
    公告日期 = Column(DateTime, nullable=False)
    重组方式 = Column(Text)
    重组内容 = Column(Text)
    是否涉及主营业务变更 = Column(Text)
    变更前主营业务 = Column(Text)
    变更后主营业务 = Column(Text)
    获准日期 = Column(DateTime)
    备注 = Column(Text)


class DebtRestructuring(Base):
    """债务重组 5.2"""
    __tablename__ = 'debt_restructuring'
    __table_args__ = {'sqlite_autoincrement': True}

    序号 = Column(Integer, primary_key=True)
    证券代码 = Column(Text, nullable=False)
    证券简称 = Column(Text)
    公告日期 = Column(DateTime, nullable=False)
    债务人 = Column(Text, nullable=False)
    债权人 = Column(Text)
    重组金额 = Column(BigInteger)
    进展状态 = Column(Text)


class Merger(Base):
    """吸收合并 5.3"""
    __tablename__ = 'merger'
    __table_args__ = {'sqlite_autoincrement': True}

    序号 = Column(Integer, primary_key=True)
    证券代码 = Column(Text, nullable=False)
    证券简称 = Column(Text)
    公告日期 = Column(DateTime, nullable=False)
    被合并方 = Column(Text, nullable=False)
    资产评估基准日 = Column(DateTime)
    是否关联交易 = Column(Text)
    对公司经营的影响 = Column(Text)
    进展状态 = Column(Text)
    股权登记日 = Column(DateTime)


class ShareVariation(Base):
    """股权变更 5.4"""
    __tablename__ = 'share_variation'
    __table_args__ = {'sqlite_autoincrement': True}

    序号 = Column(Integer, primary_key=True)
    证券代码 = Column(Text, nullable=False)
    证券简称 = Column(Text)
    公告日期 = Column(DateTime, nullable=False)
    出让方名称 = Column(Text)
    受让方名称 = Column(Text)
    股权转让方式 = Column(Text)
    股份变动类型 = Column(Text)
    变动数量 = Column(Float)
    占总股本比例 = Column(Float)
    转让金额 = Column(Float)
    股权转让状态 = Column(Text)


class Pe(Base):
    """行业市盈率 7.1"""
    __tablename__ = 'pes'

    变动日期 = Column(DateTime, primary_key=True, nullable=False)
    行业分类 = Column(Text, primary_key=True, nullable=False)
    行业层级 = Column(Integer)
    行业编码 = Column(Text, primary_key=True, nullable=False)
    行业名称 = Column(Text)
    公司数量 = Column(Integer)
    纳入计算公司数量 = Column(Integer)
    总市值_静态 = Column(Float)
    净利润_静态 = Column(Float)
    静态市盈率_加权平均 = Column(Float)
    静态市盈率_中位数 = Column(Float)
    静态市盈率_算术平均 = Column(Float)


class BigDeal(Base):
    """大宗交易 9.1"""
    __tablename__ = 'big_deal'
    __table_args__ = {'sqlite_autoincrement': True}
    序号 = Column(Integer, primary_key=True)
    证券代码 = Column(Text, nullable=False, index=True)
    证券简称 = Column(Text)
    交易日期 = Column(DateTime, nullable=False, index=True)
    买方营业部 = Column(Text)
    卖方营业部 = Column(Text)
    成交价格 = Column(Float)
    成交量 = Column(BigInteger)  # 转换为整数
    成交金额 = Column(Float)


class Meeting(Base):
    """股东大会召开情况 10.1"""
    __tablename__ = 'meeting'
    __table_args__ = {'sqlite_autoincrement': True}

    序号 = Column(Integer, primary_key=True)
    证券代码 = Column(Text, nullable=False, index=True)
    证券简称 = Column(Text)
    公告日期 = Column(DateTime, nullable=False, index=True)
    股东大会名称 = Column(Text)
    会议召开日期 = Column(DateTime, index=True)
    股东大会类别 = Column(Text)
    A股股权登记日期 = Column(DateTime, index=True)
    网络投票起始日 = Column(DateTime, index=True)
    网络投票终止日 = Column(DateTime, index=True)
    会议召开地点 = Column(Text)
    是否取消 = Column(Text)
    取消公告日期 = Column(DateTime, index=True)
    参会登记日期截止日期 = Column(DateTime, index=True)
    交易系统投票日期 = Column(DateTime, index=True)


class MeetingChange(Base):
    """股东大会相关事项变动 10.2"""
    __tablename__ = 'meeting_change'
    __table_args__ = {'sqlite_autoincrement': True}

    序号 = Column(Integer, primary_key=True)
    证券代码 = Column(Text, nullable=False, index=True)
    证券简称 = Column(Text)
    变动日期 = Column(DateTime, nullable=False, index=True)
    变更事项 = Column(Text)
    会议召开日期 = Column(DateTime, index=True)
    A股股权登记日期 = Column(DateTime, index=True)
    会议名称 = Column(Text)
    变更后_中文名称 = Column(Text)
    变更前_中文名称 = Column(Text)
    变更原因 = Column(Text)


class Motion(Base):
    """股东大会议案表 10.3"""
    __tablename__ = 'motion'
    __table_args__ = {'sqlite_autoincrement': True}

    序号 = Column(Integer, primary_key=True)
    证券代码 = Column(Text, nullable=False, index=True)
    证券简称 = Column(Text)
    会议召开日期 = Column(DateTime, index=True)
    A股股权登记日期 = Column(DateTime, index=True)
    会议名称 = Column(Text)
    变动日期 = Column(DateTime, nullable=False, index=True)
    议案序号 = Column(Text)
    议案内容 = Column(Text)
    议案类型 = Column(Text)
    议案状态 = Column(Text)


class SuspensionAndResumption(Base):
    """股东大会议案表 10.4"""
    __tablename__ = 'suspension_and_resumption'
    __table_args__ = {'sqlite_autoincrement': True}

    序号 = Column(Integer, primary_key=True)
    证券代码 = Column(Text, nullable=False, index=True)
    证券简称 = Column(Text)
    证券类别 = Column(Text)
    停牌时间 = Column(DateTime, nullable=False, index=True)
    复牌时间 = Column(DateTime, nullable=False, index=True)
    停牌期限 = Column(Text)


class MarketPublicInformation(Base):
    """市场公开信息汇总 10.5"""
    __tablename__ = 'market_public_information'
    __table_args__ = {'sqlite_autoincrement': True}

    序号 = Column(Integer, primary_key=True)
    证券代码 = Column(Text, nullable=False, index=True)
    证券简称 = Column(Text)
    交易日期 = Column(DateTime, nullable=False, index=True)
    异动类型 = Column(Text)
    异动上榜前五营业部买入净额合计 = Column(Float)
    异动上榜前五营业部卖出金额合计 = Column(Float)


class Proposed(Base):
    """拟上市公司清单 10.6"""
    __tablename__ = 'proposed'
    __table_args__ = {'sqlite_autoincrement': True}

    序号 = Column(Integer, primary_key=True)
    公司名称 = Column(Text, nullable=False)
    证券发行日期 = Column(DateTime, nullable=False)
    证券上市日期 = Column(DateTime)
    上市市场 = Column(Text)


class Suspend(Base):
    """暂停上市公司清单 10.7"""
    __tablename__ = 'suspend'
    __table_args__ = {'sqlite_autoincrement': True}

    序号 = Column(Integer, primary_key=True)
    上市代码 = Column(Text, nullable=False)
    上市简称 = Column(Text)
    上市日期 = Column(DateTime, nullable=False)
    暂停上市日期 = Column(DateTime)


class Delisted(Base):
    """终止上市公司清单 10.8"""
    __tablename__ = 'delisted'
    __table_args__ = {'sqlite_autoincrement': True}

    序号 = Column(Integer, primary_key=True)
    上市代码 = Column(Text, nullable=False)
    上市简称 = Column(Text)
    终止上市日期 = Column(DateTime, nullable=False)
    转板日期 = Column(DateTime)
    转板代码 = Column(Text)
    转板简称 = Column(Text)


class Rnav(Base):
    """基金净值增长率 11.1"""
    __tablename__ = 'rnav'

    基金代码 = Column(Text, primary_key=True, nullable=False)
    基金简称 = Column(Text)
    基金类型 = Column(Text)
    净值日期 = Column(DateTime, primary_key=True, nullable=False)
    单位净值 = Column(Float)
    累计净值 = Column(Float)
    日增长率 = Column(Float)
    近7天回报 = Column(Float)
    近30天回报 = Column(Float)
    近90天回报 = Column(Float)
    近180天回报 = Column(Float)
    今年以来回报 = Column(Float)
