import os
import time
import sys
import numpy as np
import pandas as pd

from cnswd.websource.exceptions import RetryException
from cnswd.utils import data_root, loop_period_by
from cnswd.websource.cninfo.constants import DB_NAME, DB_DATE_FREQ, TS_NAME, TS_DATE_FREQ
from cnswd.websource.cninfo.data_browse import DataBrowse
from cnswd.websource.cninfo.thematic_statistics import ThematicStatistics
from cnswd.sql.base import get_engine, get_session

from .base import DB_DATE_FIELD, DB_MODEL_MAPS, TS_DATE_FIELD, TS_MODEL_MAPS
from .units import fixed_data

record_path = os.path.join(data_root('record'), 'cninfo.csv')


def get_record(index):
    try:
        return pd.read_csv(record_path, index_col=0).loc[index].to_dict()
    except FileNotFoundError:
        df = pd.DataFrame({'完成状态': '未执行',
                           '尝试次数': 0,
                           '完成时间': pd.Timestamp('now'),
                           '备注': ''},
                          index=[index])
        df.to_csv(record_path)
        return pd.read_csv(record_path, index_col=0).loc[index].to_dict()
    except KeyError:
        return {'完成状态': '未执行', '尝试次数': 0, '完成时间': pd.Timestamp('now'), '备注': ''}


def update_record(index, status):
    """更新刷新状态，保持至本地文件"""
    df = pd.read_csv(record_path, index_col=0)
    df.loc[index] = pd.Series(status)
    df.to_csv(record_path)


class Refresher(object):
    date_freq = {}
    date_field = {}
    level_model = {}
    level_name = {}
    api_class = None

    def __init__(self, end_date=None, retry_times=20):
        if end_date is None:
            self.end_date = pd.Timestamp('now').normalize()
        else:
            self.end_date = pd.Timestamp(end_date).normalize()
        self.retry_times = retry_times

    @property
    def db_name(self):
        """数据库名称"""
        # `api class`名称首字符小写，其他不变
        name = self.api_class.__name__
        return f"{name[0].lower()}{name[1:]}"

    def get_level_class(self, level):
        """获取项目所对应的类"""
        return self.level_model[level]

    def get_freq(self, level):
        """获取项目刷新周期"""
        freq_str = self.date_freq[level][0]
        if freq_str is None:
            return None
        else:
            return freq_str[0]

    def get_date_field(self, level):
        """获取项目日期字段名称"""
        return self.date_field[level][0]

    def get_default_start_date(self, level):
        """获取项目默认开始刷新日期"""
        return self.date_field[level][1]

    def _compute_start(self, end_dates, level):
        """计算项目开始日期"""
        freq = self.get_freq(level)
        assert freq in ('Y', 'Q', 'M', 'W', 'D', 'B')
        if freq is None:
            return None
        # B -> D
        if freq == 'B':
            freq = 'D'
        today = pd.Timestamp('today')
        if len(end_dates) == 1:
            e_1, e_2 = pd.Timestamp(
                end_dates[0][0]), pd.Timestamp(end_dates[0][0])
        else:
            e_1, e_2 = pd.Timestamp(
                end_dates[0][0]), pd.Timestamp(end_dates[1][0])
        t_delta_2 = pd.Timedelta(
            2, unit=freq) if freq != 'Q' else pd.Timedelta(2*3, unit='M')
        t_delta_1 = pd.Timedelta(
            1, unit=freq) if freq != 'Q' else pd.Timedelta(1*3, unit='M')
        # 如大于二个周期，表明提取的网络数据为历史数据，不可能再更新
        # 此时只需在最后一日后加1天，即为开始日期
        if today - e_2 > t_delta_2:
            return pd.Timestamp(e_1) + pd.Timedelta(days=1)
        else:
            # 超出一个周期，使用最后日期
            if today - e_2 > t_delta_1:
                start = e_1
            else:
                start = e_2
        return start.normalize()

    def get_start_date(self, level):
        """刷新项目数据的开始日期"""
        session = get_session(self.db_name)
        class_ = self.get_level_class(level)
        date_field = self.get_date_field(level)
        default_date = self.get_default_start_date(level)
        expr = getattr(class_, date_field)
        # 降序排列
        # 指定表的最后二项日期(唯一)
        end_dates = session.query(expr).order_by(expr.desc()).distinct()[:2]
        # end_dates = session.query(expr.desc()).distinct()[:2]
        session.close()
        # 为空返回默认值
        if not end_dates:
            start_date = pd.Timestamp(default_date).normalize()
        else:
            start_date = self._compute_start(end_dates, level)
        return start_date

    def _delete(self, api, level, start):
        """删除项目开始日期之后的所有数据"""
        # 删除最近的日线数据
        # 融资融券数据导致不一致，需要清理旧数据
        class_ = self.get_level_class(level)
        table_name = class_.__tablename__
        expr = getattr(class_, self.get_date_field(level))
        session = get_session(self.db_name)
        num = session.query(class_).filter(
            expr >= start).delete(False)
        st = start.strftime(r'%Y-%m-%d')
        msg = f"删除 {self.db_name} {table_name} {st} 开始 {num}行"
        api.logger.notice(msg)
        session.commit()
        session.close()

    def _to_sql(self, api, level, df, if_exists):
        class_ = self.get_level_class(level)
        table_name = class_.__tablename__
        engine = get_engine(self.db_name)
        df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
        action = '添加' if if_exists == 'append' else '更新'
        api.logger.notice(f"{action} {self.db_name} {table_name} {len(df)} 行")

    def _add(self, api, level, df):
        df = fixed_data(df, level, self.db_name)
        self._to_sql(api, level, df, 'append')

    def _replace(self, api, level, df):
        df = fixed_data(df, level, self.db_name)
        self._to_sql(api, level, df, 'replace')

    def _loop_by_level(self, api, level, freq):
        if freq is None:
            # 一次执行，无需循环
            df = api.get_data(level)
            if not df.empty:
                self._replace(api, level, df)
        else:
            start = self.get_start_date(level)
            default_date = self.get_default_start_date(level)
            default_date = pd.Timestamp(default_date)
            start_date = start if start > default_date else default_date
            # 在开始之前，删除可能重复的数据
            if start_date is not None:
                self._delete(api, level, start_date)
            # 按freq循环
            ps = loop_period_by(start_date, self.end_date, freq, False)
            for s, e in ps:
                df = api.get_data(level, s, e)
                if not df.empty:
                    self._add(api, level, df)

    def get_status_dict(self, level):
        index = f"{self.api_class.__name__}{level}"
        return get_record(index)

    def was_completed(self, level):
        """判定项目是否完成刷新"""
        d = self.get_status_dict(level)
        now = pd.Timestamp('now')
        is_available = now - pd.Timestamp(d['完成时间']) < pd.Timedelta(hours=12)
        return d['完成状态'] == '完成' and is_available

    def __call__(self):
        with self.api_class(True) as api:
            for level in self.level_name.keys():
                freq = self.get_freq(level)
                index = f"{self.api_class.__name__}{level}"
                status = self.get_status_dict(level)
                for i in range(self.retry_times):
                    # 如果已经完成，则返回
                    if self.was_completed(level):
                        api.logger.notice(f'{level} 已经完成')
                        break
                    api.logger.info(f"{level:{12}} 第{i+1:{2}}次尝试")
                    try:
                        self._loop_by_level(api, level, freq)
                        status['完成状态'] = '完成'
                        status['备注'] = ''
                    except Exception as e:
                        if not api.is_available:
                            sys.exit(0)
                        status['完成状态'] = '异常'
                        status['备注'] = f"{e}"
                        time.sleep(np.random.random())
                        api.logger.error(e)
                    finally:
                        status['尝试次数'] = i+1
                        status['完成时间'] = pd.Timestamp('now')
                        update_record(index, status)
        self._report(self.level_name.keys())

    def _report(self, levels):
        """报告执行状态"""
        for level in levels:
            status = self.get_status_dict(level)
            msg = f"{self.api_class.__name__} {level} 执行结果：\n"
            for k, v in status.items():
                msg += f">>{k:{20}} {v} \n"
            print(msg)


class DBRefresher(Refresher):
    """深证信数据搜索栏目刷新工具"""
    date_freq = DB_DATE_FREQ
    date_field = DB_DATE_FIELD
    level_model = DB_MODEL_MAPS
    level_name = DB_NAME
    api_class = DataBrowse


class TSRefresher(Refresher):
    """深证信数据专题统计刷新工具"""
    date_freq = TS_DATE_FREQ
    date_field = TS_DATE_FIELD
    level_model = TS_MODEL_MAPS
    level_name = TS_NAME
    api_class = ThematicStatistics
