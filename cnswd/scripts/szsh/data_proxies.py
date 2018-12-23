from cnswd.data_proxy import DataProxy
from .date_utils import get_non_trading_days, get_trading_dates, is_trading_day

# 交易日期
is_trading_reader = DataProxy(is_trading_day, time_str='9:24:00')
non_trading_days_reader = DataProxy(get_non_trading_days, time_str='9:24:00')
trading_days_reader = DataProxy(get_trading_dates, time_str='9:24:00')
