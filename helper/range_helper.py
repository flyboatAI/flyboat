import datetime

import numpy as np
from dateutil.relativedelta import relativedelta

from enum_type.input_type import ValueType


def build_int_range_list(start_str, stop_str, step_str, key):
    """
    根据 start stop step 生成 int 区间数组
    :param start_str: 起始值, 如 "5"
    :param stop_str: 终止值, 如 "10"
    :param step_str: 步长, 默认 "1"
    :param key: Key
    :return: int 区间 JSON 序列 如 [{"x": 5}, {"x": 6}, {"x": 7}, {"x": 8}, {"x": 9}, {"x": 10}]
    """
    if start_str is None or stop_str is None or step_str is None:
        return []

    if not isinstance(start_str, int) or not isinstance(stop_str, int) or not isinstance(step_str, int):
        return []
    start = int(start_str)
    stop = int(stop_str)
    step = int(step_str)
    stop = stop + 1
    if start > stop or not step:
        return []
    x = np.arange(start, stop, step)
    return [{key: int_value} for int_value in list(x)]


def build_date_range_list(start_str, stop_str, date_type, key):
    """
    根据 start stop 生成日期区间数组
    :param start_str: 起始年份/年月/年月日, 如 2010 或 2000/01 或 2000/01/01
    :param stop_str: 终止年份/年月/年月日, 如 2020 或 2010/09 或 2000/04/01
    :param date_type: 日期类型
    :param key: Key
    :return: 年序列/年月序列/年月日序列
    """
    dates = []
    if not start_str or not stop_str:
        return dates
    start_str = str(start_str)
    stop_str = str(stop_str)
    date = start_str[:]
    try:
        if date_type == ValueType.YearRange.value:
            dt = datetime.datetime.strptime(start_str, "%Y")
        elif date_type == ValueType.MonthRange.value:
            dt = datetime.datetime.strptime(start_str, "%Y-%m")
        else:
            dt = datetime.datetime.strptime(start_str, "%Y-%m-%d")
        while date <= stop_str:
            dates.append(date)

            if date_type == ValueType.YearRange.value:
                dt = dt + relativedelta(years=1)
                date = dt.strftime("%Y")
            elif date_type == ValueType.MonthRange.value:
                dt = dt + relativedelta(months=1)
                date = dt.strftime("%Y-%m")
            else:
                dt = dt + relativedelta(days=1)
                date = dt.strftime("%Y-%m-%d")
        return [{key: d} for d in dates]
    except ValueError:
        return dates


def build_single_value(single_value, key):
    """
    根据 key 生成单值
    :param single_value: 单值, 如 5
    :param key: Key
    :return: 单值 如 [{"x": 5}]
    """
    if single_value is None:
        return []
    return [{key: single_value}]
