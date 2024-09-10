from datetime import datetime

from enum_type.compare_type import CompareType
from enum_type.filter_type import FilterType
from enum_type.user_data_type import UserDataType
from error.data_process_error import DataProcessError


def date_convert(value):
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except Exception:
        raise DataProcessError("日期格式转换失败") from None


def number_convert(value):
    try:
        if not value:
            return 0
        return float(value)
    except Exception:
        raise DataProcessError("数字格式转换失败") from None


convert_dict = {
    UserDataType.Number.value: number_convert,
    UserDataType.Date.value: date_convert,
}


def convert_value(value, data_type):
    convert_func = convert_dict.get(data_type)
    if convert_func:
        return convert_func(value)
    return value


def condition(x, filter_type, filter_compare_fields):
    """
    过滤条件封装
    :param x: 字典元素
    :param filter_type: 过滤条件
    :param filter_compare_fields: 需要过滤的字段字典数组
    :return: true/false
    """
    condition_results = []
    for y in filter_compare_fields:
        name = y["name"]
        date_type = y["data_type"]
        v = x.get(name, x.get(name.lower(), None))
        compare_type = y["compare_type"]
        if compare_type == CompareType.IsNull:
            condition_results.append(v is None)
        elif compare_type == CompareType.IsNotNull:
            condition_results.append(v is not None)
        elif v is None:
            condition_results.append(False)
        else:
            value = y["value"]
            value = convert_value(value, date_type)
            if compare_type == CompareType.GreatThan:
                condition_results.append(v > value)
            elif compare_type == CompareType.GreatThanOrEqual:
                condition_results.append(v >= value)
            elif compare_type == CompareType.Equal:
                condition_results.append(v == value)
            elif compare_type == CompareType.LessThan:
                condition_results.append(v < value)
            elif compare_type == CompareType.LessThanOrEqual:
                condition_results.append(v <= value)
            elif compare_type == CompareType.NotEqual:
                condition_results.append(v != value)

    return all(condition_results) if filter_type == FilterType.All else any(condition_results)


def data_filter(filter_type, data, filter_compare_fields):
    """
    过滤后的数组
    :param filter_type: 过滤类型
    :param data: 数据
    :param filter_compare_fields: 过滤字段字典数组
    :return: 过滤后的数据数组
    """
    if not filter_compare_fields:
        return data
    return [x for x in data if condition(x, filter_type, filter_compare_fields)]
