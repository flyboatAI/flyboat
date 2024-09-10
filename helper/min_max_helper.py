import pandas as pd

from error.data_process_error import DataProcessError
from helper.column_data_process_helper import process_type


def scaler_filter(scaler, filter_fields):
    """
    过滤列后的归一化字典
    :param scaler: 归一化数据
    :param filter_fields: 过滤字段字典数组
    :return: 过滤后的归一化字典
    """
    column_fields_name_arr = [x.get("name") for x in filter_fields]
    min_max_dict = {}
    for r in scaler.keys():
        name = r
        if name in column_fields_name_arr:
            min_max_dict[name] = scaler[name]
    return min_max_dict


def min_max_filter_column_data(data, min_max_fields):
    """
    过滤列后的数组
    :param data: 数据数组
    :param min_max_fields: 归一字段字典数组
    :return: 归一后的数据数组
    """
    if min_max_fields is None or data is None:
        return []
    min_max_fields_name_arr = [x["name"] for x in min_max_fields]
    n_arr = []
    for d in data:
        n_dict = {}
        for key in d.keys():
            if key in min_max_fields_name_arr:
                n_dict[key] = d[key]
        n_arr.append(n_dict)

    return n_arr


def min_max_scaler(data, min_max_fields):
    scaler = {}
    df = pd.DataFrame.from_dict(data)
    normalized_df = df.copy(deep=True)
    for column in df.columns:
        manual_val = process_type(min_max_fields, column)
        if manual_val and manual_val.get("manual"):
            max_val = manual_val.get("max_val")
            min_val = manual_val.get("min_val")
            if max_val is None or min_val is None:
                raise DataProcessError("手动输入的最大值和最小值未配置完成, 请配置之后重新使用") from None
        else:
            max_val = df[column].max()
            min_val = df[column].min()
        scaler[column] = {"min": min_val, "max": max_val}
        if min_val == max_val:
            continue
        normalized_df[column] = round((df[column] - min_val) / (max_val - min_val), 10)
    return normalized_df.to_dict("records"), scaler


def inverse_min_max_scaler(data, scaler):
    original_df = pd.DataFrame()
    columns = scaler.keys()
    original_columns = data[0].keys()
    df = pd.DataFrame.from_dict(data)
    df.columns = columns
    for column in df.columns:
        max_val = scaler[column]["max"]
        min_val = scaler[column]["min"]
        if min_val == max_val:
            original_df[column] = df[column]
        else:
            original_df[column] = (df[column] * (max_val - min_val)) + min_val
    original_df.columns = original_columns
    return original_df.to_dict("records")


def concat_min_max_data(min_max_data, data):
    if not min_max_data:
        return data
    if len(min_max_data) != len(data):
        return None
    for i in range(len(data)):
        if isinstance(data[i], dict) and isinstance(min_max_data[i], dict):
            data[i].update(min_max_data[i])
    return data
