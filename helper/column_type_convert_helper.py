import functools
import math
import operator
from datetime import date, datetime, time
from types import NoneType

import pandas as pd
from dateutil.parser import parse
from pandas import Timestamp
from pandas._libs import NaTType

from enum_type.result_code import ResultCode
from enum_type.user_data_type import UserDataType
from error.convert_error import ConvertError
from helper.result_helper import execute_success


def to_number(df, column):
    try:
        if df[column].dtype == "datetime64[ns]":
            df[column] = df[column].astype("datetime64[s]")

        df[column] = pd.to_numeric(df[column])
        return df
    except Exception:
        raise ConvertError("无法将源类型转换为数值类型") from None


def parse_date(s):
    if isinstance(s, float) or isinstance(s, int):
        if math.isnan(s):
            return None
        s = str(s)
    if isinstance(s, Timestamp):
        return s
    if isinstance(s, NoneType):
        return None
    if isinstance(s, NaTType):
        return None
    if isinstance(s, str):
        if s == "nan" or s == "NaT":
            return None
    if isinstance(s, datetime):
        return s
    if isinstance(s, date):
        return s
    if isinstance(s, time):
        return s
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    try:
        parsed_date = parse(s)
    except Exception:
        raise ConvertError("无法将源类型转换为日期类型") from None
    return parsed_date


def parse_str(s):
    if isinstance(s, float):
        if math.isnan(s):
            return None
    return str(s)


def to_date(df, column):
    df[column] = df[column].apply(parse_date)
    return df


def to_varchar(df, column):
    df[column] = df[column].apply(parse_str)
    return df


convert_function_dict = {
    UserDataType.Number.value: to_number,
    UserDataType.Date.value: to_date,
    UserDataType.Varchar2.value: to_varchar,
}


def column_type_convert_from_df_to_df(df, convert_fields, cname):
    try:
        for column in df.columns:
            convert_field_dict = convert_field(convert_fields, column)
            if convert_field_dict and convert_field_dict[cname] in convert_function_dict:
                dt = convert_field_dict[cname]
                func = convert_function_dict[dt]
                df = func(df, column)
    except Exception as e:
        raise ConvertError(f"{e}") from None
    return execute_success(data=df)


def column_type_convert_from_data_to_df(data, convert_fields, cname):
    df = pd.DataFrame.from_dict(data)
    try:
        for column in df.columns:
            convert_field_dict = convert_field(convert_fields, column)
            if convert_field_dict and convert_field_dict[cname] in convert_function_dict:
                dt = convert_field_dict[cname]
                func = convert_function_dict[dt]
                df = func(df, column)
    except Exception as e:
        raise ConvertError(f"{e}") from None
    return execute_success(data=df)


def column_type_convert(data, convert_fields):
    convert_result = column_type_convert_from_data_to_df(data, convert_fields, "to_type")
    if convert_result.code != ResultCode.Success.value:
        return convert_result
    convert_data = convert_result.data.to_dict("records")
    return execute_success(data=convert_data)


def column_fields_type_convert(fields, convert_fields):
    for field in fields:
        for convert_field_item in convert_fields:
            if field["name"] == convert_field_item["name"]:
                field["data_type"] = convert_field_item["to_type"]
                break
    return fields


def convert_field(convert_fields, column):
    if isinstance(convert_fields, dict):
        convert_fields = functools.reduce(operator.concat, [v for v in convert_fields.values()])
    return next(
        (item for item in convert_fields if item["name"].lower() == column.lower()),
        None,
    )
