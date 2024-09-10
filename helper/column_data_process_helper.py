import numpy as np
import pandas as pd

from enum_type.data_process_type import DataProcessType
from error.data_process_error import DataProcessError
from helper.error_helper import translate_error_message


def boxcox(df, column):
    from scipy.stats import boxcox

    df[column], _ = boxcox(df[column])
    return df


def log(df, column):
    df[column] = np.log(df[column])
    return df


process_function_dict = {
    DataProcessType.BoxCox.value: boxcox,
    DataProcessType.NpLog.value: log,
}


def column_data_process(data, process_fields):
    df = pd.DataFrame.from_dict(data)
    for column in df.columns:
        process_type_dict = process_type(process_fields, column)
        if process_type_dict and process_type_dict["process_type"] in process_function_dict:
            func = process_function_dict[process_type_dict["process_type"]]
            try:
                df = func(df, column)
            except ValueError as e:
                raise DataProcessError(translate_error_message(str(e))) from None
    return df.to_dict("records")


def process_type(process_fields, column):
    return next((item for item in process_fields if item["name"] == column), None)
