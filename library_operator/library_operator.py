import numpy
import torch
from numpy import ndarray
from prophet.serialize import model_from_json, model_to_json

from element.algorithm_element.regression.exponential_regression_algorithm import (
    ExponentModel,
)
from element.algorithm_element.regression.logarithm_regression_algorithm import (
    LogarithmModel,
)
from element.algorithm_element.regression.weibull_algorithm import WeibullRun
from enum_type.library_type import LibraryType
from error.data_process_error import DataProcessError
from error.predict_error import PredictError
from helper.error_helper import translate_error_message
from helper.matrix_helper import dict_array_build, prediction_matrix_build
from helper.oss_helper.oss_helper import oss_helper1


def save(model, key, library_type):
    if library_type == LibraryType.Sklearn.value:
        oss_helper1.save_model_to_s3(model, key)
    elif library_type == LibraryType.Prophet.value:
        oss_helper1.save_model_to_s3(model_to_json(model), key)
    elif library_type == LibraryType.Grey.value:
        oss_helper1.save_model_to_s3(model, key)
    elif library_type == LibraryType.ExponentialParameter.value:
        oss_helper1.save_model_to_s3(model, key)
    elif library_type == LibraryType.LogarithmParameter.value:
        oss_helper1.save_model_to_s3(model, key)
    elif library_type == LibraryType.WeibullParameter.value:
        oss_helper1.save_model_to_s3(model, key)
    elif library_type == LibraryType.Pytorch.value:
        oss_helper1.save_model_to_s3(model, key)
    else:
        raise PredictError("不支持的机器学习库") from None


def load(key, library_type):
    if library_type == LibraryType.Sklearn.value:
        return oss_helper1.load_model_from_s3(key)
    elif library_type == LibraryType.Prophet.value:
        return model_from_json(oss_helper1.load_model_from_s3(key))
    elif library_type == LibraryType.Grey.value:
        return oss_helper1.load_model_from_s3(key)
    elif library_type == LibraryType.ExponentialParameter.value:
        return oss_helper1.load_model_from_s3(key)
    elif library_type == LibraryType.LogarithmParameter.value:
        return oss_helper1.load_model_from_s3(key)
    elif library_type == LibraryType.WeibullParameter.value:
        return oss_helper1.load_model_from_s3(key)
    elif library_type == LibraryType.Pytorch.value:
        return oss_helper1.load_model_from_s3(key)
    else:
        raise PredictError("不支持的机器学习库") from None


def predict(model, data, library_type, fields=None):
    if library_type == LibraryType.Sklearn.value:
        if isinstance(data[0], dict):
            matrix = prediction_matrix_build(data)
        else:
            matrix = data
        try:
            return model.predict(matrix)
        except Exception as e:
            raise PredictError(translate_error_message(str(e))) from None

    elif library_type == LibraryType.Prophet.value:
        import pandas as pd

        if not isinstance(data, ndarray):
            matrix = prediction_matrix_build(data)
        else:
            matrix = data
        matrix = pd.DataFrame(matrix, columns=["ds"])
        predict_result = model.predict(matrix)
        return predict_result["yhat"]
    elif library_type == LibraryType.Grey.value:
        if not isinstance(data, ndarray):
            matrix = prediction_matrix_build(data)
        else:
            matrix = data
        model.period = len(matrix)
        model.forecast()
        value_list = []
        for forecast in model.analyzed_results:
            if forecast.tag != model._TAG_FORECAST_HISTORY:
                value_list.append(forecast.forecast_value)
        return numpy.array(value_list)
    elif library_type == LibraryType.ExponentialParameter.value:
        if not isinstance(data, ndarray):
            matrix = prediction_matrix_build(data)
        else:
            matrix = data
        a = model["a"]
        b = model["b"]
        c = model["c"]
        md = ExponentModel()
        y_pre = md.predict_model(matrix, a=a, b=b, c=c)
        return numpy.array(y_pre)
    elif library_type == LibraryType.LogarithmParameter.value:
        if not isinstance(data, ndarray):
            matrix = prediction_matrix_build(data)
        else:
            matrix = data
        a = model["a"]
        b = model["b"]
        md = LogarithmModel()
        y_pre = md.predict_model(matrix, a=a, b=b)
        return numpy.array(y_pre)
    elif library_type == LibraryType.WeibullParameter.value:
        if not isinstance(data, ndarray):
            matrix = prediction_matrix_build(data)
        else:
            matrix = data
        # list
        m = model["m"]
        a = model["a"]
        is_multimodal = False
        number_of_peaks = 1
        # 判断是否多峰
        if len(m) > 1:
            is_multimodal = True
            number_of_peaks = len(m)

        # 获取预计总花费和预计花费时间 time 是预计花费时间(年)，total_cost 是总花费
        data = dict_array_build(matrix.tolist(), fields)
        reg = WeibullRun(data, m, a, is_multimodal=is_multimodal, number_of_peaks=number_of_peaks)
        y_pre = reg.run()
        weibull_params = {}
        if data:
            weibull_params = data[0]
        if not weibull_params or "time" not in weibull_params or "total_cost" not in weibull_params:
            raise DataProcessError(
                "输入数据有误，缺少 time 和 total_cost 字段，无法应用于威布尔模型进行预测输出"
            ) from None
        return y_pre.values
    elif library_type == LibraryType.Pytorch.value:
        if not isinstance(data, ndarray):
            matrix = prediction_matrix_build(data)
        else:
            matrix = data
        with torch.no_grad():
            y_pre = model.forward(torch.tensor(matrix, dtype=torch.float32).to(device="cpu"))

        return y_pre.squeeze().numpy()
    else:
        raise PredictError("不支持的机器学习库") from None
