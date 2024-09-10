import numpy as np
import pandas as pd
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from element.data_analyze_element.critic_analyze import Critic
from element.data_analyze_element.critic_topsis_analyze import CriticTopsis
from element.data_analyze_element.factor_analyze import FactorAnalysis
from element.data_analyze_element.grey_relation_analyze import GreyRelationAnalysis
from element.data_analyze_element.pca_analyze import PCAAnalysis
from enum_type.user_data_type import UserDataType
from helper.http_parameter_helper import get_request_body
from helper.polynomial_processing_helper import data_conversion
from helper.response_result_helper import (
    make_json,
    response_error_result,
    response_result,
)

# 参数化系统 Controller
router = APIRouter()


def convert_deduction_data(json_):
    c_data = [j["value"] for j in json_]
    c_data_2 = list(map(list, zip(*c_data)))
    df = pd.DataFrame(c_data_2)
    c_col = [j["kpiName"] for j in json_]
    df.columns = c_col
    d = df.to_dict("records")
    return d


def convert_deduction_field(json_):
    c_col = [j["kpiName"] for j in json_]
    x = []
    for c in c_col:
        d = {"name": c, "nick_name": c, "data_type": UserDataType.Varchar2.value}
        x.append(d)
    return x


def convert_deduction_result(json_, param):
    for j in json_:
        for x in param:
            if j["kpiName"] == x["特征"]:
                if np.isinf(x["权重(%)"]):
                    j["value"] = 0
                else:
                    j["value"] = x["权重(%)"]
                break
    return json_


def convert_deduction_weight(json_):
    return [j["weight"] for j in json_]


@router.post("/pca_analyze")
async def pca_analyze(request: Request):
    """
    PCA 计算
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    if not isinstance(body, list):
        return make_json(response_error_result(message="参数传递格式错误，请检查"))
    data = convert_deduction_data(body)
    fields = convert_deduction_field(body)
    _, df = data_conversion(fields, data)
    md = PCAAnalysis(data=df)
    liner_table = md.liner_coefficient_table()
    result = convert_deduction_result(body, liner_table[0])
    return JSONResponse(make_json(response_result(data=result)))


@router.post("/factor_analyze")
async def factor_analyze(request: Request):
    """
    因子分析
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    if not isinstance(body, list):
        return make_json(response_error_result(message="参数传递格式错误，请检查"))
    data = convert_deduction_data(body)
    fields = convert_deduction_field(body)
    _, df = data_conversion(fields, data)
    md = FactorAnalysis(data=df)
    liner_table = md.liner_coefficient_table()
    result = convert_deduction_result(body, liner_table[0])
    return JSONResponse(make_json(response_result(data=result)))


@router.post("/critic_analyze")
async def critic_analyze(request: Request):
    """
    critic 分析
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    if not isinstance(body, list):
        return make_json(response_error_result(message="参数传递格式错误，请检查"))
    data = convert_deduction_data(body)
    fields = convert_deduction_field(body)
    _, df = data_conversion(fields, data)
    md = Critic(data=df)
    critic_weight_matrix = md.critic_matrix()
    result = convert_deduction_result(body, critic_weight_matrix[0])
    return JSONResponse(make_json(response_result(data=result)))


@router.post("/critic_topsis_analyze")
async def critic_topsis_analyze(request: Request):
    """
    critic_topsis 分析
    :return: response_result
    """
    # noinspection PyBroadException
    body = get_request_body(request)
    if not isinstance(body, list):
        return make_json(response_error_result(message="参数传递格式错误，请检查"))
    data = convert_deduction_data(body)
    fields = convert_deduction_field(body)
    _, df = data_conversion(fields, data)
    md = CriticTopsis(data=df)
    _, best_result = md.calc_topsis_matrix()
    # topsis_ideal_solutions = md.calc_topsis_ideal_solutions()

    # result_json = {
    #     "topsis_matrix": topsis_matrix[0],
    #     "best_result": best_result[0],
    #     "topsis_ideal_solutions": topsis_ideal_solutions[0]
    # }
    result = {"result": best_result[0][0]["相对接近度C"]}
    return JSONResponse(make_json(response_result(data=result)))


@router.post("/grey_relation_analyze")
async def grey_relation_analyze(request: Request):
    """
    灰色关联分析
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    if not isinstance(body, list):
        return make_json(response_error_result(message="参数传递格式错误，请检查"))
    data = convert_deduction_data(body)
    fields = convert_deduction_field(body)
    _, df = data_conversion(fields, data)
    weight = convert_deduction_weight(body)
    md = GreyRelationAnalysis(data=df, rho=0.5, weight=weight)
    _, score_matrix = md.calc_rank()
    result = {"result": score_matrix[0][0]["score"]}
    return JSONResponse(make_json(response_result(data=result)))
