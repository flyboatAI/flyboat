import sys

import numpy as np
from asyncer import asyncify
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sklearn.metrics import cohen_kappa_score

from element_configuration.algorithm_element.custom import (
    custom_algorithm_file_configuration,
)
from element_configuration.algorithm_element.regression import (
    linear_regression_algorithm_configuration,
    pls_regression_algorithm_configuration,
    ridge_regression_algorithm_configuration,
    svr_regression_algorithm_configuration,
)
from element_configuration.data_preprocessing_element import (
    column_data_process_configuration,
    data_split_configuration,
    min_max_scaler_configuration,
    role_setting_configuration,
)
from element_configuration.evaluate_element import regression_evaluate_configuration
from element_configuration.formula_element import (
    analogy_estimation_algorithm_configuration,
)
from element_configuration.input_output_element import (
    data_model_configuration,
    sync_input_configuration,
)
from element_configuration.model_operation_element import model_apply_configuration
from enum_type.data_model_type import DataModelType
from enum_type.input_type import ValueType
from enum_type.penalty import Penalty
from enum_type.result_code import ResultCode
from enum_type.split_type import SplitType
from error.empty_parameter_value_error import EmptyParameterValueError
from error.execute_error import ExecuteError
from error.query_error import QueryError
from error.store_error import StoreError
from helper.http_parameter_helper import (
    get_request_body,
    get_version_id_and_element_id_from_body,
)
from helper.parameterization_helper import get_element_id, get_prev_node
from helper.pipelining_helper import (
    copy_pipelining_version,
    get_input_params,
    get_output_params,
    get_sync_element_list,
    multiprocess_pipelining_exec,
    publish_pipelining,
)
from helper.response_result_helper import (
    make_json,
    response_error_result,
    response_result,
)
from helper.sample_data_generate_helper import generate_input_data
from publish.pipelining_publish import get_version

# 参数化系统 Controller
router = APIRouter()


@router.post("/element_id")
async def get_element_id_by_version_id_and_node_type(request: Request):
    """
    获取算子标识
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    version_id, _ = get_version_id_and_element_id_from_body(body)
    node_type = body["node_type"]
    element_id = get_element_id(version_id, node_type)
    return JSONResponse(
        make_json(response_result(data={"element_id": element_id}) if element_id else response_error_result())
    )


@router.post("/role_setting")
async def role_setting(request: Request):
    # noinspection PyBroadException
    """
    获取角色设置的配置
    :return:
    """
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port, prev_node_type, prev_element_id = get_prev_node(version_id, element_id)
    if prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "上级节点查询失败") from None
    query_result = role_setting_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/config_role_setting")
async def config_role_setting(request: Request):
    """
    配置角色设置算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    role_setting_fields = body["role_setting_fields"]
    prev_node_output_port, prev_node_type, prev_element_id = get_prev_node(version_id, element_id)

    if not role_setting_fields:
        raise EmptyParameterValueError(["role_setting_fields"]) from None
    if prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "上级节点查询失败") from None

    store_result = role_setting_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        role_setting_fields,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/data_split")
async def data_split(request: Request):
    # noinspection PyBroadException
    """
    获取角色设置的配置
    :return:
    """
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = data_split_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/config_data_split")
async def config_data_split(request: Request):
    """
    配置角色设置算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    split_type = SplitType.TrainTest.value
    random_seed = 123456
    train_percent = 80
    test_percent = 20
    valid_percent = 0
    prev_node_output_port, prev_node_type, prev_element_id = get_prev_node(version_id, element_id)
    if prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "上级节点查询失败") from None
    store_result = data_split_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        split_type,
        random_seed,
        train_percent,
        test_percent,
        valid_percent,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/custom_algorithm")
async def custom_algorithm(request: Request):
    # noinspection PyBroadException
    """
    获取算法配置
    :return:
    """
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = custom_algorithm_file_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/config_custom_algorithm")
async def config_custom_algorithm(request: Request):
    """
    配置自定义算法算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    algorithm_id = body["algorithm_id"]
    params = body.get("params", [])
    if not algorithm_id:
        raise EmptyParameterValueError(["algorithm_id"]) from None
    store_result = custom_algorithm_file_configuration.configuration(
        version_id, element_id, user_id, algorithm_id, params
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/data_model")
async def data_model(request: Request):
    # noinspection PyBroadException
    """
    获取数据模型的配置
    :return:
    """
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = data_model_configuration.get(version_id, element_id, user_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/config_data_model")
async def config_data_model(request: Request):
    """
    配置数据模型算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    data_model_id = body["data_model_id"]
    data_model_type = DataModelType.Datatable.value
    if not data_model_id or not data_model_type:
        raise EmptyParameterValueError(["data_model_id", "data_model_type"]) from None
    store_result = data_model_configuration.configuration(
        version_id, element_id, user_id, data_model_id, data_model_type
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/sync_input")
async def sync_input(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = sync_input_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/config_sync_input")
async def config_sync_input(request: Request):
    """
    配置同步输入算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    # [{"name": "英文", "nick_name": "中文", "data_type": "NUMBER", "remark": "remark"}]
    json_key = "attrs"
    nick_name = "data"
    value_type = ValueType.Table.value

    if not json_key or not nick_name or not value_type:
        raise EmptyParameterValueError(["json_key", "nick_name", "value_type"]) from None

    description = body.get("description", "")
    start_key = body.get("start_key", "")
    end_key = body.get("end_key", "")
    step_key = body.get("step_key", "")
    single_value_data_type = body.get("single_value_data_type", "")
    fields = body["fields"]
    sort = body.get("sort", 0)
    store_result = sync_input_configuration.configuration(
        version_id,
        element_id,
        user_id,
        json_key,
        nick_name,
        description,
        value_type,
        start_key,
        end_key,
        step_key,
        "",
        single_value_data_type,
        fields,
        sort,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/model_apply")
async def model_apply(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = model_apply_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/config_model_apply")
async def config_model_apply(request: Request):
    """
    配置模型应用算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    fields = body["fields"]
    if not fields:
        raise EmptyParameterValueError(["fields"]) from None
    store_result = model_apply_configuration.configuration(version_id, element_id, user_id, fields)
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/config_column_data_process")
async def config_column_data_process(request: Request):
    """
    配置列数据处理算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    process_fields = body["process_fields"]
    prev_node_output_port, prev_node_type, prev_element_id = get_prev_node(version_id, element_id)
    if process_fields is None:
        raise EmptyParameterValueError(["process_fields"]) from None
    if prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "上级节点查询失败") from None

    store_result = column_data_process_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        process_fields,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/config_min_max")
async def config_min_max(request: Request):
    """
    配置归一化算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    min_max_fields = body["min_max_fields"]
    prev_node_output_port, prev_node_type, prev_element_id = get_prev_node(version_id, element_id)

    if not min_max_fields:
        min_max_fields = []
    if prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "上级节点查询失败") from None
    store_result = min_max_scaler_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        min_max_fields,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/config_regression_evaluate")
async def config_regression_evaluate(request: Request):
    """
    配置回归评估算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    # ["r2", "mae", "mse"]
    evaluate_list = body["evaluate_list"]

    if not evaluate_list:
        evaluate_list = []

    store_result = regression_evaluate_configuration.configuration(version_id, element_id, user_id, evaluate_list)
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/config_analogy_estimation_algorithm")
async def config_analogy_estimation_algorithm(request: Request):
    """
    配置类比法算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    L = body.get("L", 1)
    weight = body["weight"]
    prev_node_output_port, prev_node_type, prev_element_id = get_prev_node(version_id, element_id)
    if prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "上级节点查询失败") from None
    store_result = analogy_estimation_algorithm_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        L,
        weight,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/api_schedule/{publish_id}")
async def api_schedule(request: Request):
    """
    接口执行调度引擎
    :return: response_result
    """
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    sync_input_data = body.get("sync_input_data", None)
    publish_id = request.path_params.get("publish_id")
    if not publish_id:
        raise ExecuteError(sys._getframe().f_code.co_name, "引擎执行失败，未配置 publish_id 参数") from None
    version_id = get_version(publish_id)
    if not version_id:
        raise ExecuteError(sys._getframe().f_code.co_name, "引擎执行失败，未找到对应的 version_id 参数") from None

    sync_input_element_list = get_sync_element_list(version_id)
    if sync_input_element_list and len(set([x["json_key"] for x in sync_input_element_list])) != len(
        sync_input_element_list
    ):
        raise ExecuteError(
            sys._getframe().f_code.co_name,
            "引擎执行失败，该流水线中同步输入算子存在同名的 key 配置",
        ) from None

    # 为了适应参数化软件需求，可以获取模型评估的指标，因此外部传递该过程标识
    process_id = body.get("process_id")
    sync_input_data = generate_input_data(version_id, sync_input_data)
    execute_result = await asyncify(multiprocess_pipelining_exec)(
        sync_input_data, version_id, user_id, publish_id, None, process_id, False
    )
    if execute_result.code != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, execute_result.message) from None
    return JSONResponse(make_json(response_result(data=execute_result.data, message=execute_result.message)))


@router.post("/manual_execute")
async def manual_execute(request: Request):
    """
    手动执行调度引擎
    :return: response_result
    """
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)

    # 当手动执行时，获取同步输入算子配置，自动生成测试数据
    sync_input_element_list = get_sync_element_list(version_id)

    sync_input_data = generate_input_data(version_id)

    if sync_input_element_list and not sync_input_data:
        raise ExecuteError(sys._getframe().f_code.co_name, "同步输入算子未配置完毕") from None

    if sync_input_element_list and len(set([x["json_key"] for x in sync_input_element_list])) != len(
        sync_input_element_list
    ):
        raise ExecuteError(sys._getframe().f_code.co_name, "该流水线中同步输入算子存在同名的 key 配置") from None

    if not version_id:
        raise ExecuteError(sys._getframe().f_code.co_name, "未配置 version_id 参数") from None
    execute_result = await asyncify(multiprocess_pipelining_exec)(
        sync_input_data, version_id, user_id, None, None, None, True
    )
    if execute_result.code != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, execute_result.message) from None
    return JSONResponse(make_json(response_result(data=execute_result.data, message=execute_result.message)))


@router.post("/publish")
async def publish(request: Request):
    """
    发布流水线
    :return: 成功失败
    """
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, _ = get_version_id_and_element_id_from_body(body)
    tag_id_list = ["parameterization-tag-id"]
    description = body.get("description", "")
    publish_result = publish_pipelining(version_id, tag_id_list, user_id, description)
    if publish_result.code != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "发布失败") from None
    return JSONResponse(make_json(response_result(data=publish_result.data)))


@router.post("/copy_version")
async def copy_version(request: Request):
    """
    拷贝流水线
    :return: 成功失败
    """
    body = await get_request_body(request)
    version_id, _ = get_version_id_and_element_id_from_body(body)
    pipelining_id = body["pipelining_id"]
    if not version_id or not pipelining_id:
        raise EmptyParameterValueError(["version_id", "pipelining_id"]) from None
    copy_result = await asyncify(copy_pipelining_version)(pipelining_id, version_id)
    if copy_result.code != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "复制版本失败") from None
    return JSONResponse(make_json(response_result(data=copy_result.data)))


@router.post("/algorithm_params")
async def algorithm_params(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    algorithm_id = body["algorithm_id"]
    query_result = custom_algorithm_file_configuration.get_params(algorithm_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/input_params")
async def input_params(request: Request):
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, _ = get_version_id_and_element_id_from_body(body)
    if not version_id:
        raise EmptyParameterValueError(["version_id"]) from None
    query_result = get_input_params(version_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取参数信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/output_params")
async def output_params(request: Request):
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, _ = get_version_id_and_element_id_from_body(body)
    if not version_id:
        raise EmptyParameterValueError(["version_id"]) from None
    query_result = get_output_params(version_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取参数信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/consistency_analysis")
async def consistency_analysis(request: Request):
    """
    一致性分析接口
    :return: 分数
    """
    body = await get_request_body(request)
    y1 = body["y1"]
    y2 = body["y2"]
    # <= 0 没有一致性
    # 0.01-0.20 极小一致性
    # 0.21-0.40 弱一致性
    # 0.41-0.60 中等一致性
    # 0.61-0.80 强一致性
    # 0.81-1.00 极大一致性
    y1_matrix = np.asarray(y1)
    y2_matrix = np.asarray(y2)
    # 将多维数据变为1维
    flatten_y1_matrix = y1_matrix.flatten()
    flatten_y2_matrix = y2_matrix.flatten()
    score = cohen_kappa_score(flatten_y1_matrix, flatten_y2_matrix)
    return JSONResponse(make_json(response_result(data={"score": round(score, 2)})))


@router.post("/config_svr_regression_algorithm")
async def config_svr_regression_algorithm(request: Request):
    """
    配置svr回归算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    C = body.get("c", 1.0)
    gamma = body.get("gamma", "scale")
    coef0 = body.get("coef0", 1)
    shrinking = body.get("shrinking", 1)
    max_iter = body.get("max_iter", -1)
    tol = body.get("tol", 0.0001)
    store_result = svr_regression_algorithm_configuration.configuration(
        version_id, element_id, user_id, C, gamma, coef0, shrinking, max_iter, tol
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/config_pls_regression_algorithm")
async def config_pls_regression_algorithm(request: Request):
    """
    配置偏最小二乘回归算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    # n_components, scale, max_iter, tol
    n_components = body.get("n_components", 2)
    max_iter = body.get("max_iter", 500)
    scale = body.get("scale", 1)
    tol = body.get("tol", -0.000006)
    store_result = pls_regression_algorithm_configuration.configuration(
        version_id, element_id, user_id, n_components, scale, max_iter, tol
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/config_ridge_regression_algorithm")
async def config_ridge_regression_algorithm(request: Request):
    """
    配置岭回归算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    alpha = body.get("alpha", 100)
    store_result = ridge_regression_algorithm_configuration.configuration(version_id, element_id, user_id, alpha)
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/config_linear_regression_algorithm")
async def config_linear_regression_algorithm(request: Request):
    """
    配置线性回归算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = "parameterization_user_id"
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    penalty = body.get("penalty", Penalty.L1.value)
    max_iter = body.get("max_iter", 1000)
    alpha = body.get("alpha", None)
    store_result = linear_regression_algorithm_configuration.configuration(
        version_id, element_id, user_id, penalty, max_iter, alpha
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))
