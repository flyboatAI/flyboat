import sys

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

import service.data_model.data_model_service
from element_configuration.algorithm_element.cluster import (
    k_means_algorithm_configuration,
)
from element_configuration.algorithm_element.custom import (
    custom_algorithm_file_configuration,
)
from element_configuration.algorithm_element.regression import (
    bp_regression_algorithm_configuration,
    decision_trees_regression_algorithm_configuration,
    exponential_regression_algorithm_configuration,
    lgb_regression_algorithm_configuration,
    linear_regression_algorithm_configuration,
    logarithm_regression_algorithm_configuration,
    lstm_regression_algorithm_configuration,
    pls_regression_algorithm_configuration,
    random_forest_regression_algorithm_configuration,
    ridge_regression_algorithm_configuration,
    svr_regression_algorithm_configuration,
    weibull_algorithm_configuration,
)
from element_configuration.algorithm_element.text import (
    cut_word_algorithm_configuration,
    word_count_algorithm_configuration,
    word_tag_algorithm_configuration,
)
from element_configuration.data_analyze_element import (
    box_plot_analyze_configuration,
    bubble_plot_analyze_configuration,
    grey_relation_analyze_configuration,
    histogram_plot_analyze_configuration,
    line_plot_analyze_configuration,
    pie_plot_analyze_configuration,
    scatter_plot_analyze_configuration,
    sobol_analyze_configuration,
    spider_plot_analyze_configuration,
    tree_plot_analyze_configuration,
)
from element_configuration.data_fusion_element import (
    column_add_configuration,
    column_filter_configuration,
    data_join_configuration,
)
from element_configuration.data_preprocessing_element import (
    column_data_process_configuration,
    column_type_convert_configuration,
    data_filter_configuration,
    data_replication_configuration,
    data_split_configuration,
    inverse_min_max_scaler_configuration,
    min_max_scaler_configuration,
    pca_processing_configuration,
    polynomial_processing_configuration,
    role_setting_configuration,
)
from element_configuration.evaluate_element import (
    classification_evaluate_configuration,
    regression_evaluate_configuration,
)
from element_configuration.formula_element import (
    analogy_estimation_algorithm_configuration,
    arima_regression_algorithm_configuration,
    custom_formula_configuration,
)
from element_configuration.input_output_element import (
    data_model_configuration,
    database_input_configuration,
    database_output_configuration,
    model_file_output_configuration,
    model_sync_output_configuration,
    monte_carlo_generate_configuration,
    sync_input_configuration,
    sync_output_configuration,
)
from element_configuration.model_operation_element import (
    model_apply_configuration,
    model_file_configuration,
)
from enum_type.result_code import ResultCode
from error.empty_parameter_value_error import EmptyParameterValueError
from error.query_error import QueryError
from error.store_error import StoreError
from helper.http_parameter_helper import (
    get_request_body,
    get_user_id_from_request,
    get_version_id_and_element_id_from_body,
)
from helper.response_result_helper import make_json, response_result
from helper.sample_data_generate_helper import (
    create_test_data,
    delete_test_data,
    get_test_data,
)
from helper.wrapper_helper import valid_exist_user_id

# 获取算子弹窗数据 Controller
router = APIRouter()


@router.post("/sync_input")
async def sync_input(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    user_id = get_user_id_from_request(request)
    query_result = sync_input_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/preview_sync_input_test_data")
async def preview_sync_input_test_data(request: Request):
    """
    获取同步输入算子测试数据
    :return:
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = get_test_data(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取预览数据信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/upload_sync_input_test_data")
async def upload_sync_input_test_data(request: Request):
    """
    根据同步输入算子传递的测试数据生成文件
    :return:
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    json_data = body.get("json_data")
    if not json_data or not isinstance(json_data, list):
        raise StoreError(sys._getframe().f_code.co_name, user_id, "上传的数据为空, 无法进行存储") from None

    store_result = create_test_data(user_id, version_id, element_id, json_data)
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据存储失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/delete_sync_input_test_data")
async def delete_sync_input_test_data(request: Request):
    """
    获取同步输入算子测试数据
    :return:
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    delete_result = delete_test_data(version_id, element_id)
    if delete_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "无法删除测试数据") from None
    return JSONResponse(make_json(response_result()))


@router.post("/sync_output")
async def sync_output(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = sync_output_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/model_sync_output")
async def model_sync_output(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = model_sync_output_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/data_model")
@valid_exist_user_id
async def data_model(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = data_model_configuration.get(version_id, element_id, user_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/data_models")
@valid_exist_user_id
async def data_models(request: Request):
    """
    根据用户，获取全部数据模型
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    data_model_table_name = body.get("data_model_table_name")
    query_result = service.data_model.data_model_service.get_all_data_models(user_id, data_model_table_name)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/data_model_fields")
async def data_model_fields(request: Request):
    """
    根据数据模型标识获取该模型的所有字段
    :return:
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    table_name = body.get("table_name")
    if not table_name:
        raise EmptyParameterValueError(["table_name"]) from None
    data = service.data_model.data_model_service.get_sample_data_fields_from_dt_table_by_name(table_name)
    return JSONResponse(make_json(response_result(data=data)))


@router.post("/data_model_samples")
async def data_model_samples(request: Request):
    """
    根据数据模型标识获取该模型的样本展示数据
    :return:
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    table_name = body.get("table_name")
    if not table_name:
        return JSONResponse(make_json(response_result(data=[])))
    page = service.data_model.data_model_service.get_sample_data_from_dt_table_by_name(table_name)
    data = page.data
    return JSONResponse(make_json(response_result(data=data)))


@router.post("/model_file_output")
async def model_file_output(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = model_file_output_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/data_filter")
async def data_filter(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = data_filter_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/data_split")
async def data_split(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = data_split_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/role_setting")
async def role_setting(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = role_setting_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/column_data_process")
async def column_data_process(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = column_data_process_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/column_type_convert")
async def column_type_convert(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = column_type_convert_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/min_max_scaler")
async def min_max_scaler(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = min_max_scaler_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/inverse_min_max_scaler")
async def inverse_min_max_scaler(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = inverse_min_max_scaler_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/polynomial_processing")
async def polynomial_processing(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = polynomial_processing_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/monte_carlo_generate")
async def monte_carlo_generate(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = monte_carlo_generate_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/data_replication")
async def data_replication(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = data_replication_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/model_file")
async def model_file(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = model_file_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/linear_regression_algorithm")
async def linear_regression_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = linear_regression_algorithm_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/ridge_regression_algorithm")
async def ridge_regression_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = ridge_regression_algorithm_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/pls_regression_algorithm")
async def pls_regression_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = pls_regression_algorithm_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/svr_regression_algorithm")
async def svr_regression_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = svr_regression_algorithm_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/random_forest_regression_algorithm")
async def random_forest_regression_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = random_forest_regression_algorithm_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/lgb_regression_algorithm")
async def lgb_regression_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = lgb_regression_algorithm_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/decision_trees_regression_algorithm")
async def decision_trees_regression_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = decision_trees_regression_algorithm_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/arima_regression_algorithm")
async def arima_regression_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = arima_regression_algorithm_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/lstm_regression_algorithm")
async def lstm_regression_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = lstm_regression_algorithm_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/bp_regression_algorithm")
async def bp_regression_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = bp_regression_algorithm_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/k_means_algorithm")
async def k_means_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = k_means_algorithm_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/cut_word_algorithm")
async def cut_word_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")

    query_result = cut_word_algorithm_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/word_count_algorithm")
async def word_count_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")

    query_result = word_count_algorithm_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/word_tag_algorithm")
async def word_tag_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")

    query_result = word_tag_algorithm_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/pca_processing")
async def pca_processing(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = pca_processing_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/histogram_plot_analyze")
async def histogram_plot_analyze(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = histogram_plot_analyze_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/line_plot_analyze")
async def line_plot_analyze(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = line_plot_analyze_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/scatter_plot_analyze")
async def scatter_plot_analyze(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = scatter_plot_analyze_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/pie_plot_analyze")
async def pie_plot_analyze(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = pie_plot_analyze_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/spider_plot_analyze")
async def spider_plot_analyze(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = spider_plot_analyze_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/bubble_plot_analyze")
async def bubble_plot_analyze(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = bubble_plot_analyze_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/tree_plot_analyze")
async def tree_plot_analyze(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = tree_plot_analyze_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/box_plot_analyze")
async def box_plot_analyze(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = box_plot_analyze_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/grey_relation_analyze")
async def grey_relation_analyze(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = grey_relation_analyze_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/sobol_analyze")
async def sobol_analyze(request: Request):
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = sobol_analyze_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/column_filter")
async def column_filter(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = column_filter_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/column_add")
async def column_add(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = column_add_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/arithmetic_function_list")
@valid_exist_user_id
async def arithmetic_function_list(request: Request):
    """
    算数运算函数列表
    :return: response_result
    """
    # noinspection PyBroadException
    user_id = get_user_id_from_request(request)
    query_result = column_add_configuration.get_all_arithmetic_function_list()
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "获取算术运算函数列表失败") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/data_join")
async def data_join(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port_arr = body.get("prev_node_output_port_arr")
    prev_node_type_arr = body.get("prev_node_type_arr")
    prev_element_id_arr = body.get("prev_element_id_arr")
    query_result = data_join_configuration.get(
        prev_node_output_port_arr,
        prev_node_type_arr,
        prev_element_id_arr,
        version_id,
        element_id,
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/model_apply")
async def model_apply(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = model_apply_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/custom_algorithm")
async def custom_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = custom_algorithm_file_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/algorithm_params")
async def algorithm_params(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    algorithm_id = body["algorithm_id"]
    query_result = custom_algorithm_file_configuration.get_params(algorithm_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/regression_evaluate")
async def regression_evaluate(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)

    query_result = regression_evaluate_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/classification_evaluate")
async def classification_evaluate(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)

    query_result = classification_evaluate_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/analogy_estimation_algorithm")
async def analogy_estimation_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = analogy_estimation_algorithm_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/exponential_regression_algorithm")
async def exponential_regression_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = exponential_regression_algorithm_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/logarithm_regression_algorithm")
async def logarithm_regression_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = logarithm_regression_algorithm_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/weibull_algorithm")
async def weibull_algorithm(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = weibull_algorithm_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/custom_formula")
async def custom_formula(request: Request):
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = custom_formula_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/database_input")
@valid_exist_user_id
async def database_input(request: Request):
    # 数据源输入配置查询
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    query_result = database_input_configuration.get(version_id, element_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/database_output")
@valid_exist_user_id
async def database_output(request: Request):
    # 数据源输出配置查询
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body.get("prev_node_output_port")
    prev_node_type = body.get("prev_node_type")
    prev_element_id = body.get("prev_element_id")
    query_result = database_output_configuration.get(
        prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id
    )
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/database_tables")
@valid_exist_user_id
async def database_tables(request: Request):
    # 获取数据表信息，根据数据源
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    datasource_id = body["datasource_id"]
    query_result = database_input_configuration.get_tables(datasource_id)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))


@router.post("/database_columns")
@valid_exist_user_id
async def database_columns(request: Request):
    # 获取数据表字段信息，根据数据源和数据表名
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    datasource_id = body["datasource_id"]
    table_name = body["table_name"]
    query_result = database_output_configuration.get_columns(datasource_id, table_name)
    if query_result.code != ResultCode.Success.value:
        raise QueryError(sys._getframe().f_code.co_name, user_id, "无法获取配置信息") from None
    return JSONResponse(make_json(response_result(data=query_result.data)))
