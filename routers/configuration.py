import sys

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

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
from element_configuration.model_operation_element import model_apply_configuration
from enum_type.filter_type import FilterType
from enum_type.input_type import ValueType
from enum_type.penalty import Penalty
from enum_type.result_code import ResultCode
from enum_type.split_type import SplitType
from error.empty_parameter_value_error import EmptyParameterValueError
from error.store_error import StoreError
from helper.http_parameter_helper import (
    get_request_body,
    get_user_id_from_request,
    get_version_id_and_element_id_from_body,
)
from helper.response_result_helper import (
    make_json,
    response_error_result,
    response_result,
)
from helper.wrapper_helper import valid_exist_user_id

# 配置算子数据 Controller
router = APIRouter()


@router.post("/sync_input")
@valid_exist_user_id
async def sync_input(request: Request):
    """
    配置同步输入算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    # [{"name": "英文", "nick_name": "中文", "data_type": "NUMBER", "remark": "remark"}]
    json_key = body["json_key"]
    nick_name = body["nick_name"]
    value_type = body["value_type"]

    if not json_key or not nick_name or not value_type:
        raise EmptyParameterValueError(["json_key", "nick_name", "value_type"]) from None

    description = body.get("description", "")
    start_key = body.get("start_key", "")
    end_key = body.get("end_key", "")
    step_key = body.get("step_key", "")
    single_key = body.get("single_key", "")
    single_value_data_type = body.get("single_value_data_type", "")
    fields = body.get("fields", [])
    sort = body.get("sort", 0)
    if (
        value_type == ValueType.YearRange.value
        or value_type == ValueType.MonthRange
        or value_type == ValueType.DayRange
    ) and (start_key == end_key):
        raise StoreError(
            sys._getframe().f_code.co_name,
            user_id,
            "起始标识与结束标识配置不能完全相同",
        ) from None
    if value_type == ValueType.IntRange and (start_key == end_key == step_key):
        raise StoreError(
            sys._getframe().f_code.co_name,
            user_id,
            "起始标识、结束标识以及步长标识配置不能完全相同",
        ) from None
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
        single_key,
        single_value_data_type,
        fields,
        sort,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/sync_output")
@valid_exist_user_id
async def sync_output(request: Request):
    """
    配置同步输出算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    # [{"name": "英文", "nick_name": "中文", "data_type": "NUMBER", "remark": "remark"}]
    key = body["key"]
    nick_name = body.get("nick_name")
    fields = body.get("fields", [])
    if not key:
        raise EmptyParameterValueError(["key"]) from None

    store_result = sync_output_configuration.configuration(version_id, element_id, user_id, key, nick_name, fields)
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@valid_exist_user_id
@router.post("/model_sync_output")
async def model_sync_output(request: Request):
    """
    配置模型同步输出算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    key = body["key"]
    nick_name = body.get("nick_name")
    if not key:
        raise EmptyParameterValueError(["key"]) from None

    store_result = model_sync_output_configuration.configuration(version_id, element_id, user_id, key, nick_name)
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/data_model")
@valid_exist_user_id
async def data_model(request: Request):
    """
    配置数据模型算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    data_model_id = body["data_model_id"]
    data_model_type = body["data_model_type"]
    if not data_model_id or not data_model_type:
        raise EmptyParameterValueError(["data_model_id", "data_model_type"]) from None
    store_result = data_model_configuration.configuration(
        version_id, element_id, user_id, data_model_id, data_model_type
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/model_file_output")
@valid_exist_user_id
async def model_file_output(request: Request):
    """
    配置模型输出算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    model_name = body["model_name"]
    if not model_name:
        raise EmptyParameterValueError(["model_name"]) from None
    store_result = model_file_output_configuration.configuration(version_id, element_id, user_id, model_name)
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/data_filter")
@valid_exist_user_id
async def data_filter(request: Request):
    """
    配置数据过滤算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    filter_type = body["filter_type"]
    filter_compare_fields = body["filter_compare_fields"]
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    if (
        not filter_type
        or not filter_compare_fields
        or prev_node_output_port is None
        or not prev_node_type
        or not prev_element_id
    ):
        raise EmptyParameterValueError(
            [
                "filter_type",
                "filter_compare_fields",
                "prev_node_output_port",
                "prev_node_type",
                "prev_element_id",
            ]
        )
    if filter_type != FilterType.All.value and filter_type != FilterType.Any.value:
        return make_json(response_error_result())
    store_result = data_filter_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        filter_type,
        filter_compare_fields,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/data_split")
@valid_exist_user_id
async def data_split(request: Request):
    """
    配置数据切分算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    split_type = body["split_type"]
    random_seed = body.get("random_seed", None)
    train_percent = body["train_percent"]
    test_percent = body["test_percent"]
    valid_percent = body.get("valid_percent", 0)
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    if (train_percent + test_percent + valid_percent) != 100:
        raise StoreError(
            sys._getframe().f_code.co_name,
            user_id,
            "数据库保存配置失败, 数据拆分比例之和不为 100",
        ) from None
    if (split_type == SplitType.TrainTest.value and (train_percent == 0 or test_percent == 0)) or (
        split_type == SplitType.TrainTestValid.value and (train_percent == 0 or test_percent == 0 or valid_percent == 0)
    ):
        raise StoreError(
            sys._getframe().f_code.co_name,
            user_id,
            "数据库保存配置失败, 数据拆分比例不允许为 0",
        ) from None
    if (
        not split_type
        or train_percent is None
        or test_percent is None
        or prev_node_output_port is None
        or not prev_node_type
        or not prev_element_id
    ):
        raise EmptyParameterValueError(
            [
                "split_type",
                "train_percent",
                "test_percent",
                "prev_node_output_port",
                "prev_node_type",
                "prev_element_id",
            ]
        )
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


@router.post("/role_setting")
@valid_exist_user_id
async def role_setting(request: Request):
    """
    配置角色设置算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)

    version_id, element_id = get_version_id_and_element_id_from_body(body)
    role_setting_fields = body["role_setting_fields"]
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    if not role_setting_fields or prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise EmptyParameterValueError(
            [
                "role_setting_fields",
                "prev_node_output_port",
                "prev_node_type",
                "prev_element_id",
            ]
        )
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


@router.post("/column_data_process")
@valid_exist_user_id
async def column_data_process(request: Request):
    """
    配置列数据处理算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)

    version_id, element_id = get_version_id_and_element_id_from_body(body)
    process_fields = body["process_fields"]
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    if not process_fields or prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise EmptyParameterValueError(
            [
                "process_fields",
                "prev_node_output_port",
                "prev_node_type",
                "prev_element_id",
            ]
        )
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


@router.post("/column_type_convert")
@valid_exist_user_id
async def column_type_convert(request: Request):
    """
    配置列类型转换算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)

    version_id, element_id = get_version_id_and_element_id_from_body(body)
    convert_fields = body["convert_fields"]
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    if not convert_fields or prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise EmptyParameterValueError(
            [
                "convert_fields",
                "prev_node_output_port",
                "prev_node_type",
                "prev_element_id",
            ]
        )
    store_result = column_type_convert_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        convert_fields,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/min_max_scaler")
@valid_exist_user_id
async def min_max_scaler(request: Request):
    """
    配置归一化算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)

    version_id, element_id = get_version_id_and_element_id_from_body(body)
    min_max_fields = body["min_max_fields"]
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    if not min_max_fields or prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise EmptyParameterValueError(
            [
                "min_max_fields",
                "prev_node_output_port",
                "prev_node_type",
                "prev_element_id",
            ]
        )
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


@router.post("/inverse_min_max_scaler")
@valid_exist_user_id
async def inverse_min_max_scaler(request: Request):
    """
    配置逆归一化算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    inverse_min_max_fields = body["inverse_min_max_fields"]
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    if not inverse_min_max_fields or prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise EmptyParameterValueError(
            [
                "inverse_min_max_fields",
                "prev_node_output_port",
                "prev_node_type",
                "prev_element_id",
            ]
        )
    store_result = inverse_min_max_scaler_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        inverse_min_max_fields,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/polynomial_processing")
@valid_exist_user_id
async def polynomial_processing(request: Request):
    """
    配置多项式特征生成
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)

    version_id, element_id = get_version_id_and_element_id_from_body(body)

    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    degree = body["degree"]
    interaction_only = body["interaction_only"]

    store_result = polynomial_processing_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        degree,
        interaction_only,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/pca_processing")
@valid_exist_user_id
async def pca_processing(request: Request):
    """
    pca降维
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)

    version_id, element_id = get_version_id_and_element_id_from_body(body)

    n_components = body.get("n_components", 2)
    store_result = pca_processing_configuration.configuration(version_id, element_id, user_id, n_components)
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/data_replication")
@valid_exist_user_id
async def data_replication(request: Request):
    """
    配置数据复制算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    output_port_num = body["output_port_num"]
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    if not output_port_num or prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise EmptyParameterValueError(
            [
                "output_port_num",
                "prev_node_output_port",
                "prev_node_type",
                "prev_element_id",
            ]
        )
    store_result = data_replication_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        output_port_num,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/linear_regression_algorithm")
@valid_exist_user_id
async def linear_regression_algorithm(request: Request):
    """
    配置线性回归算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
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


@router.post("/ridge_regression_algorithm")
@valid_exist_user_id
async def ridge_regression_algorithm(request: Request):
    """
    配置岭回归算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)

    version_id, element_id = get_version_id_and_element_id_from_body(body)
    alpha = body.get("alpha", 100)
    store_result = ridge_regression_algorithm_configuration.configuration(version_id, element_id, user_id, alpha)
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/pls_regression_algorithm")
@valid_exist_user_id
async def pls_regression_algorithm(request: Request):
    """
    配置偏最小二乘回归算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)

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


@router.post("/monte_carlo_generate")
@valid_exist_user_id
async def monte_carlo_generate(request: Request):
    """
    配置蒙特卡洛数据生成算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)

    version_id, element_id = get_version_id_and_element_id_from_body(body)

    num_simulations = body.get("num_simulations", 1000)
    feature_value = body["feature_value"]
    store_result = monte_carlo_generate_configuration.configuration(
        version_id, element_id, user_id, num_simulations, feature_value
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/grey_relation_analyze")
@valid_exist_user_id
async def grey_relation_analyze(request: Request):
    """
    配置灰色关联分析算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    rho = body.get("rho", 0.5)
    weight = body.get("weight", [])
    store_result = grey_relation_analyze_configuration.configuration(version_id, element_id, user_id, rho, weight)
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/sobol_analyze")
@valid_exist_user_id
async def sobol_analyze(request: Request):
    """
    配置敏感度分析算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    feature_value = body.get("feature_value", [])
    store_result = sobol_analyze_configuration.configuration(version_id, element_id, user_id, feature_value)
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/svr_regression_algorithm")
@valid_exist_user_id
async def svr_regression_algorithm(request: Request):
    """
    配置svr回归算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
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


@router.post("/random_forest_regression_algorithm")
@valid_exist_user_id
async def random_forest_regression_algorithm(request: Request):
    """
    配置随机森林回归算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)

    version_id, element_id = get_version_id_and_element_id_from_body(body)

    n_estimators = body.get("n_estimators", 100)
    criterion = body.get("criterion", "squared_error")
    max_depth = body.get("max_depth", 200)
    min_samples_split = body.get("min_samples_split", 2)
    min_samples_leaf = body.get("min_samples_leaf", 1)
    min_weight_fraction_leaf = body.get("min_weight_fraction_leaf", 0.0)
    max_features = body.get("max_features", "sqrt")
    max_leaf_nodes = body.get("max_leaf_nodes", 100)
    min_impurity_decrease = body.get("min_impurity_decrease", 0.0)
    bootstrap = body.get("bootstrap", 1)
    random_state = body.get("random_state", 40)

    store_result = random_forest_regression_algorithm_configuration.configuration(
        version_id,
        element_id,
        user_id,
        n_estimators,
        criterion,
        max_depth,
        min_samples_split,
        min_samples_leaf,
        min_weight_fraction_leaf,
        max_features,
        max_leaf_nodes,
        min_impurity_decrease,
        bootstrap,
        random_state,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/decision_trees_regression_algorithm")
@valid_exist_user_id
async def decision_trees_regression_algorithm(request: Request):
    """
    配置决策树回归算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    # max_depth, min_samples_split, min_samples_leaf,
    # max_leaf_nodes, max_features, criterion

    max_depth = body.get("max_depth", 6)
    min_samples_split = body.get("min_samples_split", 2)
    min_samples_leaf = body.get("min_samples_leaf", 4)
    max_leaf_nodes = body.get("max_leaf_nodes", 5)
    max_features = body.get("max_features", "log2")
    criterion = body.get("criterion", "mse")

    store_result = decision_trees_regression_algorithm_configuration.configuration(
        version_id,
        element_id,
        user_id,
        max_depth,
        min_samples_split,
        min_samples_leaf,
        max_leaf_nodes,
        max_features,
        criterion,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/lgb_regression_algorithm")
@valid_exist_user_id
async def lgb_regression_algorithm(request: Request):
    """
    配置决策树回归算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    # boosting_type, num_leaves, learning_rate,
    # feature_fraction, max_depth, bagging_fraction,
    # num_boost_round

    boosting_type = body.get("boosting_type", "gbdt")
    num_leaves = body.get("num_leaves", 31)
    learning_rate = body.get("learning_rate", 0.1)
    feature_fraction = body.get("feature_fraction", 0.9)
    max_depth = body.get("max_depth", 1000)
    bagging_fraction = body.get("bagging_fraction", 0.8)
    num_boost_round = body.get("num_boost_round", 6)
    objective = body.get("objective", "regression")
    store_result = lgb_regression_algorithm_configuration.configuration(
        version_id,
        element_id,
        user_id,
        boosting_type,
        num_leaves,
        learning_rate,
        feature_fraction,
        max_depth,
        bagging_fraction,
        num_boost_round,
        objective,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/arima_regression_algorithm")
@valid_exist_user_id
async def arima_regression_algorithm(request: Request):
    """
    配置时间序列算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    p = body.get("p", 2)
    d = body.get("d", 2)
    q = body.get("q", 2)
    store_result = arima_regression_algorithm_configuration.configuration(version_id, element_id, user_id, p, d, q)
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/bp_regression_algorithm")
@valid_exist_user_id
async def bp_regression_algorithm(request: Request):
    """
    配置bp回归算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    # epoch, lr

    epoch = body.get("epoch", 5000)
    lr = body.get("lr", 0.001)
    number_of_node = body.get("number_of_node", 32)
    store_result = bp_regression_algorithm_configuration.configuration(
        version_id, element_id, user_id, epoch, lr, number_of_node
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/lstm_regression_algorithm")
@valid_exist_user_id
async def lstm_regression_algorithm(request: Request):
    """
    配置lstm回归算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    # epoch, lr,hidden_size,num_layers,dropout

    epoch = body.get("epoch", 5000)
    lr = body.get("lr", 0.001)
    hidden_size = body.get("hidden_size", 500)
    num_layers = body.get("num_layers", 5)
    dropout = body.get("dropout", 0.5)

    store_result = lstm_regression_algorithm_configuration.configuration(
        version_id, element_id, user_id, epoch, lr, hidden_size, num_layers, dropout
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/k_means_algorithm")
@valid_exist_user_id
async def k_means_algorithm(request: Request):
    """
    配置决策树回归算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    k_num = body.get("k_num", 2)
    store_result = k_means_algorithm_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        k_num,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/cut_word_algorithm")
@valid_exist_user_id
async def cut_word_algorithm(request: Request):
    """
    配置分词算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    column_name = body.get("column_name", "")
    store_result = cut_word_algorithm_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        column_name,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/word_count_algorithm")
@valid_exist_user_id
async def word_count_algorithm(request: Request):
    """
    配置词频算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    column_name = body.get("column_name", "")
    store_result = word_count_algorithm_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        column_name,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/word_tag_algorithm")
@valid_exist_user_id
async def word_tag_algorithm(request: Request):
    """
    配置关键词提取算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    column_name = body.get("column_name", "")
    top_k = body.get("top_k", 5)
    store_result = word_tag_algorithm_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        top_k,
        column_name,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/histogram_plot_analyze")
@valid_exist_user_id
async def histogram_plot_analyze(request: Request):
    """
    配置柱状图分析算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    x = body["x"]
    y = body["y"]
    if not x or not y or prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise EmptyParameterValueError(["x", "y", "prev_node_output_port", "prev_node_type", "prev_element_id"])
    store_result = histogram_plot_analyze_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        x,
        y,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/line_plot_analyze")
@valid_exist_user_id
async def line_plot_analyze(request: Request):
    """
    配置折线图分析算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    x = body["x"]
    y = body["y"]
    if not x or not y or prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise EmptyParameterValueError(["x", "y", "prev_node_output_port", "prev_node_type", "prev_element_id"])
    store_result = line_plot_analyze_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        x,
        y,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/scatter_plot_analyze")
@valid_exist_user_id
async def scatter_plot_analyze(request: Request):
    """
    配置散点图分析算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    x = body["x"]
    y = body["y"]
    if not x or not y or prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise EmptyParameterValueError(["x", "y", "prev_node_output_port", "prev_node_type", "prev_element_id"])
    store_result = scatter_plot_analyze_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        x,
        y,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/pie_plot_analyze")
@valid_exist_user_id
async def pie_plot_analyze(request: Request):
    """
    配置饼图分析算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    dimension_mapping = body["dimension_mapping"]
    if not dimension_mapping or prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise EmptyParameterValueError(
            [
                "dimension_mapping",
                "prev_node_output_port",
                "prev_node_type",
                "prev_element_id",
            ]
        )
    store_result = pie_plot_analyze_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        dimension_mapping,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/spider_plot_analyze")
@valid_exist_user_id
async def spider_plot_analyze(request: Request):
    """
    配置雷达图分析算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    dimension_mapping = body["dimension_mapping"]
    if not dimension_mapping or prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise EmptyParameterValueError(
            [
                "dimension_mapping",
                "prev_node_output_port",
                "prev_node_type",
                "prev_element_id",
            ]
        )
    store_result = spider_plot_analyze_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        dimension_mapping,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/bubble_plot_analyze")
@valid_exist_user_id
async def bubble_plot_analyze(request: Request):
    """
    配置气泡图分析算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    x = body["x"]
    y = body["y"]
    if not x or not y or prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise EmptyParameterValueError(["x", "y", "prev_node_output_port", "prev_node_type", "prev_element_id"])
    store_result = bubble_plot_analyze_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        x,
        y,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/tree_plot_analyze")
@valid_exist_user_id
async def tree_plot_analyze(request: Request):
    """
    配置树图分析算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    id_column = body["id_column"]
    pid_column = body["pid_column"]
    name_column = body["name_column"]
    value_column = body["value_column"]
    if (
        not id_column
        or not pid_column
        or not name_column
        or not value_column
        or prev_node_output_port is None
        or not prev_node_type
        or not prev_element_id
    ):
        raise EmptyParameterValueError(
            [
                "id_column",
                "pid_column",
                "name_column",
                "value_column",
                "prev_node_output_port",
                "prev_node_type",
                "prev_element_id",
            ]
        )
    store_result = tree_plot_analyze_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        id_column,
        pid_column,
        name_column,
        value_column,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/box_plot_analyze")
@valid_exist_user_id
async def box_plot_analyze(request: Request):
    """
    配置盒须图分析算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    dimension_column = body["dimension_column"]
    value_column = body["value_column"]
    if (
        not dimension_column
        or not value_column
        or prev_node_output_port is None
        or not prev_node_type
        or not prev_element_id
    ):
        raise EmptyParameterValueError(
            [
                "dimension_column",
                "value_column",
                "prev_node_output_port",
                "prev_node_type",
                "prev_element_id",
            ]
        )
    store_result = box_plot_analyze_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        dimension_column,
        value_column,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/column_filter")
@valid_exist_user_id
async def column_filter(request: Request):
    """
    配置列过滤算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    filter_fields = body.get("filter_fields", [])
    if not filter_fields or prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise EmptyParameterValueError(
            [
                "filter_fields",
                "prev_node_output_port",
                "prev_node_type",
                "prev_element_id",
            ]
        )
    store_result = column_filter_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        filter_fields,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/column_add")
@valid_exist_user_id
async def column_add(request: Request):
    """
    配置列新增算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    add_fields = body.get("add_fields", [])
    if not add_fields or prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise EmptyParameterValueError(["add_fields", "prev_node_output_port", "prev_node_type", "prev_element_id"])
    store_result = column_add_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port,
        prev_node_type,
        prev_element_id,
        add_fields,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/data_join")
@valid_exist_user_id
async def data_join(request: Request):
    """
    配置数据连接算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port_arr = body.get("prev_node_output_port_arr", [])
    prev_node_type_arr = body.get("prev_node_type_arr", [])
    prev_element_id_arr = body.get("prev_element_id_arr", [])
    join_type = body.get("join_type", None)
    join_field = body.get("join_field", [])
    fields = body.get("fields", [])

    if (
        not join_type
        or not join_field
        or not fields
        or not prev_node_output_port_arr
        or not prev_node_type_arr
        or not prev_element_id_arr
    ):
        raise EmptyParameterValueError(
            [
                "join_type",
                "join_field",
                "fields" "prev_node_output_port",
                "prev_node_type",
                "prev_element_id",
            ]
        )
    store_result = data_join_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port_arr,
        prev_node_type_arr,
        prev_element_id_arr,
        join_type,
        join_field,
        fields,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/model_apply")
@valid_exist_user_id
async def model_apply(request: Request):
    """
    配置模型应用算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    fields = body["fields"]
    if not fields:
        raise EmptyParameterValueError(["fields"]) from None
    store_result = model_apply_configuration.configuration(version_id, element_id, user_id, fields)
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/custom_algorithm")
@valid_exist_user_id
async def custom_algorithm(request: Request):
    """
    配置自定义算法算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
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


@router.post("/regression_evaluate")
@valid_exist_user_id
async def regression_evaluate(request: Request):
    """
    配置回归评估算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    # ["r2", "mae", "mse"]
    evaluate_list = body["evaluate_list"]

    if not evaluate_list:
        raise EmptyParameterValueError(["evaluate_list"]) from None
    store_result = regression_evaluate_configuration.configuration(version_id, element_id, user_id, evaluate_list)
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/classification_evaluate")
@valid_exist_user_id
async def classification_evaluate(request: Request):
    """
    配置分类评估算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    # ["f1", "precision", "recall"]
    evaluate_list = body["evaluate_list"]

    if not evaluate_list:
        raise EmptyParameterValueError(["evaluate_list"]) from None
    store_result = classification_evaluate_configuration.configuration(version_id, element_id, user_id, evaluate_list)
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/analogy_estimation_algorithm")
@valid_exist_user_id
async def analogy_estimation_algorithm(request: Request):
    """
    配置类比法算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    L = body["L"]
    weight = body["weight"]
    prev_node_output_port = body["prev_node_output_port"]
    prev_node_type = body["prev_node_type"]
    prev_element_id = body["prev_element_id"]
    if prev_node_output_port is None or not prev_node_type or not prev_element_id:
        raise EmptyParameterValueError(["prev_node_output_port", "prev_node_type", "prev_element_id"])
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


@router.post("/weibull_algorithm")
@valid_exist_user_id
async def weibull_algorithm(request: Request):
    """
    配置威布尔算法算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    # m, a, epoch
    calc_method = body.get("calc_method", "linear_regression")
    is_multimodal = body.get("is_multimodal", 0)
    number_of_peaks = body.get("number_of_peaks", 1)
    number_of_peaks_region = body.get("number_of_peaks_region", [[]])
    m_a_list = body.get("m_a_list", [[4, 1]])
    optimization_method = body.get("optimization_method", "sgd")
    lr = body.get("lr", 1e-3)
    epoch = body.get("epoch", 1000)

    store_result = weibull_algorithm_configuration.configuration(
        version_id,
        element_id,
        user_id,
        calc_method,
        is_multimodal,
        number_of_peaks,
        number_of_peaks_region,
        m_a_list,
        optimization_method,
        lr,
        epoch,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/logarithm_regression_algorithm")
@valid_exist_user_id
async def logarithm_regression_algorithm(request: Request):
    """
    配置对数回归算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    # a,b
    a = body.get("a", 1)
    b = body.get("b", 2)

    store_result = logarithm_regression_algorithm_configuration.configuration(version_id, element_id, user_id, a, b)
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/exponential_regression_algorithm")
@valid_exist_user_id
async def exponential_regression_algorithm(request: Request):
    """
    配置指数回归算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    # a,b
    a = body.get("a", 1)
    b = body.get("b", 2)
    c = body.get("c", 3)

    store_result = exponential_regression_algorithm_configuration.configuration(
        version_id, element_id, user_id, a, b, c
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/custom_formula")
@valid_exist_user_id
async def custom_formula(request: Request):
    """
    配置自定义公式算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    prev_node_output_port_arr = body.get("prev_node_output_port_arr", [])
    prev_node_type_arr = body.get("prev_node_type_arr", [])
    prev_element_id_arr = body.get("prev_element_id_arr", [])
    formula_content = body["formula_content"]
    if not formula_content:
        raise EmptyParameterValueError(["formula_content"]) from None
    store_result = custom_formula_configuration.configuration(
        version_id,
        element_id,
        user_id,
        prev_node_output_port_arr,
        prev_node_type_arr,
        prev_element_id_arr,
        formula_content,
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/database_input")
@valid_exist_user_id
async def database_input(request: Request):
    """
    配置数据输入算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    datasource_id = body["datasource_id"]
    data_table_name = body["data_table_name"]
    if not datasource_id or not data_table_name:
        raise EmptyParameterValueError(["datasource_id", "data_table_name"]) from None
    store_result = database_input_configuration.configuration(
        version_id, element_id, user_id, datasource_id, data_table_name
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据源输入算子保存配置失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/database_output")
@valid_exist_user_id
async def database_output(request: Request):
    """
    配置数据输出算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    datasource_id = body["datasource_id"]
    data_table_name = body["data_table_name"]
    columns = body["columns"]
    if not datasource_id or not data_table_name or not columns:
        raise EmptyParameterValueError(["datasource_id", "data_table_name", "columns"]) from None
    store_result = database_output_configuration.configuration(
        version_id, element_id, user_id, datasource_id, data_table_name, columns
    )
    if store_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "数据源输出算子保存配置失败") from None
    return JSONResponse(make_json(response_result()))
