import sys
from collections import defaultdict
from typing import Annotated

from asyncer import asyncify
from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse
from websockets.sync.client import connect

from config import setting
from core.pipelining_engine import cancel_execute_engine
from enum_type.element_config_type import ElementConfigType
from enum_type.result_code import ResultCode
from error.empty_parameter_value_error import EmptyParameterValueError
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.app_helper import register_app
from helper.http_parameter_helper import (
    get_request_body,
    get_user_id_from_blade_auth,
    get_user_id_from_request,
    get_version_id_and_element_id_from_body,
)
from helper.insight_helper import insight_element
from helper.oss_helper.oss_helper import oss_helper1
from helper.pipelining_element_helper import (
    create_new_pipelining_element,
    get_pipelining_element_list,
    get_registered_pipelining_element_list,
    register_pipelining_element_by_id,
    unregister_pipelining_element_by_id,
)
from helper.pipelining_helper import (
    add_pipelining,
    cancel_publish_pipelining,
    config_order,
    config_pipelining_dag,
    config_pipelining_logic_flow,
    copy_pipelining_version,
    delete_experiment_space_with_id,
    delete_pipelining_by_id,
    delete_pipelining_version,
    delete_pipelining_version_multi,
    disable_element_list,
    edit_pipelining,
    edit_version_name,
    get_all_pipelining_list,
    get_experiment_space_list,
    get_pipelining_and_version_tree,
    get_pipelining_list_by_name,
    get_pipelining_logic_flow,
    get_pipelining_version_list,
    get_sync_element_list,
    move_pipelining,
    multiprocess_pipelining_exec,
    publish_pipelining,
    save_experiment_space,
    save_experiment_space_with_id,
)
from helper.pipelining_publish_helper import (
    all_publish_list,
    associate_client_with_publish_id_list,
    change_publish_name_and_description,
    delete_client_publish_list,
    get_all_client,
    get_audit,
    get_call_count,
    get_client_publish_list,
    get_status_count,
    publish_list_by_tag,
)
from helper.response_result_helper import make_json, response_result
from helper.sample_data_generate_helper import generate_input_data
from helper.token_helper import (
    delete_secret_by_id,
    generate_key,
    get_secret_by_publish_id,
)
from helper.warning_helper import UNUSED
from helper.websocket_helper import generate_create_message, send_message
from helper.wrapper_helper import (
    valid_exist_user_id,
    valid_exist_user_id_from_blade_auth,
)
from insight.insight_element_data import recent_process_id
from license_service.serial_number import SerialNumber
from parameter_entity.data_model.general_data_model import GeneralDataModel
from parameter_entity.logic_flow.dag import Dag
from parameter_entity.logic_flow.logic_flow import LogicFlow
from parameter_entity.pipelining_element.pipelining_element import PipeliningElement
from publish.pipelining_publish import get_version
from service.data_model.data_model_service import (
    create_excel_file_by_table,
    get_sample_data_fields_from_datasource,
    get_sample_data_from_datasource,
)
from service.element.element_service import get_element_tree
from service.pipelining_element.pipelining_element_service import (
    delete_pipelining_element_by_id,
)

# 配置流水线数据 Controller
router = APIRouter()


@router.post("/cancel_execute")
@valid_exist_user_id
async def cancel_execute(request: Request):
    """
    取消执行
    :return: response_result
    """
    body = await get_request_body(request)
    version_id, _ = get_version_id_and_element_id_from_body(body)
    pid = body.get("pid")
    if not pid:
        raise KeyError("pid") from None
    connect_id = body["connect_id"]
    cancel_execute_engine(pid, connect_id)
    return JSONResponse(make_json(response_result()))


@router.post("/manual_execute")
@valid_exist_user_id
async def manual_execute(request: Request):
    """
    手动执行调度引擎
    :return: response_result
    """
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, _ = get_version_id_and_element_id_from_body(body)
    connect_id = body["connect_id"]
    to_element_id = body.get("to_element_id")

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

    with connect(setting.WEBSOCKET_URL) as websocket:
        msg = generate_create_message(connect_id)
        send_message(websocket, msg)
    serial_number = SerialNumber.get_serial_number()
    execute_result = await asyncify(multiprocess_pipelining_exec)(
        sync_input_data,
        version_id,
        user_id,
        None,
        connect_id,
        None,
        True,
        to_element_id,
        serial_number,
    )
    if execute_result.code != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, execute_result.message) from None
    return JSONResponse(make_json(response_result(data=execute_result.data, message=execute_result.message)))


@router.post("/api_schedule/{publish_id}")
async def api_schedule(request: Request):
    """
    接口执行调度引擎
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = setting.ZMS_MAGIC
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
    serial_number = SerialNumber.get_serial_number()
    execute_result = await asyncify(multiprocess_pipelining_exec)(
        sync_input_data,
        version_id,
        user_id,
        publish_id,
        None,
        process_id,
        False,
        None,
        serial_number,
    )
    if execute_result.code != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, execute_result.message) from None
    return JSONResponse(make_json(response_result(data=execute_result.data, message=execute_result.message)))


@router.post("/recent_process")
@valid_exist_user_id
async def recent_process(request: Request):
    """
    获取该流水线版本最新执行标识
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    version_id, _ = get_version_id_and_element_id_from_body(body)
    execute_result = recent_process_id(version_id)
    if execute_result.code != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, execute_result.message) from None
    return JSONResponse(make_json(response_result(data=execute_result.data)))


@router.post("/insight")
@valid_exist_user_id
async def insight(request: Request):
    """
    洞察信息接口
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    node_type = body["node_type"]
    process_id = body["process_id"]
    execute_result = await asyncify(insight_element)(version_id, element_id, node_type, process_id)
    if execute_result.code != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, execute_result.message) from None
    return JSONResponse(make_json(response_result(data=execute_result.data)))


@router.post("/publish")
@valid_exist_user_id
async def publish(request: Request):
    """
    发布流水线
    :return: 成功失败
    """
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, _ = get_version_id_and_element_id_from_body(body)
    tag_id_list = body["tag_id_list"]
    description = body.get("description", "")
    publish_result = publish_pipelining(version_id, tag_id_list, user_id, description)
    if publish_result.code != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "发布失败") from None
    return JSONResponse(make_json(response_result(data=publish_result.data)))


@router.post("/cancel_publish")
@valid_exist_user_id
async def cancel_publish(request: Request):
    """
    发布流水线
    :return: 成功失败
    """
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    publish_id = body["publish_id"]
    cancel_result = cancel_publish_pipelining(publish_id, user_id)
    if cancel_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "取消发布失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/pipelining_list")
@valid_exist_user_id
async def pipelining_list(request: Request):
    """
    获取流水线列表
    :return: 列表
    """
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    pipelining_name = body.get("pipelining_name")
    experiment_id = body["experiment_id"]
    if pipelining_name:
        pipelining_arr = await asyncify(get_pipelining_list_by_name)(user_id, experiment_id, pipelining_name)
    else:
        pipelining_arr = await asyncify(get_all_pipelining_list)(user_id, experiment_id)

    return JSONResponse(make_json(response_result(data=pipelining_arr)))


@router.post("/change")
@valid_exist_user_id
async def change(request: Request):
    """
    编辑流水线，修改名称和描述
    :return: 成功失败
    """
    body = await get_request_body(request)
    pipelining_name = body.get("pipelining_name", "")
    pipelining_id = body.get("pipelining_id")
    description = body.get("description", "")
    if not pipelining_name or not pipelining_id:
        raise EmptyParameterValueError(["pipelining_nane", "pipelining_id"])
    edit_result = edit_pipelining(pipelining_id, pipelining_name, description)
    if edit_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "编辑失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/delete")
@valid_exist_user_id
async def delete(request: Request):
    """
    删除流水线
    :return: 成功失败
    """
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    pipelining_id = body.get("pipelining_id")
    if not pipelining_id:
        raise EmptyParameterValueError(["pipelining_id"]) from None
    delete_result = delete_pipelining_by_id(user_id, pipelining_id)
    if delete_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "删除失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/move")
@valid_exist_user_id
async def move(request: Request):
    """
    移动流水线至其他空间
    :return: 成功失败
    """
    body = await get_request_body(request)
    pipelining_id = body.get("pipelining_id")
    experiment_id = body.get("experiment_id")
    if not pipelining_id or not experiment_id:
        raise EmptyParameterValueError(["pipelining_id", "experiment_id"]) from None
    move_result = move_pipelining(experiment_id, pipelining_id)
    if move_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "移动失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/add")
@valid_exist_user_id
async def add(request: Request):
    """
    新增流水线
    :return: 成功失败
    """
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    experiment_id = body.get("experiment_id")
    pipelining_name = body.get("pipelining_name", "")
    description = body.get("description", "")
    if not pipelining_name or not experiment_id:
        raise EmptyParameterValueError(["pipelining_name", "experiment_id"]) from None
    add_result = add_pipelining(experiment_id, pipelining_name, description, user_id)
    if add_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "新增失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/version_list")
@valid_exist_user_id
async def version_list(request: Request):
    """
    获取某一流水线的全部版本信息列表
    :return: 列表
    """
    body = await get_request_body(request)
    pipelining_id = body.get("pipelining_id", None)
    if not pipelining_id:
        raise EmptyParameterValueError(["pipelining_id"]) from None
    pipelining_version_list = await asyncify(get_pipelining_version_list)(pipelining_id)
    return JSONResponse(make_json(response_result(data=pipelining_version_list)))


@router.post("/logic_flow_by_version_id")
@valid_exist_user_id
async def logic_flow_by_version_id(request: Request):
    """
    获取画布 logic_flow
    :return: 成功失败
    """
    body = await get_request_body(request)
    version_id, _ = get_version_id_and_element_id_from_body(body)
    if not version_id:
        raise EmptyParameterValueError(["version_id"]) from None
    logic_flow_dict = get_pipelining_logic_flow(version_id)
    if not logic_flow_dict:
        raise ExecuteError(sys._getframe().f_code.co_name, "获取画布信息失败") from None
    relative_path = logic_flow_dict.get("logic_flow")
    logic_flow_data = await asyncify(oss_helper1.get_json_file_data)(relative_path)
    return JSONResponse(make_json(response_result(data=logic_flow_data)))


@router.post("/logic_flow")
@valid_exist_user_id
async def get_logic_flow(request: Request):
    """
    获取画布 logic_flow
    :return: 成功失败
    """
    body = await get_request_body(request)
    relative_path = body.get("relative_path", None)
    if not relative_path:
        raise EmptyParameterValueError(["relative_path"]) from None
    relative_path = body.get("relative_path", "")
    logic_flow_data = await asyncify(oss_helper1.get_json_file_data)(relative_path)
    return JSONResponse(make_json(response_result(data=logic_flow_data)))


@router.post("/logic_flow_config")
@valid_exist_user_id_from_blade_auth
def logic_flow_config(blade_auth: Annotated[str | None, Header()], logic_flow: LogicFlow):
    """
    配置画布 logic_flow
    :return: 成功失败
    """
    UNUSED(blade_auth)
    version_id = logic_flow.version_id
    if not version_id:
        raise EmptyParameterValueError(["version_id"]) from None
    logic_flow_str = logic_flow.logic_flow
    config_result = config_pipelining_logic_flow(version_id, logic_flow_str)
    if config_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "更新流水线失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/dag_config")
@valid_exist_user_id_from_blade_auth
def dag_config(blade_auth: Annotated[str | None, Header()], dag: Dag):
    """
    配置画布 DAG
    :return: 成功失败
    """
    UNUSED(blade_auth)
    version_id = dag.version_id
    if not version_id:
        raise EmptyParameterValueError(["version_id"]) from None
    dag_str = dag.dag
    config_result = config_pipelining_dag(version_id, dag_str)
    if config_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "更新流水线失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/disable_elements")
@valid_exist_user_id
async def disable_elements(request: Request):
    """
    算子置为未配置状态
    :return: 成功失败
    """
    body = await get_request_body(request)
    #   {
    #     "sync_input": ["element_id_1", "element_id_2"],
    #     "data_model": ["element_id_3"]
    #   }
    version_id, _ = get_version_id_and_element_id_from_body(body)
    type_element_id_dict = body["dict"]
    if not version_id:
        raise EmptyParameterValueError(["version_id"]) from None
    disable_result = disable_element_list(version_id, type_element_id_dict)
    if disable_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "算子失效更新失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/copy_version")
@valid_exist_user_id
async def copy_version(request: Request):
    """
    拷贝流水线
    :return: 成功失败
    """
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, _ = get_version_id_and_element_id_from_body(body)
    pipelining_id = body["pipelining_id"]
    if not pipelining_id:
        raise EmptyParameterValueError(["pipelining_id"]) from None

    copy_result = await asyncify(copy_pipelining_version)(pipelining_id, version_id, user_id=user_id)
    if copy_result.code != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "复制版本失败") from None
    return JSONResponse(make_json(response_result(data=copy_result.data)))


@router.post("/delete_version")
@valid_exist_user_id
async def delete_version(request: Request):
    """
    删除流水线
    :return: 成功失败
    """
    body = await get_request_body(request)
    version_id, _ = get_version_id_and_element_id_from_body(body)
    pipelining_id = body["pipelining_id"]
    if not version_id:
        raise EmptyParameterValueError(["version_id", "pipelining_id"]) from None
    delete_result = delete_pipelining_version(pipelining_id, version_id)
    if delete_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "删除版本失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/sync_input_list")
async def sync_input_list(request: Request):
    """
    获取某版本同步输入算子列表，用于配置顺序
    :return: 同步输入算子列表
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    version_id, _ = get_version_id_and_element_id_from_body(body)
    return JSONResponse(make_json(response_result(data=get_sync_element_list(version_id))))


@router.post("/sync_output_list")
async def sync_output_list(request: Request):
    """
    获取某版本同步输入算子列表，用于配置顺序
    :return: 同步输入算子列表
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    version_id, _ = get_version_id_and_element_id_from_body(body)
    return JSONResponse(
        make_json(response_result(data=get_sync_element_list(version_id, ElementConfigType.Output.value)))
    )


@router.post("/sync_model_list")
async def sync_model_list(request: Request):
    """
    获取某版本同步输入算子列表，用于配置顺序
    :return: 同步输入算子列表
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    version_id, _ = get_version_id_and_element_id_from_body(body)
    return JSONResponse(
        make_json(response_result(data=get_sync_element_list(version_id, ElementConfigType.ModelOutput.value)))
    )


@router.post("/config_sync_element_order")
async def config_sync_element_order(request: Request):
    """
    配置同步输入算子列表顺序，用于前台显示
    :return: 成功失败
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, _ = get_version_id_and_element_id_from_body(body)
    # [{id: "", sort: 0}, {"id": "", sort: 1}]
    element_list = body["element_list"]
    config_result = config_order(element_list, version_id)
    if config_result != ResultCode.Success.value:
        raise StoreError(sys._getframe().f_code.co_name, user_id, "顺序调整失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/publish_list")
@valid_exist_user_id
async def publish_list(request: Request):
    """
    根据标签标识获取发布列表
    :return: 发布列表
    """
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, _ = get_version_id_and_element_id_from_body(body)
    tag_id = body.get("tag_id")
    return JSONResponse(make_json(response_result(data=publish_list_by_tag(tag_id, user_id))))


@router.post("/call_count")
@valid_exist_user_id
async def call_count(request: Request):
    """
    获取接口统计信息(调用次数)
    :return: 获取接口统计信息
    """
    body = await get_request_body(request)
    publish_id = body["publish_id"]
    start_time = body["start_time"]
    end_time = body["end_time"]
    return JSONResponse(make_json(response_result(data=get_call_count(start_time, end_time, publish_id))))


@router.post("/status_count")
@valid_exist_user_id
async def status_count(request: Request):
    """
    获取接口统计信息(成功、失败次数)
    :return: 获取接口统计信息
    """
    body = await get_request_body(request)
    publish_id = body["publish_id"]
    start_time = body["start_time"]
    end_time = body["end_time"]
    return JSONResponse(make_json(response_result(data=get_status_count(start_time, end_time, publish_id))))


@router.post("/audit")
@valid_exist_user_id
async def audit(request: Request):
    """
    获取接口统计信息(成功、失败次数)
    :return: 获取接口统计信息
    """
    body = await get_request_body(request)
    publish_id = body["publish_id"]
    return JSONResponse(make_json(response_result(data=get_audit(publish_id))))


@router.post("/generate_private_key")
@valid_exist_user_id
async def generate_private_key(request: Request):
    """
    生成密钥
    :return: 生成成功/失败
    """
    body = await get_request_body(request)
    publish_id = body["publish_id"]
    secret_name = body["secret_name"]
    description = body.get("description")
    user_id = get_user_id_from_request(request)
    generate_result = generate_key(publish_id, user_id, secret_name, description)
    if generate_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "密钥生成失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/private_key_list")
@valid_exist_user_id
async def private_key_list(request: Request):
    """
    获取密钥列表
    :return: 密钥列表
    """
    body = await get_request_body(request)
    publish_id = body["publish_id"]
    return JSONResponse(make_json(response_result(data=get_secret_by_publish_id(publish_id))))


@router.post("/delete_private_key")
@valid_exist_user_id
async def delete_private_key(request: Request):
    """
    删除密钥
    :return: 删除成功/失败
    """
    body = await get_request_body(request)
    private_id = body["private_id"]
    delete_result = delete_secret_by_id(private_id)
    if delete_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "密钥删除失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/change_publish_information")
@valid_exist_user_id
async def change_publish_information(request: Request):
    """
    更新发布信息
    :return: 更新成功/失败
    """
    body = await get_request_body(request)
    publish_id = body["publish_id"]
    publish_name = body["publish_name"]
    description = body["description"]
    change_result = change_publish_name_and_description(publish_id, publish_name, description)
    if change_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "发布信息修改失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/version_name_change")
@valid_exist_user_id
async def version_name_change(request: Request):
    """
    编辑版本名称
    :return: 成功失败
    """
    body = await get_request_body(request)
    version_id, _ = get_version_id_and_element_id_from_body(body)
    pipelining_id = body.get("pipelining_id")
    version_name = body.get("version_name")
    if not version_id or not pipelining_id or not version_name:
        raise EmptyParameterValueError(["version_id", "pipelining_id", "version_name"]) from None
    edit_result = edit_version_name(pipelining_id, version_id, version_name)
    if edit_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "编辑失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/all_publish_list")
async def get_all_publish_list(request: Request):
    """
    获取全部发布服务列表
    :return: 服务列表
    """
    body = await get_request_body(request)
    experiment_id = body.get("experiment_id")
    return JSONResponse(make_json(response_result(data=all_publish_list(experiment_id))))


@router.post("/client_list")
@valid_exist_user_id
async def get_client_list(request: Request):
    """
    获取客户端列表
    :return: 客户端列表
    """
    UNUSED(request)
    return JSONResponse(make_json(response_result(data=get_all_client())))


@router.post("/client_id_associate_publish_id_list")
@valid_exist_user_id
async def client_id_associate_publish_id_list(request: Request):
    """
    客户端标识与发布列表关联
    :return: 成功失败
    """
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    experiment_id = body.get("experiment_id")
    publish_id_list = body.get("publish_id_list")
    client_id = body.get("client_id")
    if not client_id or not publish_id_list:
        raise EmptyParameterValueError(["client_id", "publish_id_list"]) from None
    associate_result = associate_client_with_publish_id_list(user_id, client_id, experiment_id, publish_id_list)
    if associate_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "关联失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/client_publish_pipelining_list")
@valid_exist_user_id
async def client_publish_pipelining_list(request: Request):
    """
    与客户端关联的发布列表
    :return: 列表
    """
    UNUSED(request)
    client_list = get_client_publish_list()
    result_dict = defaultdict(list)

    for item in client_list:
        result_dict[item["client_name"]].append(item)
    result_list = [
        {
            "client_name": k,
            "create_user": v[0]["user_name"],  # 创建时，至少有一个发布的流水线，所以取第 0 个没问题
            "create_time": v[0]["create_time"],
            "space_name": v[0]["space_name"],
            "experiment_id": v[0]["experiment_id"],
            "publish_list": v,
        }
        for k, v in result_dict.items()
    ]
    return JSONResponse(make_json(response_result(data=result_list)))


@router.post("/delete_client_associate_publish_id_list")
@valid_exist_user_id
async def delete_client_associate_publish_id_list(request: Request):
    """
    删除客户端关联的发布列表
    :return: 成功失败
    """
    body = await get_request_body(request)
    client_id = body.get("client_id")
    if not client_id:
        raise EmptyParameterValueError(["client_id"]) from None
    delete_result = delete_client_publish_list(client_id)
    if delete_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "删除失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/app_register")
@valid_exist_user_id
async def app_register(request: Request):
    """
    应用注册
    :return: 注册结果
    """
    body = await get_request_body(request)
    app_name = body.get("app_name")
    app_key = body.get("app_key")
    app_secret = body.get("app_secret")
    user_id = get_user_id_from_request(request)
    if not app_name or not app_key or not app_secret:
        raise EmptyParameterValueError(["version_id", "app_name", "app_key", "app_secret"]) from None
    register_result = register_app(user_id, app_name, app_key, app_secret)
    if register_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "注册应用失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/experiment_space_list")
@valid_exist_user_id
async def experiment_space_list(request: Request):
    """
    获取实验空间列表
    :return: 注册结果
    """
    user_id = get_user_id_from_request(request)
    space_list = get_experiment_space_list(user_id)
    return JSONResponse(make_json(response_result(data=space_list)))


@router.post("/add_experiment_space")
@valid_exist_user_id
async def add_experiment_space(request: Request):
    """
    新增实验空间
    :return: 新增结果
    """
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    space_name = body.get("space_name", "")
    space_description = body.get("space_description", "")
    placeholder = body.get("placeholder", "")
    result = save_experiment_space(user_id, space_name, space_description, placeholder)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "新增空间失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/edit_experiment_space")
@valid_exist_user_id
async def edit_experiment_space(request: Request):
    """
    编辑实验空间
    :return: 编辑结果
    """
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    space_id = body.get("space_id")
    space_name = body.get("space_name", "")
    space_description = body.get("space_description", "")
    result = save_experiment_space_with_id(user_id, space_id, space_name, space_description)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "编辑空间失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/delete_experiment_space")
@valid_exist_user_id
async def delete_experiment_space(request: Request):
    """
    删除实验空间
    :return: 删除结果
    """
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    space_id = body.get("space_id")
    result = delete_experiment_space_with_id(user_id, space_id)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "删除空间失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/create_pipelining_element")
@valid_exist_user_id
async def create_pipelining_element(request: Request):
    """
    创建流水线为算子
    :return: 创建结果
    """
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    name = body["name"]
    description = body.get("description", "")
    origin_version_id = body["origin_version_id"]
    origin_pipelining_id = body["origin_pipelining_id"]

    copy_result = await asyncify(copy_pipelining_version)(origin_pipelining_id, origin_version_id, default_delete=True)
    if copy_result.code != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "创建管道算子失败") from None
    new_version_id = copy_result.data["new_version_id"]
    result = create_new_pipelining_element(user_id, name, description, new_version_id, origin_pipelining_id)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "创建管道算子失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/delete_pipelining_element")
@valid_exist_user_id_from_blade_auth
async def delete_pipelining_element(blade_auth: Annotated[str | None, Header()], pipelining_element: PipeliningElement):
    """
    创建流水线为算子
    :return: 创建结果
    """
    user_id = get_user_id_from_blade_auth(blade_auth)
    pipelining_element_id = pipelining_element.id
    result = delete_pipelining_element_by_id(pipelining_element_id, user_id)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "删除管道算子失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/register_pipelining_element")
@valid_exist_user_id
async def register_pipelining_element(request: Request):
    """
    注册流水线为管道算子
    :return: 注册结果
    """
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    pipelining_element_id = body["pipelining_element_id"]
    result = register_pipelining_element_by_id(user_id, pipelining_element_id)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "注册管道算子失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/unregister_pipelining_element")
@valid_exist_user_id
async def unregister_pipelining_element(request: Request):
    """
    取消注册流水线为管道算子
    :return: 取消注册结果
    """
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    pipelining_element_id = body["pipelining_element_id"]
    result = unregister_pipelining_element_by_id(user_id, pipelining_element_id)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "取消注册管道算子失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/registered_pipelining_element_list")
@valid_exist_user_id
async def registered_pipelining_element_list(request: Request):
    """
    注册管道仓库列表
    :return: 列表
    """
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    name = body.get("name")
    current = body.get("current", 1)
    size = body.get("size", 10)
    result = get_registered_pipelining_element_list(current, size, user_id, name)
    return JSONResponse(make_json(response_result(data=make_json(result))))


@router.post("/pipelining_element_list")
@valid_exist_user_id
async def pipelining_element_list(request: Request):
    """
    个人管道仓库列表
    :return: 列表
    """
    user_id = get_user_id_from_request(request)
    body = await get_request_body(request)
    name = body.get("name")
    current = body.get("current", 1)
    size = body.get("size", 10)
    result = get_pipelining_element_list(current, size, user_id, name)
    return JSONResponse(make_json(response_result(data=make_json(result))))


@router.post("/get_element_tree")
@valid_exist_user_id
async def element_tree(request: Request):
    """
    获取算子（算子列表+算子分类）树
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    UNUSED(body)  # 未来可能根据不同用户返回不同算子列表
    execute_result = get_element_tree()
    if execute_result.code != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, execute_result.message) from None
    return JSONResponse(make_json(response_result(data=execute_result.data)))


@router.post("/pipelining_and_version_tree")
@valid_exist_user_id
async def pipelining_and_version_tree(request: Request):
    """
    获取流水线+版本列表树
    :return: 列表树
    """
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    current = body.get("current")
    size = body.get("size")
    pipelining_name = body.get("pipelining_name")
    experiment_id = body["experiment_id"]
    pipelining_arr = await asyncify(get_pipelining_and_version_tree)(
        current, size, user_id, experiment_id, pipelining_name
    )

    return JSONResponse(make_json(response_result(data=make_json(pipelining_arr))))


@router.post("/delete_version_multi")
@valid_exist_user_id
async def delete_version_multi(request: Request):
    """
    批量删除流水线
    :return: 成功失败
    """
    body = await get_request_body(request)
    # version_id, _ = get_version_id_and_element_id_from_body(body)
    version_ids = body["version_ids"]
    if not version_ids:
        raise EmptyParameterValueError(["version_ids"]) from None
    delete_result = delete_pipelining_version_multi(version_ids)
    if delete_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "删除版本失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/generate_data_model_presigned_url")
async def generate_data_model_presigned_url(request: Request):
    """
    生成数据模型预下载链接
    :return: 链接 URL
    """
    body = await get_request_body(request)
    table_id = body["table_id"]
    # 通过 table_id 获取对应数据并创建成 excel 文件上传 minio, 返回对象标识
    key = create_excel_file_by_table(table_id)
    # 根据对象标识获取该对象的临时下载链接
    url = oss_helper1.generate_s3_presigned_url_for_excel(key)
    if not url:
        raise ExecuteError(sys._getframe().f_code.co_name, "生成临时下载链接失败") from None
    return JSONResponse(make_json(response_result(data=url)))


@router.post("/generate_model_presigned_url")
async def generate_model_presigned_url(request: Request):
    """
    生成模型预下载链接
    :return: 链接 URL
    """
    body = await get_request_body(request)
    key = body["file_id"]
    # 根据对象标识获取该对象的临时下载链接
    url = oss_helper1.generate_s3_presigned_url_for_model(key)
    if not url:
        raise ExecuteError(sys._getframe().f_code.co_name, "生成临时下载链接失败") from None
    return JSONResponse(make_json(response_result(data=url)))


@router.post("/upload_model_file")
async def upload_model_file(request: Request):
    """
    上传模型文件到对象存储
    :return: key
    """
    async with request.form() as form:
        file = form["file"]
        file_name = file.filename
        content = await file.read()

        # 根据对象标识获取该对象的临时下载链接
        key = oss_helper1.upload_model_to_s3(content, file_name)
        if not key:
            raise ExecuteError(sys._getframe().f_code.co_name, "保存模型文件失败") from None
        return JSONResponse(make_json(response_result(data=key)))


@router.post("/data_model_fields")
async def data_model_fields(data_model: GeneralDataModel):
    """
    根据数据模型标识获取该模型的所有字段
    :return:
    """
    # noinspection PyBroadException
    table_name = data_model.table_name
    datasource_id = data_model.datasource_id
    if not datasource_id or not table_name:
        raise EmptyParameterValueError(["datasource_id, table_name"]) from None
    result = get_sample_data_fields_from_datasource(datasource_id, table_name)
    return JSONResponse(make_json(response_result(data=result)))


@router.post("/data_model_samples")
async def data_model_samples(data_model: GeneralDataModel):
    """
    根据数据模型标识获取该模型的样本展示数据
    :return:
    """
    # noinspection PyBroadException
    table_name = data_model.table_name
    datasource_id = data_model.datasource_id
    if not datasource_id or not table_name:
        raise EmptyParameterValueError(["datasource_id, table_name"]) from None
    result = get_sample_data_from_datasource(datasource_id, table_name)
    return JSONResponse(make_json(response_result(data=result)))
