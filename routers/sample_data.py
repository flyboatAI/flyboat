import sys
from typing import Annotated

from fastapi import APIRouter, Header
from fastapi.responses import JSONResponse

from core.pipelining_engine import machine_learning_execute_engine
from enum_type.response_code import ResponseCode
from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from error.general_error import GeneralError
from helper.dag_helper import get_dag
from helper.http_parameter_helper import get_user_id_from_blade_auth
from helper.response_result_helper import make_json, response_result
from helper.sample_data_generate_helper import generate_input_data
from helper.wrapper_helper import valid_exist_user_id_from_blade_auth
from license_service.serial_number import SerialNumber
from parameter_entity.sample_data.sample_data import SampleDataModel
from service.data_model.data_model_service import (
    get_split_sample_data,
    rewrite_split_sample_data,
)

# 样本数据相关接口 Controller
router = APIRouter()


@router.post("/split_2_or_3")
@valid_exist_user_id_from_blade_auth
async def split_2_or_3(blade_auth: Annotated[str | None, Header()], sample_data_model: SampleDataModel):
    """
    获取数据分割运行时的数据
    :return: response_result
    """
    user_id = get_user_id_from_blade_auth(blade_auth)
    version_id = sample_data_model.version_id
    element_id = sample_data_model.element_id
    # 如果存在配好的数据, 则直接显示配置好的
    split_sample_data = get_split_sample_data(version_id, element_id)
    if split_sample_data and split_sample_data["data"]:
        return JSONResponse(make_json(response_result(data=split_sample_data)))

    # 不存在则执行至该算子, 获取各个集合的数据展示
    sync_input_data = generate_input_data(version_id)
    serial_number = SerialNumber.get_serial_number()
    dag_arr = get_dag(version_id)
    try:
        result = await machine_learning_execute_engine(
            sync_input_data,
            version_id,
            user_id,
            None,
            dag_arr,
            None,
            websocket=None,
            shared_mem=None,
            dag_dict=None,
            child=False,
            to_element_id=element_id,
            serial_number=serial_number,
        )
    except Exception:
        raise GeneralError("前置存在未配置的算子, 请配置完毕再进行样本数据配置")
    if result.code != ResponseCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, result.message) from None
    sample_data = result.data["data"]
    sample_data_fields = result.data["fields"]
    data = {}
    fields = {}
    if len(sample_data) == 2 and len(sample_data_fields) == 2:
        data["train"] = sample_data[0]
        data["test"] = sample_data[1]
        fields["train"] = sample_data_fields[0]
        fields["test"] = sample_data_fields[1]
    elif len(sample_data) == 3 and len(sample_data_fields) == 3:
        data["train"] = sample_data[0]
        data["test"] = sample_data[1]
        data["valid"] = sample_data[2]
        fields["train"] = sample_data_fields[0]
        fields["test"] = sample_data_fields[1]
        fields["valid"] = sample_data_fields[2]

    return JSONResponse(make_json(response_result(data={"data": data, "fields": fields})))


@router.post("/config_split_2_or_3")
@valid_exist_user_id_from_blade_auth
def config_split_2_or_3(blade_auth: Annotated[str | None, Header()], sample_data_model: SampleDataModel):
    """
    配置数据分割运行时的数据
    :return: response_result
    """
    user_id = get_user_id_from_blade_auth(blade_auth)
    # 先删除, 再配置
    result = rewrite_split_sample_data(user_id, sample_data_model)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, result.message) from None
    return JSONResponse(make_json(response_result()))
