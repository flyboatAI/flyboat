from typing import Annotated

from fastapi import APIRouter, Header
from fastapi.responses import JSONResponse

from error.empty_parameter_value_error import EmptyParameterValueError
from helper.response_result_helper import make_json, response_result
from helper.warning_helper import UNUSED
from helper.wrapper_helper import valid_exist_user_id_from_blade_auth
from parameter_entity.publish.publish import Publish
from publish.pipelining_publish import get_version
from service.publish.publish_service import get_sync_input_parameter_by_version, get_sync_output_parameter_by_version

# 配置流水线数据 Controller
router = APIRouter()


@router.post("/sync_input_parameter")
@valid_exist_user_id_from_blade_auth
async def sync_input_parameter(blade_auth: Annotated[str | None, Header()], publish: Publish):
    """
    获取同步输入参数, 展示在发布管理中
    :return: response_result
    """
    UNUSED(blade_auth)
    version_id = publish.version_id
    if not version_id:
        raise EmptyParameterValueError(["version_id"]) from None
    data = get_sync_input_parameter_by_version(version_id)
    return JSONResponse(make_json(response_result(data=data)))


@router.post("/sync_input_parameter_by_publish_id")
@valid_exist_user_id_from_blade_auth
async def sync_input_parameter_by_publish_id(blade_auth: Annotated[str | None, Header()], publish: Publish):
    """
    获取同步输入参数, 展示在发布管理中
    :return: response_result
    """
    UNUSED(blade_auth)
    publish_id = publish.publish_id
    if not publish_id:
        raise EmptyParameterValueError(["publish_id"]) from None
    version_id = get_version(publish_id)
    data = get_sync_input_parameter_by_version(version_id)
    return JSONResponse(make_json(response_result(data=data)))


@router.post("/sync_output_parameter_by_publish_id")
@valid_exist_user_id_from_blade_auth
async def sync_output_parameter_by_publish_id(blade_auth: Annotated[str | None, Header()], publish: Publish):
    """
    获取同步输入参数, 展示在发布管理中
    :return: response_result
    """
    UNUSED(blade_auth)
    publish_id = publish.publish_id
    if not publish_id:
        raise EmptyParameterValueError(["publish_id"]) from None
    version_id = get_version(publish_id)
    data = get_sync_output_parameter_by_version(version_id)
    return JSONResponse(make_json(response_result(data=data)))
