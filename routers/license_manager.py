import sys
from typing import Annotated

from fastapi import APIRouter, Header
from fastapi.responses import JSONResponse

from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from helper.http_parameter_helper import get_user_id_from_blade_auth
from helper.response_result_helper import make_json, response_result
from helper.warning_helper import UNUSED
from helper.wrapper_helper import valid_exist_user_id_from_blade_auth
from license_service.serial_number import SerialNumber
from parameter_entity.license.license_data import IdentifierAndExp, LicenseData
from service.user import user_service

# 初始化数据源 Controller
router = APIRouter()


@router.post("/get_pc_identifier")
@valid_exist_user_id_from_blade_auth
async def get_pc_identifier(blade_auth: Annotated[str | None, Header()]):
    """
    返回前端用户需要提供的本机唯一编号
    :return: response_result
    """
    UNUSED(blade_auth)
    pc_identifier = SerialNumber.get_hashed_machine_id()
    return JSONResponse(make_json(response_result(data=pc_identifier)))


@router.post("/upload_license_data")
@valid_exist_user_id_from_blade_auth
async def upload_license_data(blade_auth: Annotated[str | None, Header()], license_data: LicenseData):
    """
    上传序列号
    :return: response_result
    """
    serial_number = license_data.serial_number
    user_id = get_user_id_from_blade_auth(blade_auth)
    # 校验用户角色 administrator 才允许更新序列号
    is_administrator = user_service.get_role(user_id)
    if not is_administrator:
        raise ExecuteError(sys._getframe().f_code.co_name, "保存序列号失败, 只有管理员才允许修改序列号") from None
    result = SerialNumber.upload_serial_number(serial_number)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "保存序列号失败") from None
    return JSONResponse(make_json(response_result(data=serial_number)))


@router.post("/get_license_data")
@valid_exist_user_id_from_blade_auth
async def get_license_data(blade_auth: Annotated[str | None, Header()]):
    """
    获取序列号
    :return: response_result
    """
    UNUSED(blade_auth)
    serial_number = SerialNumber.get_serial_number()
    return JSONResponse(make_json(response_result(data=serial_number)))


@router.post("/generate_license_data")
@valid_exist_user_id_from_blade_auth
async def generate_license_data(blade_auth: Annotated[str | None, Header()], id_and_exp: IdentifierAndExp):
    """
    (内部使用)返回签名后的授权数据
    :return: response_result
    """
    UNUSED(blade_auth)
    key = id_and_exp.key
    exp = id_and_exp.exp
    sig = SerialNumber.sig_data(key, exp)
    serial_number = SerialNumber.license_data(key, exp, sig)
    return JSONResponse(make_json(response_result(data=serial_number)))


@router.post("/valid_license_data")
@valid_exist_user_id_from_blade_auth
async def valid_license_data(blade_auth: Annotated[str | None, Header()], license_data: LicenseData):
    """
    (内部使用)验证签名
    :return: response_result
    """
    UNUSED(blade_auth)
    serial_number = license_data.serial_number
    SerialNumber.valid_privkey_sign_data(serial_number)
    return JSONResponse(make_json(response_result()))
