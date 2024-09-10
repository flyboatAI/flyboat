from typing import Annotated

from fastapi import APIRouter, Header
from fastapi.responses import JSONResponse

from helper.response_result_helper import make_json, response_result
from helper.warning_helper import UNUSED
from helper.wrapper_helper import valid_exist_user_id_from_blade_auth
from license_service.serial_number import SerialNumber

# 初始化数据源 Controller
router = APIRouter()


@router.post("/get_user_info")
@valid_exist_user_id_from_blade_auth
async def get_user_info(blade_auth: Annotated[str | None, Header()]):
    """
    获取用户动态信息(授权过期时间、通知数量等)
    :return: response_result
    """
    UNUSED(blade_auth)
    exp = SerialNumber.get_exp_time_by_serial_number()
    return JSONResponse(make_json(response_result(data={"exp": exp})))
