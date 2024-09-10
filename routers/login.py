import sys

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from helper.response_result_helper import make_json, response_result
from service.login.login_service import create_demo_sample_data_and_pipelining

# 登录 Controller
router = APIRouter()


@router.post("/create_demo/{user_id}")
async def create_demo(user_id: str):
    """
    新建用户时, 创建样例数据和样例流水线
    :return: response_result
    """
    result = create_demo_sample_data_and_pipelining(user_id)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "创建样例数据失败") from None
    return JSONResponse(make_json(response_result()))
