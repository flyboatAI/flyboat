from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from helper.http_parameter_helper import (
    get_request_body,
    get_user_id_from_request,
    get_version_id_and_element_id_from_body,
)
from helper.init_helper import general_delete, general_init
from helper.response_result_helper import make_json
from helper.wrapper_helper import valid_exist_user_id

# 初始化算子数据 Controller
router = APIRouter()


@router.post("/delete_element")
@valid_exist_user_id
async def delete(request: Request):
    """
    删除算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    node_type = body["node_type"]
    return JSONResponse(make_json(general_delete(version_id, element_id, user_id, node_type)))


@router.post("/create_element")
@valid_exist_user_id
async def create(request: Request):
    """
    创建算子
    :return: response_result
    """
    # noinspection PyBroadException
    body = await get_request_body(request)
    user_id = get_user_id_from_request(request)
    version_id, element_id = get_version_id_and_element_id_from_body(body)
    node_type = body["node_type"]
    kwargs = {k: body[k] for k in body.keys() if k not in ["version_id", "element_id", "user_id", "node_type"]}
    return JSONResponse(make_json(general_init(version_id, element_id, user_id, node_type, **kwargs)))
