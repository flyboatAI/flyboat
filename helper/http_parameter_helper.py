from fastapi import Request

from helper.auth_helper import get_user_id


async def get_request_body(request: Request):
    """
    根据 FastAPI request 对象获取请求体
    :param request: FastAPI Request
    :return: body
    """
    if not request or not await request.body():
        return {}
    return await request.json()


def get_version_id_and_element_id_from_body(body: dict):
    """
    获取 body 里的统一参数
    :param body: body
    :return: version_id, element_id, user_id 元组
    """
    version_id = body.get("version_id", None)
    element_id = body.get("element_id", None)

    return version_id, element_id


async def get_version_id_and_element_id_from_request(request: Request):
    """
    根据 FastAPI request 对象获取请求体
    :param request: FastAPI Request
    :return: (version_id, element_id)
    """
    body = await get_request_body(request)
    version_id = body.get("version_id", None)
    element_id = body.get("element_id", None)

    return version_id, element_id


def get_user_id_from_request(request: Request):
    if not request:
        return None
    blade_auth = request.headers.get("blade-auth")
    user_id = get_user_id(blade_auth)
    return user_id


def get_user_id_from_blade_auth(blade_auth: str):
    if not blade_auth:
        return None
    user_id = get_user_id(blade_auth)
    return user_id


def get_app_key_from_request(request: Request):
    if not request:
        return None
    app_key = request.headers.get("app_key")
    return app_key


def get_app_token_from_request(request: Request):
    if not request:
        return None
    token = request.headers.get("client_token")
    return token


def get_app_timestamp_from_request(request: Request):
    if not request:
        return None
    client_timestamp = request.headers.get("client_timestamp")
    return client_timestamp


def get_publish_id_from_request(request: Request):
    if not request:
        return None
    client_timestamp = request.headers.get("client_timestamp")
    return client_timestamp
