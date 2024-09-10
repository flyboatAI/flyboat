import asyncio
import functools

from fastapi import Request

from config import setting
from config.dag_config import element_info_dict
from error.init_error import InitError
from error.no_such_user_error import NoSuchUserError
from error.valid_token_error import ValidTokenError
from helper.http_parameter_helper import (
    get_app_key_from_request,
    get_app_timestamp_from_request,
    get_app_token_from_request,
    get_publish_id_from_request,
    get_user_id_from_blade_auth,
    get_user_id_from_request,
)
from helper.token_helper import valid_token

INIT_NODE_TYPE_POSITION = 3
DELETE_NODE_TYPE_POSITION = 2


def valid_delete_sql(func):
    """
    装饰器统一处理 init_element 的 SQL 校验
    :param func: 原始函数
    :return: 原始返回值
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        node_type = args[DELETE_NODE_TYPE_POSITION]
        element = element_info_dict.get(node_type, None)
        if not element:
            raise InitError(f"未找到 {node_type} 类型对应的算子") from None
        return func(*args, **kwargs)

    return wrapper


def valid_init_sql(func):
    """
    装饰器统一处理 init_element 的 SQL 校验
    :param func: 原始函数
    :return: 原始返回值
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        node_type = args[INIT_NODE_TYPE_POSITION]
        element = element_info_dict.get(node_type, None)
        if not element:
            raise InitError(f"未找到 {node_type} 类型对应的算子") from None
        return func(*args, **kwargs)

    return wrapper


def valid_exist_user_id_from_blade_auth(func):
    @functools.wraps(func)
    async def async_wrapper(blade_auth: str, *args, **kwargs):
        user_id = get_user_id_from_blade_auth(blade_auth)
        if not user_id:
            raise NoSuchUserError("无法解析用户标识, 鉴权失败") from None
        return await func(blade_auth, *args, **kwargs)

    @functools.wraps(func)
    def wrapper(blade_auth: str, *args, **kwargs):
        user_id = get_user_id_from_blade_auth(blade_auth)
        if not user_id:
            raise NoSuchUserError("无法解析用户标识, 鉴权失败") from None
        return func(blade_auth, *args, **kwargs)

    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    return wrapper


def valid_exist_user_id(func):
    """
    装饰器统一处理 Controller 层用户校验
    :param func: 原始函数
    :return: 原始返回值
    """

    @functools.wraps(func)
    async def async_wrapper(request: Request, *args, **kwargs):
        user_id = get_user_id_from_request(request)
        if not user_id:
            raise NoSuchUserError("未传递用户标识, 鉴权失败") from None
        return await func(request, *args, **kwargs)

    @functools.wraps(func)
    def wrapper(request: Request, *args, **kwargs):
        user_id = get_user_id_from_request(request)
        if not user_id:
            raise NoSuchUserError("未传递用户标识, 鉴权失败") from None
        return func(request, *args, **kwargs)

    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    return wrapper


def valid_token_by_header(func):
    """
    装饰器统一校验 Controller 层 API 调用权限
    :param func: 原始函数
    :return: 原始返回值
    """

    @functools.wraps(func)
    async def async_wrapper(request: Request, *args, **kwargs):
        # 鉴权
        app_key = get_app_key_from_request(request)
        token = get_app_token_from_request(request)
        client_timestamp = get_app_timestamp_from_request(request)

        if not app_key:
            raise ValidTokenError("未传递 app_key 字段, 请检查接口详情") from None
        publish_id = get_publish_id_from_request(request)

        if app_key != setting.ZMS_MAGIC and not valid_token(token, app_key, client_timestamp, publish_id):
            raise ValidTokenError(f"传递的 app_key:{app_key} 校验失败, 请检查接口详情") from None

        return await func(request, *args, **kwargs)

    @functools.wraps(func)
    def wrapper(request: Request, *args, **kwargs):
        user_id = get_user_id_from_request(request)
        if not user_id:
            raise NoSuchUserError("未传递用户标识, 鉴权失败") from None
        return func(request, *args, **kwargs)

    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    return wrapper
