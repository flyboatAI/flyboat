import math
from datetime import date, datetime

from enum_type.response_code import ResponseCode
from helper.result_helper import ExecuteResult


def make_json(json_object):
    __dict = json_object.__dict__
    return make_obj_can_json_serializable(__dict)


def make_obj_can_json_serializable(obj):
    if isinstance(obj, dict):
        return {k: make_obj_can_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_obj_can_json_serializable(item) for item in obj]
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return str(obj)
    elif isinstance(obj, bytes):
        return obj.decode("utf-8")
    return obj


def response_result(code=ResponseCode.Success.value, message="", data=None):
    """
    Http Response
    :param code: response code
    :param message: response message
    :param data: response data
    :return: response
    """
    return ExecuteResult(code, message, data)


def response_error_result(code=ResponseCode.Error.value, message="", data=None):
    """
    Http Error Response
    :param code: response code
    :param message: response message
    :param data: response data
    :return: response
    """
    return response_result(code, message, data)
