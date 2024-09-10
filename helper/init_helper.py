import sys

from config.dag_config import (
    DELETE_ELEMENT_FUNC_NAME,
    DELETE_MODULE,
    EMPTY_MODULE,
    INIT_ELEMENT_FUNC_NAME,
    INIT_MODULE,
    element_info_dict,
)
from enum_type.result_code import ResultCode
from error.delete_error import DeleteError
from error.store_error import StoreError
from helper.response_result_helper import response_error_result, response_result


def general_init(version_id, element_id, user_id, node_type, **kwargs):
    """
    初始化通用算子
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param node_type: 节点类型
    :return: response_result
    """
    # noinspection PyBroadException
    try:
        if not node_type or node_type not in element_info_dict:
            return response_error_result()
        element = element_info_dict.get(node_type, None)
        if not element:
            return response_error_result()
        module = element.get(INIT_MODULE, None)
        if module is None:
            return response_error_result()
        if module == EMPTY_MODULE:
            return response_result()
        # noinspection PyTypeChecker
        module = __import__(module, fromlist=True)
        if not hasattr(module, INIT_ELEMENT_FUNC_NAME):
            return response_error_result()
        init_element = getattr(module, INIT_ELEMENT_FUNC_NAME)

        store_result = init_element(version_id, element_id, user_id, node_type, **kwargs)

        if store_result != ResultCode.Success.value:
            # noinspection PyProtectedMember
            raise StoreError(sys._getframe().f_code.co_name, user_id, "数据库保存配置失败") from None
        return response_result()
    except Exception as e:
        raise StoreError(sys._getframe().f_code.co_name, user_id, f"{e}") from None


def general_delete(version_id, element_id, user_id, node_type):
    """
    删除算子操作
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param node_type: 节点类型
    :return: response_result
    """
    try:
        if not node_type or node_type not in element_info_dict:
            return response_error_result()
        element = element_info_dict.get(node_type, None)
        if not element:
            return response_error_result()
        module = element.get(DELETE_MODULE, None)
        if module is None:
            return response_error_result()
        if module == EMPTY_MODULE:
            return response_result()
        # noinspection PyTypeChecker
        module = __import__(module, fromlist=True)
        if not hasattr(module, DELETE_ELEMENT_FUNC_NAME):
            return response_error_result()
        delete_element = getattr(module, DELETE_ELEMENT_FUNC_NAME)
        delete_result = delete_element(version_id, element_id, node_type)
        if delete_result != ResultCode.Success.value:
            # noinspection PyProtectedMember
            raise DeleteError(sys._getframe().f_code.co_name, user_id, "删除失败") from None
        return response_result()
    except Exception as e:
        raise DeleteError(sys._getframe().f_code.co_name, user_id, f"{e}") from None
