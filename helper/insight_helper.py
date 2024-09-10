from config.dag_config import (
    EMPTY_MODULE,
    INSIGHT_ELEMENT_FUNC_NAME,
    INSIGHT_MODULE,
    element_info_dict,
)
from error.insight_error import InsightError
from helper.result_helper import execute_error, execute_success


def insight_element(version_id, element_id, node_type, process_id):
    """
    初始化通用算子
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param node_type: 节点类型
    :param process_id: 执行过程标识
    :return: response_result
    """
    # noinspection PyBroadException
    try:
        if not node_type or node_type not in element_info_dict:
            return execute_error()
        element = element_info_dict.get(node_type, None)
        if not element:
            return execute_error()
        module = element.get(INSIGHT_MODULE, None)
        if module is None:
            return execute_error()
        if module == EMPTY_MODULE:
            return execute_success()
        # noinspection PyTypeChecker
        module = __import__(module, fromlist=True)
        if not hasattr(module, INSIGHT_ELEMENT_FUNC_NAME):
            return execute_error()
        insight_element_func = getattr(module, INSIGHT_ELEMENT_FUNC_NAME)
        return insight_element_func(version_id, element_id, node_type, process_id)
    except Exception as e:
        raise InsightError(f"{e}") from None
