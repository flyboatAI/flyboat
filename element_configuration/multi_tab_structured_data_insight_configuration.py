from helper.warning_helper import UNUSED
from insight.insight_element_data import insight_multi_tab_structured_data


def insight_element(version_id, element_id, node_type, process_id):
    """
    洞察数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param node_type: 节点类型
    :param process_id: 过程表示
    :return: 洞察数据
    """
    UNUSED(node_type)
    return insight_multi_tab_structured_data(process_id, version_id, element_id)
