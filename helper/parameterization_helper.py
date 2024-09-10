from helper.dag_helper import get_dag, topological_sort
from helper.sql_helper.init_sql_helper import db_helper1


def get_algorithm_name_list(element_id_list):
    id_list_str = "','".join(element_id_list)
    sql = (
        f"select t2.algorithm_name "
        f"from ml_custom_algorithm_element t1 "
        f"left join ml_algorithm t2 "
        f"on t1.algorithm_id=t2.id "
        f"where t1.id in ('{id_list_str}')"
    )
    return db_helper1.fetchall(sql)


def get_element_ids(version_id, node_type):
    dag_arr = get_dag(version_id)
    if not dag_arr:
        return None
    dag_id_arr = topological_sort(dag_arr)
    element_ids = []
    for element_id in dag_id_arr:
        nodes = [x for x in dag_arr if x["id"] == element_id]
        if not nodes:
            return None
        # 获取要执行的算子节点, 根据执行顺序, 从全局 DAG 字典中该节点需要执行的算子处理函数
        node = nodes[0]
        current_node_type = node["type"]
        if node_type == current_node_type:
            element_ids.append(element_id)
    return element_ids


def get_element_id(version_id, node_type):
    dag_arr = get_dag(version_id)
    if not dag_arr:
        return None
    dag_id_arr = topological_sort(dag_arr)

    for element_id in dag_id_arr:
        nodes = [x for x in dag_arr if x["id"] == element_id]
        if not nodes:
            return None
        # 获取要执行的算子节点, 根据执行顺序, 从全局 DAG 字典中该节点需要执行的算子处理函数
        node = nodes[0]
        current_node_type = node["type"]
        if node_type == current_node_type:
            return element_id
    return None


def get_prev_node(version_id, element_id):
    dag_arr = get_dag(version_id)
    if not dag_arr:
        return None
    nodes = [x for x in dag_arr if x["id"] == element_id]
    if not nodes:
        return None
    # 获取要执行的算子节点, 根据执行顺序, 从全局 DAG 字典中该节点需要执行的算子处理函数
    node = nodes[0]
    # "dependency": [{
    #     "id": "1",
    #     "source_port": 0,
    #     "dist_port": 0,
    #     "type": "data"
    # }]
    dependency = node["dependency"]
    dependency_nodes = [
        dependency_node
        for dependency_node in dependency
        if dependency_node["type"] == "data" and dependency_node["dist_port"] == 0
    ]
    dependency_node = None
    if dependency_nodes:
        dependency_node = dependency_nodes[0]

    if dependency_node:
        prev_id = dependency_node["id"]
        prev_nodes = [x for x in dag_arr if x["id"] == prev_id]
        if not prev_nodes:
            return None
        # 获取要执行的算子节点, 根据执行顺序, 从全局 DAG 字典中该节点需要执行的算子处理函数
        prev_node = prev_nodes[0]
        if prev_node:
            return dependency_node["source_port"], prev_node["type"], prev_node["id"]
    return None, None, None
