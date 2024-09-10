import json

from config.dag_config import SYNC_INPUT
from enum_type.result_code import ResultCode
from helper.oss_helper.oss_helper import oss_helper1
from helper.sql_helper.init_sql_helper import db_helper1

DATA_FIELDS_ROLE_SCALER_ARR_NUMBER = 4
MODEL_ARR_NUMBER = 1
DEPENDENCY_ID_ARR_NUMBER = 1


class ContainerBuild:
    def __init__(self, code, container):
        """
        初始化方法
        :param code: 结果码
        :param container: 容器
        """
        self.code = code
        self.container = container
        pass


def topological_sort(dag_config):
    """
    根据配置生成 DAG
    配置示例:
    [
      { "type": "sync_input", "id": "1", "dependency": [] },
      {
        "type": "linear_regression_algorithm",
        "id": "2",
        "dependency": [{
          "id": "1",
          "source_port": 0,
          "dist_port": 0,
          "type": "data"
        }]
      },
      {
        "type": "model_file_output",
        "id": "3",
        "dependency": [{
          "id": "2",
          "source_port": 0,
          "dist_port": 0,
          "type": "model"
        }]
      }
    ]
    :param dag_config: 配置
    :return: 执行顺序id数组
    """
    g = {}
    for o in dag_config:
        g[o["id"]] = [x["id"] for x in o["dependency"]]
    # 创建入度字典
    in_degrees = dict((u, 0) for u in g)
    # 获取每个节点的入度
    for u in g:
        for v in g[u]:
            in_degrees[v] += 1
    # 使用列表作为队列并将入度为0的添加到队列中
    q = [u for u in g if in_degrees[u] == 0]
    res = []
    # 当队列中有元素时执行
    while q:
        # 从队首取出元素
        u = q.pop(0)
        # 将取出的元素存入结果中
        res.append(u)
        # 移除与取出元素相关的指向, 即将所有与取出元素相关的元素的入度减少1
        for v in g[u]:
            in_degrees[v] -= 1
            # 若被移除指向的元素入度为0, 则添加到队列中
            if in_degrees[v] == 0:
                q.append(v)
    res.reverse()
    return res


def dependency_nodes(node_id, dag_config):
    """
    获取节点 id 为 node_id 的依赖节点, 组合为数据列表和模型列表元组返回
    示例:
        算子1                  算子2
        +--------+            +--------+
        |        |            |        |
        |        0-----+      0        |
        |        |     |      |        |
        |        1     +----->1        |
        |        |            |        |
        +--------+            +--------+
    dag_data_dict:
    {
        1:
            {
                "source_id": "node_1_id",
                "source_port": 0
            }
    }
    :param node_id: 节点id
    :param dag_config: 配置
    :return: 数据字典和模型字典元组
    """
    for o in dag_config:
        if node_id == o["id"]:
            dag_data_dict = {
                x["dist_port"]: {"source_id": x["id"], "source_port": x["source_port"]}
                for x in o["dependency"]
                if x["type"] == "data"
            }
            dag_model_dict = {
                x["dist_port"]: {"source_id": x["id"], "source_port": x["source_port"]}
                for x in o["dependency"]
                if x["type"] == "model"
            }
            return dag_data_dict, dag_model_dict
    return ()


def container_init(index_arr, n):
    """
    初始化 None 容器
    :param index_arr: 目的端口号数组
    :param n: 容器个数
    :return: n 个 None 数组组成的元组
    """
    # 初始化默认大小的数组
    if index_arr:
        none_arr = [None] * (index_arr[-1] + 1)
    else:
        none_arr = []
    container_arr = []
    for _ in range(n):
        n_a = none_arr[:]
        container_arr.append(n_a)
    return tuple(container_arr)


def dependency_container_build(node):
    """
    组装依赖的算子 id 数组
    :param node: 当前节点
    :return: 算子 id 数组
    """
    dependency = node["dependency"]
    if not dependency:
        return ContainerBuild(ResultCode.Success.value, [])
    port_id_dict = {x["dist_port"]: x["id"] for x in dependency}

    index_arr = list(port_id_dict.keys())
    # 存储的是算子端口号, 默认起始位置为 0
    index_arr.sort()
    # 根据最大目的端口号, 初始化数组
    (dependency_id_arr,) = container_init(index_arr, DEPENDENCY_ID_ARR_NUMBER)
    for index in index_arr:
        dependency_id = port_id_dict[index]
        dependency_id_arr[index] = dependency_id

    return ContainerBuild(ResultCode.Success.value, dependency_id_arr)


def data_container_build(dag_data_dict, dag_dict, node_type, sync_input_data):
    """
    组装数据数组、字段数组、角色数组
    :param dag_data_dict: 根据配置文件解析的依赖的数据字典
    :param dag_dict: 内存中保存的算子执行结果
    :param node_type: 节点类型, 用于特殊处理同步输入节点
    :param sync_input_data: 同步输入节点输入的数据
    :return: 组装好的数据数组、字段数组、角色数组
    """
    if node_type == SYNC_INPUT:
        return ContainerBuild(
            ResultCode.Success.value,
            ([sync_input_data],) + tuple([] for _ in range(DATA_FIELDS_ROLE_SCALER_ARR_NUMBER - 1)),
        )

    if not dag_data_dict or not dag_dict:
        return ContainerBuild(
            ResultCode.Success.value,
            tuple([] for _ in range(DATA_FIELDS_ROLE_SCALER_ARR_NUMBER)),
        )

    index_arr = list(dag_data_dict.keys())
    # 存储的是算子端口号, 默认起始位置为 0
    index_arr.sort()
    # 根据最大目的端口号, 初始化数组
    data_arr, field_arr, role_arr, scaler_arr = container_init(index_arr, DATA_FIELDS_ROLE_SCALER_ARR_NUMBER)

    # 从内存数据字典将依赖的 data、field、role, scaler 数组拼接好
    for index in index_arr:
        element_dag_dict_result = get_element_dag_dict(dag_data_dict, index, dag_dict)
        if element_dag_dict_result.code != ResultCode.Success.value:
            return ContainerBuild(ResultCode.Error.value, ())
        element_dag_tuple, source_element_port = element_dag_dict_result.container
        if not element_dag_tuple or len(element_dag_tuple) != 5:
            return ContainerBuild(ResultCode.Error.value, ())
        element_data, element_field, element_role, _, element_scaler = element_dag_tuple
        if not element_data:
            return ContainerBuild(ResultCode.Error.value, ())
        # 从源算子的某一端口输出的数据
        source_port_data = element_data[source_element_port]
        data_arr[index] = source_port_data

        # 组装 field_arr
        if element_field and len(element_field) > source_element_port:
            source_port_field = element_field[source_element_port]
            field_arr[index] = source_port_field

        # 组装 role_arr
        if element_role and len(element_role) > source_element_port:
            source_port_role = element_role[source_element_port]
            role_arr[index] = source_port_role

        # 组装 scaler_arr
        if element_scaler and len(element_scaler) > source_element_port:
            source_port_scaler = element_scaler[source_element_port]
            scaler_arr[index] = source_port_scaler
    return ContainerBuild(ResultCode.Success.value, (data_arr, field_arr, role_arr, scaler_arr))


def model_container_build(dag_model_dict, dag_dict):
    """
    组装模型数组
    :param dag_model_dict: 根据配置文件解析的依赖的模型字典
    :param dag_dict: 内存中保存的算子执行结果 {"node_id": ([], [], [], [], [])}
    :return: 组装好的模型数组
    """
    if not dag_model_dict or not dag_dict:
        return ContainerBuild(ResultCode.Success.value, (tuple([] for _ in range(MODEL_ARR_NUMBER))))

    index_arr = list(dag_model_dict.keys())
    # 存储的是算子端口号, 默认起始位置为 0
    index_arr.sort()
    (model_arr,) = container_init(index_arr, MODEL_ARR_NUMBER)
    # 从内存数据字典将依赖的 model 数组拼接好
    for index in index_arr:
        element_dag_dict_result = get_element_dag_dict(dag_model_dict, index, dag_dict)
        if element_dag_dict_result.code != ResultCode.Success.value:
            return ContainerBuild(ResultCode.Error.value, ())
        element_dag_tuple, source_element_port = element_dag_dict_result.container
        if not element_dag_tuple or len(element_dag_tuple) != 5:
            return ContainerBuild(ResultCode.Error.value, ())
        _, _, role_settings, element_model, _ = element_dag_tuple
        if not element_model:
            return ContainerBuild(ResultCode.Error.value, ())
        # 从源算子的某一端口输出的模型
        source_port_model = element_model[source_element_port]
        # 组装模型数组
        model_arr[index] = source_port_model
    return ContainerBuild(ResultCode.Success.value, (model_arr,))


def get_element_dag_dict(dag_data_model_dict, index, dag_dict):
    """
    获取 index port 上连接的上级算子的内存数据集合
    :param dag_data_model_dict: 格式
    {
        1:
            {
                "source_id": "node_1_id",
                "source_port": 0
            }
    }
    :param index: port
    :param dag_dict: 已运行的内存数据集合, 格式
    {
        "node_id": ([], [], [], [], [])
    }
    :return: 数据、port 元组
    """
    element_id_port_dict = dag_data_model_dict.get(index)
    if element_id_port_dict is None:
        return ContainerBuild(ResultCode.Error.value, ())
    # 获取与目的端口相连接源端口算子 id 和端口号
    source_element_id = element_id_port_dict.get("source_id")
    source_element_port = element_id_port_dict.get("source_port")
    if source_element_port is None or source_element_id is None:
        return ContainerBuild(ResultCode.Error.value, ())
    # 在内存数据字典中获取到该源算子 id 对应的模型信息
    element_dag_tuple = dag_dict.get(source_element_id, None)
    if not isinstance(element_dag_tuple, tuple) and element_dag_tuple is None:
        return ContainerBuild(ResultCode.Error.value, ())
    return ContainerBuild(ResultCode.Success.value, (element_dag_tuple, source_element_port))


def get_dag(version_id):
    """
    获取该版本的 DAG 数据
    :param version_id: 版本标识
    :return: DAG 数据
    """
    pipelining_config_sql = f"select dag_config from ml_pipelining_version " f"where id='{version_id}'"
    pipelining_config_dict = db_helper1.fetchone(pipelining_config_sql, [])
    if not pipelining_config_dict or "dag_config" not in pipelining_config_dict:
        return None
    dag_config = pipelining_config_dict["dag_config"]
    if not dag_config:
        return None
    dag_config_str = oss_helper1.get_json_file_data(dag_config)
    # json_str -> arr
    dag_config_arr = json.loads(dag_config_str)
    return dag_config_arr


def valid_dag_exist_sync_input_element(dag_arr):
    """
    校验该流水线是否存在同步输入算子
    :param dag_arr: dag 配置
    :return: 是、否
    """
    return SYNC_INPUT in [element["type"] for element in dag_arr]
