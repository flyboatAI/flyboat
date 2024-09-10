from config.dag_config import ERROR_CONFIG_DAG_DICT_MAGIC, TABLE_NAME, element_info_dict
from enum_type.result_code import ResultCode
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED
from helper.wrapper_helper import valid_delete_sql


def init_element(version_id, element_id, user_id, node_type, **kwargs):
    """
    初始化算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param node_type: 节点类型
    :return: 配置成功/失败
    """
    UNUSED(version_id, element_id, user_id, node_type, kwargs)
    return ResultCode.Success.value


def generate_init_sql(version_id, element_id, user_id, node_type):
    """
    生成算子配置初始化的 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param node_type: 节点类型
    :return: SQL
    """
    UNUSED(version_id, element_id, user_id, node_type)
    return None


def delete_element(version_id, element_id, node_type):
    """
    删除算子操作
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param node_type: 节点类型
    :return: 删除结果
    """
    delete_sql = generate_delete_sql(version_id, element_id, node_type)
    if delete_sql == ERROR_CONFIG_DAG_DICT_MAGIC:
        return ResultCode.Error.value
    if delete_sql is None:
        return ResultCode.Success.value
    return db_helper1.execute(delete_sql)


@valid_delete_sql
def generate_delete_sql(version_id, element_id, node_type):
    """
    生成删除 SQL
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param node_type: 节点类型
    :return: SQL
    """
    element = element_info_dict.get(node_type, None)
    table_name = element.get(TABLE_NAME)
    if not table_name:
        return None

    # 删除算子数据
    # noinspection SqlResolve
    delete_sql = f"delete from {table_name} " f"where version_id='{version_id}' " f"and id='{element_id}'"
    return delete_sql
