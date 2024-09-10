import json
import sys

from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from enum_type.user_data_type import UserDataType
from error.element_configuration_config_error import ElementConfigurationConfigError
from helper.data_store_helper import output_port_record_sql
from helper.element_port_helper import get_fields_and_role
from helper.fields_helper import generate_fields
from helper.result_helper import execute_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.time_helper import current_time
from helper.warning_helper import UNUSED
from helper.wrapper_helper import valid_init_sql


def get(version_id, element_id):
    """
    获取数据算子弹窗数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :return: {
                "k_num": 2
              }
    """
    k_means_sql = (
        f"select k_num "
        f"from ml_k_means_element "
        f"where version_id='{version_id}' "
        f"and id='{element_id}' "
        f"and is_enabled={Enabled.Yes.value}"
    )
    k_means_dict = db_helper1.fetchone(k_means_sql, [])
    if not k_means_dict or (k_means_dict["k_num"] is None):
        return execute_success(data={"k_num": 2})
    k_num = k_means_dict["k_num"]
    return execute_success(data={"k_num": k_num})


def configuration(
    version_id,
    element_id,
    user_id,
    prev_node_output_port,
    prev_node_type,
    prev_element_id,
    k_num,
):
    """
    用于保存算子配置数据
    :param k_num: 聚类的数量
    :param prev_node_output_port: 所连接算子的输出端口
    :param prev_node_type: 所连接算子的类型
    :param prev_element_id: 所连接算子的标识
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :return: 配置成功/失败
    """
    query_result = get_fields_and_role(prev_node_output_port, prev_node_type, prev_element_id, version_id, db_helper1)
    if query_result.code != ResultCode.Success.value:
        raise ElementConfigurationConfigError(sys._getframe().f_code.co_name, "获取前置算子失败") from None
    fields = query_result.fields
    new_fields = [x for x in fields]
    new_fields.append(generate_fields("簇_标", data_type=UserDataType.Number.value))
    role_arr_json = json.dumps([])
    fields_arr_json = json.dumps([new_fields])
    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    output_configuration_store_sql = output_port_record_sql(version_id, element_id, user_id)
    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    configuration_sql = generate_configuration_sql(version_id, element_id, user_id, k_num)
    return db_helper1.execute_arr(
        [output_configuration_store_sql, configuration_sql],
        {0: [fields_arr_json, role_arr_json]},
    )


def generate_configuration_sql(version_id, element_id, user_id, k_num):
    """
    生成插入或更新算子 SQL 语句
    :param k_num: 据类的簇数量
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :return: SQL
    """
    now = current_time()
    if k_num is None:
        k_num = 2
    # 插入或更新
    sql = (
        f"merge into ml_k_means_element t1 "
        f"using (select "
        f"'{element_id}' id, "
        f"'{version_id}' version_id, "
        f"'{user_id}' user_id, "
        f"{k_num} k_num "
        f"from dual) t2 on (t1.id = t2.id "
        f"and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, "
        f"version_id, k_num, "
        f"is_enabled) values"
        f"(t2.id, "
        f"t2.user_id, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, "
        f"t2.k_num, "
        f"{Enabled.Yes.value}) "
        f"when matched then "
        f"update set t1.k_num=t2.k_num, "
        f"t1.is_enabled={Enabled.Yes.value}"
    )
    return sql


def init_element(version_id, element_id, user_id, node_type):
    """
    初始化算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param node_type: 节点类型
    :return: 配置成功/失败
    """
    init_sql = generate_init_sql(version_id, element_id, user_id, node_type)
    if init_sql is None:
        return ResultCode.Success.value
    return db_helper1.execute_arr([init_sql])


@valid_init_sql
def generate_init_sql(version_id, element_id, user_id, node_type):
    """
    生成算子配置初始化的 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param node_type: 节点类型
    :return: SQL
    """
    UNUSED(node_type)
    now = current_time()
    # 初始化算子数据
    # noinspection SqlResolve
    init_sql = (
        f"insert into ml_k_means_element"
        f"(id, create_user, create_time, "
        f"version_id, k_num, "
        f"is_enabled)"
        f"values"
        f"('{element_id}', "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"2,"
        f"{Enabled.No.value})"
    )

    return init_sql


def copy_element(new_version_id, old_version_id):
    """
    拷贝算子数据
    :param new_version_id: 新版本标识
    :param old_version_id: 旧版本标识
    :return: 拷贝成功/失败
    """
    copy_sql = generate_copy_sql(new_version_id, old_version_id)
    return db_helper1.execute(copy_sql)


def generate_copy_sql(new_version_id, old_version_id):
    """
    生成算子拷贝的 SQL 语句
    :param new_version_id: 新版本标识
    :param old_version_id: 旧版本标识
    :return: SQL
    """
    now = current_time()
    # 拷贝算子数据
    copy_sql = (
        f"insert into ml_k_means_element "
        f"select id, create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', k_num, is_enabled "
        f"from ml_k_means_element "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql
