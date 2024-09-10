import json
import sys

from enum_type.deleted import Deleted
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from error.element_configuration_config_error import ElementConfigurationConfigError
from helper.data_store_helper import output_port_record_sql
from helper.result_helper import execute_success
from helper.sql_helper.init_sql_helper import db_helper1, db_helper2
from helper.time_helper import current_time
from helper.warning_helper import UNUSED
from helper.wrapper_helper import valid_init_sql


def get(version_id, element_id, user_id):
    """
    获取数据算子弹窗数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :return: {
                "data_model_type": "",
                "table_name": "",
                "data_model_table_name": "",
                "data_model_name": "",
                "data_model_id": ""
              }
    """
    data_model_sql = (
        f"select data_model_id, data_model_type "
        f"from ml_data_model_element "
        f"where id='{element_id}' "
        f"and version_id='{version_id}' "
        f"and is_enabled={Enabled.Yes.value}"
    )

    data_model_dict = db_helper1.fetchone(data_model_sql)
    if not data_model_dict:
        return execute_success(data=None)
    data_model_type = data_model_dict.get("data_model_type")
    data_model_id = data_model_dict.get("data_model_id")
    if not data_model_type or not data_model_id:
        return execute_success(data=None)

    data_model_table_sql = (
        f"select remark as data_model_table from blade_dict_biz " f"where dict_key='{data_model_type}'"
    )
    data_model_table_dict = db_helper1.fetchone(data_model_table_sql)
    if not data_model_table_dict or "data_model_table" not in data_model_table_dict:
        return execute_success(data=None)
    data_model_table = data_model_table_dict.get("data_model_table")
    # noinspection SqlResolve
    table_sql = (
        f"select t1.data_model_name, t2.name "
        f"from {data_model_table} t1 "
        f"left join ml_ddm_table t2 on "
        f"t1.table_id=t2.id "
        f"where t1.id='{data_model_id}' and "
        f"t1.is_deleted={Deleted.No.value} and "
        f"t1.create_user='{user_id}'"
    )

    table_dict = db_helper1.fetchone(table_sql)
    if not table_dict:
        return execute_success(data=None)
    return execute_success(
        data={
            "data_model_type": data_model_type,
            "table_name": table_dict.get("name"),
            "data_model_table_name": data_model_table,
            "data_model_name": table_dict.get("data_model_name"),
            "data_model_id": data_model_id,
        }
    )


def configuration(version_id, element_id, user_id, data_model_id, data_model_type):
    """
    用于保存算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param data_model_id: 数据模型标识
    :param data_model_type: 数据模型类型
    :return: 配置成功/失败
    """
    data_model_table_sql = (
        f"select remark as data_model_table from blade_dict_biz " f"where dict_key='{data_model_type}'"
    )
    data_model_table_dict = db_helper1.fetchone(data_model_table_sql)
    if not data_model_table_dict or "data_model_table" not in data_model_table_dict:
        raise ElementConfigurationConfigError(sys._getframe().f_code.co_name, "获取字典表失败") from None

    data_model_table = data_model_table_dict.get("data_model_table")
    # noinspection SqlResolve
    table_id_sql = f"select table_id from {data_model_table} " f"where id='{data_model_id}'"
    table_id_dict = db_helper1.fetchone(table_id_sql, [])
    if not table_id_dict:
        raise ElementConfigurationConfigError(sys._getframe().f_code.co_name, "获取模型表标识失败") from None
    table_id = table_id_dict["table_id"]
    if not table_id:
        raise ElementConfigurationConfigError(sys._getframe().f_code.co_name, "获取模型表标识失败") from None
    # 从该表获取数据模型
    table_name_sql = f"select name from ml_ddm_table where id='{table_id}'"
    table_name_dict = db_helper1.fetchone(table_name_sql, [])
    if not table_name_dict:
        raise ElementConfigurationConfigError(sys._getframe().f_code.co_name, "获取模型表标识失败") from None
    table_name = table_name_dict["name"]
    if not table_name:
        raise ElementConfigurationConfigError(sys._getframe().f_code.co_name, "获取模型表标识失败") from None
    fields = db_helper2.fetch_fields_format(table_name)
    fields_arr = [fields]
    fields_arr_json = json.dumps(fields_arr)
    role_arr_json = json.dumps([])
    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    output_configuration_store_sql = output_port_record_sql(version_id, element_id, user_id)
    configuration_sql = generate_configuration_sql(version_id, element_id, user_id, data_model_id, data_model_type)
    return db_helper1.execute_arr(
        [output_configuration_store_sql, configuration_sql],
        {0: [fields_arr_json, role_arr_json]},
    )


def generate_configuration_sql(version_id, element_id, user_id, data_model_id, data_model_type):
    """
    生成插入或更新算子 SQL 语句
    :param data_model_type:
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param data_model_id: 数据模型标识
    :return: SQL
    """
    now = current_time()
    # 插入或更新
    configuration_sql = (
        f"merge into ml_data_model_element t1 "
        f"using (select "
        f"'{element_id}' id, "
        f"'{version_id}' version_id, "
        f"'{data_model_id}' data_model_id, "
        f"'{data_model_type}' data_model_type, "
        f"'{user_id}' user_id from dual) t2 on (t1.id = t2.id "
        f"and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, version_id, "
        f"data_model_id, data_model_type, is_enabled) values"
        f"(t2.id, "
        f"t2.user_id, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, "
        f"t2.data_model_id, "
        f"t2.data_model_type, "
        f"{Enabled.Yes.value})"
        f"when matched then "
        f"update set t1.data_model_id=t2.data_model_id, "
        f"t1.data_model_type=t2.data_model_type, "
        f"t1.is_enabled={Enabled.Yes.value}"
    )
    return configuration_sql


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
        f"insert into ml_data_model_element"
        f"(id, create_user, create_time, "
        f"version_id, data_model_id, data_model_type, is_enabled)"
        f"values"
        f"('{element_id}', "
        f"'{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"'', "
        f"'', "
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
        f"insert into ml_data_model_element "
        f"select id, create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', data_model_id, data_model_type, is_enabled "
        f"from ml_data_model_element "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql
