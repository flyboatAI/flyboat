from element_configuration.input_output_element.sync_input_configuration import (
    delete_sync_element_configuration_sql,
    get_config_id,
    sync_element_config_configuration_sql,
)
from enum_type.deleted import Deleted
from enum_type.element_config_type import ElementConfigType
from enum_type.enabled import Enabled
from enum_type.input_type import ValueType
from enum_type.result_code import ResultCode
from helper.generate_helper import generate_uuid, uuid_and_now
from helper.result_helper import execute_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.time_helper import current_time
from helper.warning_helper import UNUSED
from helper.wrapper_helper import valid_delete_sql, valid_init_sql


def get(version_id, element_id):
    """
    获取数据算子弹窗数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :return: {
                "key": "",
                "nick_name": ""
              }
    """
    model_sync_output_sql = (
        f"select t2.id as config_id, t2.json_key as key, t2.nick_name "
        f"from ml_model_sync_output_element t1 "
        f"left join ml_sync_element_config t2 on "
        f"t1.id=t2.element_id "
        f"and t1.version_id=t2.version_id "
        f"where t1.id='{element_id}' "
        f"and t1.version_id='{version_id}' "
        f"and t2.is_deleted={Deleted.No.value} "
        f"and t2.element_type='{ElementConfigType.ModelOutput.value}'"
    )
    model_sync_output_dict = db_helper1.fetchone(model_sync_output_sql, [])
    if not model_sync_output_dict:
        return execute_success(data={"key": "", "nick_name": ""})
    return execute_success(data=model_sync_output_dict)


def configuration(version_id, element_id, user_id, key, nick_name):
    """
    用于保存算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param key: 标识
    :param nick_name: 别名
    :return: 配置成功/失败
    """
    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    configuration_sql = model_sync_output_configuration_sql(version_id, element_id, user_id)
    config_id_dict = db_helper1.fetchone(get_config_id(version_id, element_id))
    if config_id_dict is None or config_id_dict.get("id") is None:
        config_id = generate_uuid()
    else:
        config_id = config_id_dict.get("id")
    config_configuration_sql = sync_element_config_configuration_sql(
        config_id,
        version_id,
        element_id,
        user_id,
        ElementConfigType.ModelOutput.value,
        key,
        nick_name,
        value_type=ValueType.Model.value,
    )

    return db_helper1.execute_arr([configuration_sql, config_configuration_sql])


def model_sync_output_configuration_sql(version_id, element_id, user_id):
    """
    生成算子配置初始化的 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :return: SQL
    """
    now = current_time()

    # 插入或更新
    configuration_sql = (
        f"merge into ml_model_sync_output_element t1 "
        f"using (select "
        f"'{element_id}' id, "
        f"'{version_id}' version_id, "
        f"'{user_id}' user_id, "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss') create_time "
        f"from dual) t2 on (t1.id = t2.id and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, "
        f"version_id, is_enabled) values"
        f"(t2.id, "
        f"t2.user_id, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, {Enabled.Yes.value}) "
        f"when matched then "
        f"update set t1.is_enabled={Enabled.Yes.value}"
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
    init_sql, init_config_sql = generate_init_sql(version_id, element_id, user_id, node_type)
    if init_sql is None:
        return ResultCode.Success.value
    return db_helper1.execute_arr([init_sql, init_config_sql])


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
    uuid_, now = uuid_and_now()
    # 初始化算子数据
    # noinspection SqlResolve
    init_sql = (
        f"insert into ml_model_sync_output_element"
        f"(id, create_user, create_time, "
        f"version_id, is_enabled)"
        f"values"
        f"('{element_id}', "
        f"'{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"{Enabled.No.value})"
    )

    init_config_sql = (
        f"insert into ml_sync_element_config(id, is_deleted, create_user, create_time, "
        f"value_type, element_id, version_id, element_type)"
        f"values"
        f"('{uuid_}', {Deleted.No.value}, "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{ValueType.Model.value}', '{element_id}', '{version_id}', "
        f"'{ElementConfigType.ModelOutput.value}')"
    )

    return init_sql, init_config_sql


def copy_element(new_version_id, old_version_id):
    """
    拷贝算子数据
    :param new_version_id: 新版本标识
    :param old_version_id: 旧版本标识
    :return: 拷贝成功/失败
    """
    copy_sql_arr = generate_copy_sql(new_version_id, old_version_id)
    return db_helper1.execute_arr(copy_sql_arr)


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
        f"insert into ml_model_sync_output_element "
        f"select id, create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', is_enabled "
        f"from ml_model_sync_output_element "
        f"where version_id='{old_version_id}'"
    )

    copy_config_sql = (
        f"insert into ml_sync_element_config(id, is_deleted, "
        f"create_user, create_time, update_user, update_time, nick_name, "
        f"description, json_key, value_type, start_key, end_key, "
        f"step_key, single_key, single_value_data_type, version_id, "
        f"element_id, element_type, publish_id, sort) select CREATEGUID(), is_deleted, "
        f"create_user, create_time, update_user, update_time, nick_name, description, "
        f"json_key, value_type, start_key, end_key, step_key, single_key, "
        f"single_value_data_type, "
        f"'{new_version_id}', element_id, element_type, null, sort "
        f"from ml_sync_element_config "
        f"where version_id='{old_version_id}' "
        f"and is_deleted={Deleted.No.value} "
        f"and element_type='{ElementConfigType.ModelOutput.value}'"
    )

    return [copy_sql, copy_config_sql]


def delete_element(version_id, element_id, node_type):
    """
    删除算子操作
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param node_type: 节点类型
    :return: 删除结果
    """
    delete_sql, delete_config_sql = generate_delete_sql(version_id, element_id, node_type)

    if delete_sql is None:
        return ResultCode.Success.value
    return db_helper1.execute_arr([delete_sql, delete_config_sql])


@valid_delete_sql
def generate_delete_sql(version_id, element_id, node_type):
    """
    生成删除 SQL
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param node_type: 节点类型
    :return: SQL
    """
    UNUSED(node_type)
    # 删除算子数据
    # noinspection SqlResolve
    delete_sql = (
        f"delete from ml_model_sync_output_element " f"where version_id='{version_id}' " f"and id='{element_id}'"
    )
    delete_config_sql = delete_sync_element_configuration_sql(version_id, element_id)
    return delete_sql, delete_config_sql
