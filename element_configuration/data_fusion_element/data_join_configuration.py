import json
import sys

from enum_type.enabled import Enabled
from enum_type.join_type import JoinType
from enum_type.result_code import ResultCode
from error.element_configuration_config_error import ElementConfigurationConfigError
from error.element_configuration_query_error import ElementConfigurationQueryError
from helper.data_store_helper import output_port_record_sql
from helper.dictionary_rename_helper import field_rename
from helper.element_port_helper import get_fields_and_role, port
from helper.result_helper import execute_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.time_helper import current_time
from helper.warning_helper import UNUSED
from helper.wrapper_helper import valid_init_sql


def get(
    prev_node_output_port_arr,
    prev_node_type_arr,
    prev_element_id_arr,
    version_id,
    element_id,
):
    """
    获取数据算子弹窗数据
    :param prev_node_output_port_arr: 所连接算子的输出端口数组
    :param prev_node_type_arr: 所连接算子的类型数组
    :param prev_element_id_arr: 所连接算子的标识数组
    :param version_id: 版本标识
    :param element_id: 算子标识
    :return: {
                "fields_0": [{"name": "", "nick_name": "", "data_type": "NUMBER"}],
                "fields_1": [{"name": "", "nick_name": "", "data_type": "NUMBER"}],
                "join_type": "inner_join",
                "join_field": ["", ""],
                "fields": ["", ""]
              }
    """
    # join_type, join_field, fields
    if (
        not (prev_node_output_port_arr and prev_node_type_arr)
        or not prev_element_id_arr
        or len(prev_node_output_port_arr) != 2
        or len(prev_node_type_arr) != 2
        or len(prev_element_id_arr) != 2
    ):
        return execute_success(
            data={
                "fields_0": [],
                "fields_1": [],
                "join_type": JoinType.InnerJoin.value,
                "join_field": [],
                "fields": [],
            }
        )

    prev_node_output_port_0 = port(0, prev_node_output_port_arr)
    prev_node_output_port_1 = port(1, prev_node_output_port_arr)

    prev_node_type_0 = port(0, prev_node_type_arr)
    prev_node_type_1 = port(1, prev_node_type_arr)

    prev_element_id_0 = port(0, prev_element_id_arr)
    prev_element_id_1 = port(1, prev_element_id_arr)

    query_result_0 = get_fields_and_role(
        prev_node_output_port_0,
        prev_node_type_0,
        prev_element_id_0,
        version_id,
        db_helper1,
    )
    query_result_1 = get_fields_and_role(
        prev_node_output_port_1,
        prev_node_type_1,
        prev_element_id_1,
        version_id,
        db_helper1,
    )
    if query_result_0.code != ResultCode.Success.value or query_result_1.code != ResultCode.Success.value:
        raise ElementConfigurationQueryError(sys._getframe().f_code.co_name, "获取前置算子失败, 请检查画布算子连接")
    fields_0 = query_result_0.fields
    fields_1 = query_result_1.fields

    prev_element_id_0 += "-0"
    prev_element_id_1 += "-1"

    convert_fields_0 = field_rename(fields_0, prev_element_id_0)
    convert_fields_1 = field_rename(fields_1, prev_element_id_1)

    data_join_sql = (
        f"select join_type, join_field, fields "
        f"from ml_data_join_element "
        f"where version_id='{version_id}' "
        f"and id='{element_id}' "
        f"and is_enabled={Enabled.Yes.value}"
    )
    data_join_dict = db_helper1.fetchone(data_join_sql, [])
    if not data_join_dict or (
        data_join_dict["join_type"] is None and data_join_dict["join_field"] is None and data_join_dict["fields"]
    ):
        return execute_success(
            data={
                "fields_0": convert_fields_0,
                "fields_1": convert_fields_1,
                "join_type": JoinType.InnerJoin.value,
                "join_field": [],
                "fields": [],
            }
        )
    if data_join_dict["fields"]:
        data_join_dict["fields"] = json.loads(data_join_dict["fields"])
    if data_join_dict["join_field"]:
        data_join_dict["join_field"] = json.loads(data_join_dict["join_field"])
    return execute_success(
        data={
            **{"fields_0": convert_fields_0, "fields_1": convert_fields_1},
            **data_join_dict,
        }
    )


def configuration(
    version_id,
    element_id,
    user_id,
    prev_node_output_port_arr,
    prev_node_type_arr,
    prev_element_id_arr,
    join_type,
    join_field,
    fields,
):
    """
    用于保存算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param prev_node_output_port_arr: 所连接算子的输出端口数组
    :param prev_node_type_arr: 所连接算子的类型数组
    :param prev_element_id_arr: 所连接算子的标识数组
    :param join_type: 连接类型
    :param join_field: 连接字段
    :param fields: 显示字段数组
    :return: 配置成功/失败
    """

    if not join_type:
        raise ElementConfigurationConfigError(sys._getframe().f_code.co_name, "数据连接类型未配置") from None

    prev_node_output_port_0 = port(0, prev_node_output_port_arr)
    prev_node_output_port_1 = port(1, prev_node_output_port_arr)

    prev_node_type_0 = port(0, prev_node_type_arr)
    prev_node_type_1 = port(1, prev_node_type_arr)

    prev_element_id_0 = port(0, prev_element_id_arr)
    prev_element_id_1 = port(1, prev_element_id_arr)

    query_result_0 = get_fields_and_role(
        prev_node_output_port_0,
        prev_node_type_0,
        prev_element_id_0,
        version_id,
        db_helper1,
    )
    query_result_1 = get_fields_and_role(
        prev_node_output_port_1,
        prev_node_type_1,
        prev_element_id_1,
        version_id,
        db_helper1,
    )

    prev_element_id_0 += "-0"
    prev_element_id_1 += "-1"

    if query_result_0.code != ResultCode.Success.value or query_result_1.code != ResultCode.Success.value:
        raise ElementConfigurationConfigError(sys._getframe().f_code.co_name, "获取前置算子失败") from None
    fields_0 = query_result_0.fields
    fields_1 = query_result_1.fields
    role_0 = query_result_0.role
    role_1 = query_result_1.role

    convert_fields_0 = field_rename(fields_0, prev_element_id_0)
    convert_fields_1 = field_rename(fields_1, prev_element_id_1)

    convert_role_0 = field_rename(role_0, prev_element_id_0)
    convert_role_1 = field_rename(role_1, prev_element_id_1)

    convert_fields = [x for x in convert_fields_0 if x["name"] in fields]
    convert_fields.extend([x for x in convert_fields_1 if x["name"] in fields])

    role = []
    if convert_role_0:
        role.extend([x for x in convert_role_0 if x["name"] in fields])
    if convert_role_1:
        role.extend([x for x in convert_role_1 if x["name"] in fields])

    fields_arr = [fields]
    role_arr = [role] if role is not None else []
    fields_arr_json = json.dumps(fields_arr)
    role_arr_json = json.dumps(role_arr)

    if join_field is None:
        join_field = []
    join_field_json = json.dumps(join_field)

    if fields is None:
        fields = []
    fields_json = json.dumps(fields)

    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    output_configuration_store_sql = output_port_record_sql(version_id, element_id, user_id)

    configuration_sql = generate_configuration_sql(version_id, element_id, user_id, join_type)
    return db_helper1.execute_arr(
        [output_configuration_store_sql, configuration_sql],
        {
            0: [fields_arr_json, role_arr_json],
            1: [join_field_json, fields_json, join_field_json, fields_json],
        },
    )


def generate_configuration_sql(version_id, element_id, user_id, join_type):
    """
    生成插入或更新算子 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param join_type: 连接类型
    :return: SQL
    """
    now = current_time()
    # 插入或更新
    sql = (
        f"merge into ml_data_join_element t1 "
        f"using (select "
        f"'{element_id}' id, "
        f"'{version_id}' version_id, "
        f"'{join_type}' join_type, "
        f"'{user_id}' user_id from dual) t2 on (t1.id = t2.id and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, "
        f"version_id, join_type, join_field, fields, "
        f"is_enabled) values"
        f"(t2.id, "
        f"t2.user_id, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, "
        f"t2.join_type, "
        f":1, "
        f":2, "
        f"{Enabled.Yes.value}) "
        f"when matched then "
        f"update set t1.join_type=t2.join_type, "
        f"t1.join_field=:3, "
        f"t1.fields=:4, "
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
        f"insert into ml_data_join_element"
        f"(id, create_user, create_time, "
        f"version_id, join_type, join_field, fields, "
        f"is_enabled)"
        f"values"
        f"('{element_id}', "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"'{JoinType.InnerJoin.value}', "
        f"'{json.dumps([])}', "
        f"'{json.dumps([])}', "
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
        f"insert into ml_data_join_element "
        f"select id, create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', join_type, join_field, fields, is_enabled "
        f"from ml_data_join_element "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql
