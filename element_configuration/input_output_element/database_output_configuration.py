import sys

from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from error.element_configuration_config_error import ElementConfigurationConfigError
from error.element_configuration_query_error import ElementConfigurationQueryError
from helper.element_port_helper import get_fields_and_role
from helper.generate_helper import generate_uuid, uuid_and_now
from helper.result_helper import execute_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.time_helper import current_time
from helper.warning_helper import UNUSED
from helper.wrapper_helper import valid_delete_sql, valid_init_sql
from service.data_source.data_source_service import get_transient_sql_helper


def get(prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id):
    """
    获取数据算子弹窗数据
    :param prev_node_output_port: 所连接算子的输出端口
    :param prev_node_type: 所连接算子的类型
    :param prev_element_id: 所连接算子的标识
    :param version_id: 版本标识
    :param element_id: 算子标识
    :return: {
                "datasource_id": "",
                "data_table_name": "",
                "fields": [{"name": "", "nick_name": "", "data_type": "NUMBER"}],
                "columns": [｛”source_column_code": "", "target_column_code": ""｝]
             }
    """
    datasource_id = ""
    data_table_name = ""
    columns = []

    database_output_sql = (
        f"select datasource_id, data_table_name "
        f"from ml_database_output_element "
        f"where id='{element_id}' "
        f"and version_id='{version_id}' "
        f"and is_enabled={Enabled.Yes.value} "
    )
    database_output_dict = db_helper1.fetchone(database_output_sql)
    if database_output_dict and "datasource_id" in database_output_dict:
        # datasource_id = database_output_dict["datasource_id"]
        # data_table_name = database_output_dict["data_table_name"]
        datasource_id = database_output_dict.get("datasource_id")
        data_table_name = database_output_dict.get("data_table_name")
        # 列配置表
        database_output_column_sql = (
            f"select source_column_code, target_column_code "
            f"from ml_database_output_element_column "
            f"where element_id='{element_id}' "
            f"and version_id='{version_id}' "
            f"order by sort"
        )
        database_output_column_arr = db_helper1.fetchall(database_output_column_sql)
        if database_output_column_arr:
            columns = database_output_column_arr

    # 前置算子
    query_result = get_fields_and_role(prev_node_output_port, prev_node_type, prev_element_id, version_id, db_helper1)
    if query_result.code != ResultCode.Success.value:
        raise ElementConfigurationQueryError(sys._getframe().f_code.co_name, "获取前置算子失败, 请检查画布算子连接")
    fields = query_result.fields

    return execute_success(
        data={
            "datasource_id": datasource_id,
            "data_table_name": data_table_name,
            "fields": fields,
            "columns": columns,
        }
    )


def get_columns(datasource_id, table_name):
    """
    获取数据表字段信息
    :param datasource_id: 数据源ID
    :param table_name: 表名
    :return: [{
                "column_name": "ID",
                "data_type": "VARCHAR2",
                ...
             }]
    """
    if not datasource_id or not table_name:
        raise ElementConfigurationQueryError(sys._getframe().f_code.co_name, "请配置数据库输出算子的数据源和目标数据表")

    # 获取数据源信息
    db_helper_target = get_transient_sql_helper(datasource_id)
    fields = db_helper_target.fetch_fields(table_name)
    # data = [f.column_name for f in fields]
    return execute_success(data=fields)


def configuration(version_id, element_id, user_id, datasource_id, data_table_name, columns):
    """
    用于保存算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param datasource_id: 数据源ID
    :param data_table_name: 表名
    :param columns: 列配置集合
    :return: 配置成功/失败
    """
    # 校验
    if columns and not isinstance(columns, list):
        raise ElementConfigurationConfigError(sys._getframe().f_code.co_name, "保存数据库输出算子失败, 请配置列信息")

    now = current_time()

    # 取出库中主表信息是否存在
    count = db_helper1.fetchone(
        f"select count(1) as c from ml_database_output_element where id='{element_id}' and version_id='{version_id}'"
    ).get("c")

    # 保存主表的脚本
    if count == 0:
        save_database_output_sql = (
            f"insert into ml_database_output_element "
            f"(id, create_user, create_time, "
            f"version_id, datasource_id, data_table_name, is_enabled) "
            f"values('{element_id}','{user_id}',to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'),"
            f"'{version_id}', '{datasource_id}','{data_table_name}',{Enabled.Yes.value})"
        )
    else:
        save_database_output_sql = (
            f"update ml_database_output_element "
            f" set datasource_id='{datasource_id}'"
            f", data_table_name='{data_table_name}'"
            f", is_enabled='{Enabled.Yes.value}' "
            f" where id='{element_id}' and version_id='{version_id}'"
        )

    # 删除列配置表的脚本
    delete_database_output_column_sql = (
        f"delete from ml_database_output_element_column "
        f"where element_id='{element_id}' and version_id='{version_id}'"
    )

    # 保存列配置表的脚本
    save_database_output_column_sql = batch_insert_database_output_column_sql(version_id, element_id, user_id, columns)

    return db_helper1.execute_arr(
        [
            save_database_output_sql,
            delete_database_output_column_sql,
            save_database_output_column_sql,
        ]
    )


def batch_insert_database_output_column_sql(version_id, element_id, user_id, columns):
    """
    生成批量插入的数据库输出列配置表的SQL脚本
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param columns: 列配置对象集合
    :return: SQL脚本：insert into [table_name] (...) select ... from dual union select ... from dual ...
    """
    now = current_time()

    insert_sql = (
        "insert into ml_database_output_element_column("
        "id, create_user, create_time, "
        "version_id, element_id, "
        "source_column_code, target_column_code, sort) "
    )
    sql_arr = []
    sort = 1
    for col in columns:
        uuid_ = generate_uuid()
        source_column_code = col.get("source_column_code") or ""
        target_column_code = col.get("target_column_code") or ""
        sql = (
            f"select '{uuid_}', '{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
            f"'{version_id}', '{element_id}', "
            f"'{source_column_code}', '{target_column_code}', {sort} "
            f"from dual"
        )
        sql_arr.append(sql)
        sort += 1

    return insert_sql + " union ".join(sql_arr)


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
    uuid_, now = uuid_and_now()
    # 初始化算子数据
    # noinspection SqlResolve
    init_sql = (
        f"insert into ml_database_output_element "
        f"(id, create_user, create_time, "
        f"version_id, is_enabled) "
        f"values "
        f"('{element_id}', "
        f"'{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
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
        f"insert into ml_database_output_element "
        f"(id, create_user, create_time, version_id, "
        f"datasource_id, data_table_name, is_enabled) "
        f"select id, create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), '{new_version_id}', "
        f"datasource_id, data_table_name, is_enabled "
        f"from ml_database_output_element "
        f"where version_id='{old_version_id}'"
    )

    copy_column_sql = (
        f"insert into ml_database_output_element_column "
        f"(id, create_user, create_time, "
        f"version_id, element_id, "
        f"source_column_code, target_column_code, sort) "
        f"select id, create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', element_id, "
        f"source_column_code, target_column_code, sort "
        f"from ml_database_output_element_column "
        f"where version_id='{old_version_id}'"
    )

    return [copy_sql, copy_column_sql]


def delete_element(version_id, element_id, node_type):
    """
    删除算子操作
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param node_type: 节点类型
    :return: 删除结果
    """
    delete_sql, delete_column_sql = generate_delete_sql(version_id, element_id, node_type)
    if delete_sql is None:
        return ResultCode.Success.value
    return db_helper1.execute_arr([delete_sql, delete_column_sql])


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
    delete_sql = f"delete from ml_database_output_element " f"where version_id='{version_id}' " f"and id='{element_id}'"
    delete_column_sql = (
        f"delete from ml_database_output_element_column "
        f"where version_id='{version_id}' "
        f"and element_id='{element_id}'"
    )
    return delete_sql, delete_column_sql
