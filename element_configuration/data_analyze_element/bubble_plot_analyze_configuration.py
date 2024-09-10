import json
import sys

from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from error.element_configuration_config_error import ElementConfigurationConfigError
from error.element_configuration_query_error import ElementConfigurationQueryError
from helper.data_store_helper import output_port_record_sql
from helper.element_port_helper import get_fields_and_role
from helper.result_helper import execute_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.time_helper import current_time
from helper.warning_helper import UNUSED
from helper.wrapper_helper import valid_init_sql


def get(prev_node_output_port, prev_node_type, prev_element_id, version_id, element_id):
    """
    获取数据算子弹窗数据
    :param prev_node_output_port: 所连接算子的输出端口
    :param prev_node_type: 所连接算子的类型
    :param prev_element_id: 所连接算子的标识
    :param version_id: 版本标识
    :param element_id: 算子标识
    :return: {
                "fields": [{"name": "", "nick_name": "", "data_type": 0}],
                "x": "",
                "y": ""
              }
    """
    query_result = get_fields_and_role(prev_node_output_port, prev_node_type, prev_element_id, version_id, db_helper1)
    if query_result.code != ResultCode.Success.value:
        raise ElementConfigurationQueryError(sys._getframe().f_code.co_name, "获取前置算子失败, 请检查画布算子连接")
    fields = query_result.fields
    bubble_plot_analyze_sql = (
        f"select x, y "
        f"from ml_bubble_plot_analyze_element "
        f"where version_id='{version_id}' "
        f"and id='{element_id}' "
        f"and is_enabled={Enabled.Yes.value}"
    )
    bubble_plot_analyze_dict = db_helper1.fetchone(bubble_plot_analyze_sql, [])
    if not bubble_plot_analyze_dict or (
        bubble_plot_analyze_dict["x"] is None and bubble_plot_analyze_dict["y"] is None
    ):
        return execute_success(data={"fields": fields, "x": None, "y": None})
    return execute_success(data={**{"fields": fields}, **bubble_plot_analyze_dict})


def configuration(
    version_id,
    element_id,
    user_id,
    prev_node_output_port,
    prev_node_type,
    prev_element_id,
    x,
    y,
):
    """
    用于保存算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param prev_node_output_port: 所连接算子的输出端口
    :param prev_node_type: 所连接算子的类型
    :param prev_element_id: 所连接算子的标识
    :param x: x轴
    :param y: y轴
    :return: 配置成功/失败
    """
    query_result = get_fields_and_role(prev_node_output_port, prev_node_type, prev_element_id, version_id, db_helper1)
    if query_result.code != ResultCode.Success.value:
        raise ElementConfigurationConfigError(sys._getframe().f_code.co_name, "获取前置算子失败") from None
    fields = query_result.fields
    role = query_result.role
    fields_arr = [fields]
    role_arr = [role] if role is not None else []
    fields_arr_json = json.dumps(fields_arr)
    role_arr_json = json.dumps(role_arr)
    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    output_configuration_store_sql = output_port_record_sql(version_id, element_id, user_id)
    configuration_sql = generate_configuration_sql(version_id, element_id, user_id, x, y)
    return db_helper1.execute_arr(
        [output_configuration_store_sql, configuration_sql],
        {0: [fields_arr_json, role_arr_json]},
    )


def generate_configuration_sql(version_id, element_id, user_id, x, y):
    """
    生成插入或更新算子 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param x: x轴
    :param y: y轴
    :return: SQL
    """
    now = current_time()
    x_sql = f"'{x}' x" if x is not None else "null x"
    y_sql = f"'{y}' y" if y is not None else "null y"
    # 插入或更新
    sql = (
        f"merge into ml_bubble_plot_analyze_element t1 "
        f"using (select "
        f"'{element_id}' id, "
        f"'{version_id}' version_id, "
        f"{x_sql}, "
        f"{y_sql}, "
        f"'{user_id}' user_id from dual) t2 on (t1.id = t2.id and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, "
        f"version_id, x, y, "
        f"is_enabled) values"
        f"(t2.id, "
        f"t2.user_id, "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, "
        f"t2.x, "
        f"t2.y, "
        f"{Enabled.Yes.value}) "
        f"when matched then "
        f"update set t1.x=t2.x, "
        f"t1.y=t2.y, "
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
        f"insert into ml_bubble_plot_analyze_element"
        f"(id, create_user, create_time, "
        f"version_id, x, y, is_enabled)"
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
        f"insert into ml_bubble_plot_analyze_element "
        f"select id, create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', x, y, is_enabled "
        f"from ml_bubble_plot_analyze_element "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql
