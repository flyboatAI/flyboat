import json
import sys

from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from error.data_process_error import DataProcessError
from error.element_configuration_config_error import ElementConfigurationConfigError
from helper.data_store_helper import output_port_record_sql
from helper.element_port_helper import get_fields_and_role
from helper.result_helper import execute_success
from helper.sample_data_generate_helper import generate_table_data
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
                "formula_content": "",
              }
    """
    custom_formula_sql = (
        f"select formula_content "
        f"from ml_custom_formula_element "
        f"where version_id='{version_id}' "
        f"and id='{element_id}'"
    )
    custom_formula_dict = db_helper1.fetchone(custom_formula_sql, [])
    if not custom_formula_dict:
        return execute_success(data={"formula_content": ""})

    return execute_success(data=custom_formula_dict)


def configuration(
    version_id,
    element_id,
    user_id,
    prev_node_output_port_arr,
    prev_node_type_arr,
    prev_element_id_arr,
    formula_content,
):
    """
    用于保存算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param prev_node_output_port_arr: 所连接算子的输出端口数组
    :param prev_node_type_arr: 所连接算子的类型数组
    :param prev_element_id_arr: 所连接算子的标识数组
    :param formula_content: 自定义公式代码
    :return: 配置成功/失败
    """
    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    configuration_sql = generate_configuration_sql(version_id, element_id, user_id)
    fields_arr = []
    data_arr = []
    for i in range(0, len(prev_node_output_port_arr)):
        prev_node_output_port = prev_node_output_port_arr[i]
        prev_node_type = prev_node_type_arr[i]
        prev_element_id = prev_element_id_arr[i]
        query_result = get_fields_and_role(
            prev_node_output_port,
            prev_node_type,
            prev_element_id,
            version_id,
            db_helper1,
        )
        if query_result.code != ResultCode.Success.value:
            import re

            p = re.compile(r"output_fields\s*=\s*\[(.*?)]", re.MULTILINE | re.DOTALL)
            fields_str_arr = p.findall(formula_content)
            fields_arr_json = None
            if fields_str_arr:
                fields_str = fields_str_arr[0]
                fields_str = fields_str.replace("'", '"')
                fields_arr_json = f"[[{fields_str}]]"

            if not fields_arr_json:
                raise DataProcessError("未配置输出字段信息，配置自定义公式算子失败") from None
            # noinspection PyBroadException
            try:
                json.loads(fields_arr_json)
            except Exception:
                raise DataProcessError("配置的输出字段信息格式错误，请检查确认(例如末尾是否多逗号)") from None
            fields_arr = []
            data_arr = []
            break
        else:
            fields = query_result.fields
            fields_arr.append(fields)
            data = generate_table_data(fields)
            data_arr.append(data)
    if not fields_arr or not data_arr:
        raise ElementConfigurationConfigError(sys._getframe().f_code.co_name, "获取前置算子数据以及字段失败") from None
    params = {
        "connect_id": None,
        "websocket": None,
        "data_arr": data_arr,
        "fields_arr": fields_arr,
        "output_data": None,
        "output_fields": None,
    }

    exec(formula_content, params)

    output_fields = params["output_fields"]
    output_fields_arr = [output_fields]
    fields_arr_json = json.dumps(output_fields_arr)

    role_arr_json = json.dumps([])

    output_configuration_store_sql = output_port_record_sql(version_id, element_id, user_id)
    return db_helper1.execute_arr(
        [output_configuration_store_sql, configuration_sql],
        {0: [fields_arr_json, role_arr_json], 1: [formula_content, formula_content]},
    )


def generate_configuration_sql(version_id, element_id, user_id):
    """
    生成插入或更新算子 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :return: SQL
    """
    now = current_time()
    # 插入或更新
    configuration_sql = (
        f"merge into ml_custom_formula_element t1 "
        f"using (select "
        f"'{element_id}' id, "
        f"'{version_id}' version_id, "
        f"'{user_id}' user_id "
        f"from dual) t2 on (t1.id = t2.id and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, version_id, "
        f"formula_content, is_enabled) "
        f"values"
        f"(t2.id, "
        f"t2.user_id, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, "
        f":1, "
        f"{Enabled.Yes.value}) "
        f"when matched then "
        f"update set t1.formula_content=:2, "
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
        f"insert into ml_custom_formula_element"
        f"(id, create_user, create_time, "
        f"version_id, formula_content, "
        f"is_enabled)"
        f"values"
        f"('{element_id}', "
        f"'{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"null, "
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
        f"insert into ml_custom_formula_element "
        f"select id, create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', formula_content, is_enabled "
        f"from ml_custom_formula_element "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql
