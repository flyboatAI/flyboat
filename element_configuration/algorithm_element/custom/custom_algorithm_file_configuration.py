import json

from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from helper.generate_helper import generate_uuid
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
                "algorithm_id": "",
                "algorithm_name": "",
                "code": "",
                "description": "",
                "params": [
                    {
                        "id": "",
                        "name": "",
                        "value": "",
                        "description": ""
                    }
                ]
              }
    """
    algorithm_sql = (
        f"select t1.algorithm_id, "
        f"t2.algorithm_name, "
        f"t2.algorithm_content as code,"
        f"t2.description "
        f"from ml_custom_algorithm_element t1 "
        f"left join ml_algorithm t2 "
        f"on t1.algorithm_id=t2.id "
        f"where t1.version_id='{version_id}' "
        f"and t1.id='{element_id}' "
        f"and t1.is_enabled={Enabled.Yes.value}"
    )
    algorithm_dict = db_helper1.fetchone(algorithm_sql, [])
    if not algorithm_dict:
        return execute_success(
            data={
                "algorithm_id": "",
                "algorithm_name": "",
                "code": "",
                "description": "",
                "params": [],
            }
        )

    algorithm_params_sql = (
        f"select t2.id, t2.name, t2.nick_name, "
        f"t2.parameter_data_type, t2.algorithm_id, "
        f"t2.is_array, t2.enum_value, t2.sort, t1.value, t2.description "
        f"from ml_custom_algorithm_config t1 "
        f"left join ml_algorithm_params t2 "
        f"on t2.id=t1.param_id "
        f"where t1.version_id='{version_id}' "
        f"and t1.element_id='{element_id}' "
        f"and t2.id is not null "
        f"order by t2.sort"
    )
    params_arr = db_helper1.fetchall(algorithm_params_sql)
    for param in params_arr:
        enum_value = param.get("enum_value")
        if enum_value:
            param["enum_list"] = enum_value.split(",")
        else:
            param["enum_list"] = []
        is_array = param.get("is_array")
        value = param.get("value")
        if is_array and value:
            param["value"] = json.loads(value)

    algorithm_dict["params"] = params_arr
    return execute_success(data=algorithm_dict)


def get_params(algorithm_id):
    """
    根据算法标识获取配置的参数信息
    :param algorithm_id: 算法标识
    :return: 参数列表
    """
    algorithm_params_sql = (
        f"select id, name, nick_name, parameter_data_type, "
        f"is_array, enum_value, algorithm_id, sort, description "
        f"from ml_algorithm_params "
        f"where algorithm_id='{algorithm_id}' order by sort"
    )
    params_arr = db_helper1.fetchall(algorithm_params_sql)
    for param in params_arr:
        enum_value = param.get("enum_value")
        if enum_value:
            param["enum_list"] = enum_value.split(",")
        else:
            param["enum_list"] = []
    return execute_success(data=params_arr)


def configuration(version_id, element_id, user_id, algorithm_id, params):
    """
    用于保存算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param algorithm_id: 自定义算法标识
    :param params: 参数数组信息
    :return: 配置成功/失败
    """
    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    configuration_sql = generate_configuration_sql(version_id, element_id, user_id, algorithm_id)
    delete_sql = delete_params_config_sql(version_id, element_id)
    params_configuration_sql = algorithm_parameter_configuration_sql(version_id, element_id, user_id, params)
    return db_helper1.execute_arr(
        [configuration_sql, delete_sql, params_configuration_sql] if len(params) else [configuration_sql]
    )


def generate_configuration_sql(version_id, element_id, user_id, algorithm_id):
    """
    生成插入或更新算子 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param algorithm_id: 算法标识
    :return: SQL
    """
    now = current_time()
    if not algorithm_id:
        algorithm_id = ""
    # 插入或更新
    configuration_sql = (
        f"merge into ml_custom_algorithm_element t1 "
        f"using (select "
        f"'{element_id}' id, "
        f"'{version_id}' version_id, "
        f"'{user_id}' user_id,"
        f"'{algorithm_id}' algorithm_id "
        f"from dual) t2 on (t1.id = t2.id and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, version_id, "
        f"algorithm_id, is_enabled) "
        f"values"
        f"(t2.id, "
        f"t2.user_id, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, "
        f"t2.algorithm_id, "
        f"{Enabled.Yes.value}) "
        f"when matched then "
        f"update set t1.algorithm_id=t2.algorithm_id, "
        f"t1.is_enabled={Enabled.Yes.value}"
    )
    return configuration_sql


def delete_params_config_sql(version_id, element_id):
    """
    算法参数删除 SQL
    :param version_id: 版本标识
    :param element_id: 算子标识
    :return: SQL
    """
    delete_sql = (
        f"delete from ml_custom_algorithm_config " f"where version_id='{version_id}' " f"and element_id='{element_id}'"
    )
    return delete_sql


def algorithm_parameter_configuration_sql(version_id, element_id, user_id, params):
    """
    算法参数配置 SQL
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param params: 参数数组信息
    :return: SQL
    """
    now = current_time()
    begin_configuration_sql = (
        "insert into ml_custom_algorithm_config(id, create_user, "
        "create_time, version_id, "
        "element_id, param_id, value) "
    )
    configuration_sql_arr = []
    params_len = len(params)
    for i in range(params_len):
        parameter = params[i]
        param_id = parameter.get("param_id")
        value = parameter.get("value")
        if isinstance(value, list):
            value = json.dumps(value)
        configuration_sql = (
            f"select '{generate_uuid()}', '{user_id}', "
            f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
            f"'{version_id}', '{element_id}', "
            f"'{param_id}', "
            f"'{value}' from dual"
        )
        configuration_sql_arr.append(configuration_sql)
    union_sql = " union ".join(configuration_sql_arr)
    return begin_configuration_sql + " ( " + union_sql + " ) "


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
    # 初始化算子数据
    # noinspection SqlResolve
    init_sql = (
        f"insert into ml_custom_algorithm_element"
        f"(id, create_user, create_time, "
        f"version_id, algorithm_id, "
        f"is_enabled)"
        f"values"
        f"('{element_id}', "
        f"'{user_id}', "
        f"to_date('{current_time()}', 'yyyy-mm-dd hh24:mi:ss'), "
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
    copy_params_sql = generate_parameter_copy_sql(new_version_id, old_version_id)
    return db_helper1.execute_arr([copy_sql, copy_params_sql])


def generate_copy_sql(new_version_id, old_version_id):
    """
    生成算子拷贝的 SQL 语句
    :param new_version_id: 新版本标识
    :param old_version_id: 旧版本标识
    :return: SQL
    """

    # 拷贝算子数据
    copy_sql = (
        f"insert into ml_custom_algorithm_element "
        f"select id, create_user, to_date('{current_time()}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', algorithm_id, is_enabled "
        f"from ml_custom_algorithm_element "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql


def generate_parameter_copy_sql(new_version_id, old_version_id):
    copy_sql = (
        f"insert into ml_custom_algorithm_config "
        f"select createguid(), create_user, to_date('{current_time()}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', element_id, param_id, value "
        f"from ml_custom_algorithm_config "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql
