import json

from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
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
                "model_name": "",
                "fields": []
              }
    """
    model_file_output_sql = (
        f"select model_name, fields "
        f"from ml_model_output_element "
        f"where version_id='{version_id}' "
        f"and id='{element_id}' "
        f"and is_enabled={Enabled.Yes.value}"
    )
    model_file_output_dict = db_helper1.fetchone(model_file_output_sql, [])
    if not model_file_output_dict:
        return execute_success(data={"model_name": None, "fields": []})
    fields_json = model_file_output_dict["fields"]
    if fields_json:
        fields = json.loads(fields_json)
        model_file_output_dict["fields"] = fields
    return execute_success(data=model_file_output_dict)


def configuration(version_id, element_id, user_id, model_name):
    """
    用于保存算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param model_name: 输出模型名称
    :return: 配置成功/失败
    """
    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    configuration_sql = generate_configuration_sql(version_id, element_id, user_id, model_name)
    return db_helper1.execute_arr([configuration_sql])


def generate_configuration_sql(version_id, element_id, user_id, model_name, fields=None):
    """
    生成算子配置初始化的 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param model_name: 输出模型名称
    :param fields: 输出模型的输出字段数组
    :return: SQL
    """
    now = current_time()
    if model_name is None:
        model_name = ""
    if fields is None:
        fields = []

    fields_json = json.dumps(fields)
    # 插入或更新
    configuration_sql = (
        f"merge into ml_model_output_element t1 "
        f"using (select "
        f"'{element_id}' id, "
        f"'{version_id}' version_id, "
        f"'{user_id}' user_id,"
        f"'{model_name}' model_name, "
        f"'{fields_json}' fields "
        f"from dual) t2 on (t1.id = t2.id and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, version_id, "
        f"model_name, fields, is_enabled) "
        f"values"
        f"(t2.id, "
        f"t2.user_id, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, "
        f"t2.model_name, "
        f"t2.fields,"
        f"{Enabled.Yes.value}) "
        f"when matched then "
        f"update set t1.model_name=t2.model_name, "
        f"t1.fields=t2.fields, "
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
        f"insert into ml_model_output_element"
        f"(id, create_user, create_time, "
        f"version_id, model_name, is_enabled)"
        f"values"
        f"('{element_id}', "
        f"'{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
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
        f"insert into ml_model_output_element "
        f"select id, create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', model_name, fields, remark, is_enabled "
        f"from ml_model_output_element "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql
