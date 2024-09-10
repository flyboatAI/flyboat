import json

from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from error.init_error import InitError
from helper.result_helper import execute_error, execute_success
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
                "model_id": "model_id",
                "model_name": "model_name",
                "fields":
                    [{
                        "name": "",
                        "nick_name": "",
                        "data_type": "NUMBER"
                    }]
              }
    """
    model_file_sql = (
        f"select t1.model_id, t2.name as model_name, t1.fields "
        f"from ml_model_file_element t1 "
        f"left join ml_models t2 on t1.model_id=t2.id "
        f"where t1.version_id='{version_id}' "
        f"and t1.id='{element_id}' "
        f"and is_enabled={Enabled.Yes.value}"
    )
    model_file_dict = db_helper1.fetchone(model_file_sql, [])
    if not model_file_dict:
        return execute_error()
    fields_json = model_file_dict["fields"]
    if not fields_json:
        model_file_dict["fields"] = json.loads(fields_json)
    return execute_success(data=model_file_dict)


def configuration(version_id, user_id, model_id, fields):
    """
    用于保存算子配置数据
    :param version_id: 版本标识
    :param user_id: 用户表示
    :param model_id: 输出模型标识
    :param fields: 输出模型的输出字段数组
    :return: 配置成功/失败
    """
    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    configuration_sql = generate_configuration_sql(version_id, user_id, model_id, fields)
    return db_helper1.execute_arr([configuration_sql])


def generate_configuration_sql(version_id, user_id, model_id, fields):
    """
    生成插入或更新算子 SQL 语句
    :param version_id: 版本标识
    :param user_id: 用户标识
    :param model_id: 输出模型标识
    :param fields: 输出模型的输出字段数组
    :return: SQL
    """
    now = current_time()
    if not model_id:
        model_id = ""
    if not fields:
        fields = []
    fields_json = json.dumps(fields)
    # 插入或更新
    configuration_sql = (
        f"merge into ml_model_file_element t1 "
        f"using (select "
        f"'{model_id}' id, "
        f"'{version_id}' version_id, "
        f"'{user_id}' user_id, "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss') create_time, "
        f"'{fields_json}' fields "
        f"'from dual) t2 on (t1.id = t2.id and t1.version_id=t2.version_id) "
        f"when matched then "
        f"update set t1.fields=t2.fields, "
        f"t1.is_enabled={Enabled.Yes.value}"
    )
    return configuration_sql


def init_element(version_id, element_id, user_id, node_type, **kwargs):
    """
    初始化算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param node_type: 节点类型
    :return: 配置成功/失败
    """
    init_sql = generate_init_sql(version_id, element_id, user_id, node_type, **kwargs)
    if init_sql is None:
        return ResultCode.Success.value
    return db_helper1.execute_arr([init_sql])


@valid_init_sql
def generate_init_sql(version_id, element_id, user_id, node_type, **kwargs):
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
    model_id = kwargs.get("model_id", None)
    if model_id is None:
        raise InitError("model_id 参数未配置") from None

    fields = kwargs.get("fields", [])
    fields = json.dumps(fields)
    # 初始化算子数据
    # noinspection SqlResolve
    init_sql = (
        f"insert into ml_model_file_element"
        f"(id, create_user, create_time, "
        f"version_id, model_id, fields, "
        f"is_enabled) "
        f"select "
        f"'{element_id}', "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"'{model_id}', "
        f"'{fields}', "
        f"{Enabled.Yes.value} "
        f"from dual"
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
    # 拷贝算子数据
    copy_sql = (
        f"insert into ml_model_file_element "
        f"select id, create_user, to_date('{current_time()}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', model_id, is_enabled, fields "
        f"from ml_model_file_element "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql
