import json

from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from enum_type.user_data_type import UserDataType
from helper.data_store_helper import output_port_record_sql
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
                "evaluate_list": []
              }
    """
    evaluate_list_sql = (
        f"select evaluate_list "
        f"from ml_regression_evaluate_element "
        f"where version_id='{version_id}' "
        f"and id='{element_id}' "
        f"and is_enabled={Enabled.Yes.value}"
    )
    evaluate_list_dict = db_helper1.fetchone(evaluate_list_sql, [])
    if not evaluate_list_dict or "evaluate_list" not in evaluate_list_dict:
        return execute_success(data={"evaluate_list": []})
    evaluate_list_json = evaluate_list_dict["evaluate_list"]
    if evaluate_list_json:
        evaluate_list = json.loads(evaluate_list_json)
        return execute_success(data={"evaluate_list": evaluate_list})
    return execute_success(data={"evaluate_list": []})


def configuration(version_id, element_id, user_id, evaluate_list):
    """
    用于保存算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param evaluate_list: 评估列表
    :return: 配置成功/失败
    """
    fields = [
        generate_fields("evaluate", nick_name="指标", data_type=UserDataType.Varchar2.value),
        generate_fields("value", nick_name="值", data_type=UserDataType.Number.value),
    ]
    field = {"Type": "类型", "Description": "描述", "ICC": "ICC值", "CI95%": "CI(95%)"}
    icc_fields = []
    for col_k in field.keys():
        icc_fields.append(generate_fields(col_k, nick_name=field[col_k], data_type=UserDataType.Varchar2.value))
    evaluate_result_fields = {
        "r2": fields,
        "mse": fields,
        "mae": fields,
        "rmse": fields,
        "icc": icc_fields,
    }
    fields_arr = [evaluate_result_fields.get(evaluate) for evaluate in evaluate_list]
    role_arr = []
    fields_arr_json = json.dumps(fields_arr)
    role_arr_json = json.dumps(role_arr)

    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    output_configuration_store_sql = output_port_record_sql(version_id, element_id, user_id)

    if evaluate_list is None:
        evaluate_list = []
    evaluate_list_json = json.dumps(evaluate_list)

    configuration_sql = generate_configuration_sql(version_id, element_id, user_id)
    return db_helper1.execute_arr(
        [output_configuration_store_sql, configuration_sql],
        {
            0: [fields_arr_json, role_arr_json],
            1: [evaluate_list_json, evaluate_list_json],
        },
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
    sql = (
        f"merge into ml_regression_evaluate_element t1 "
        f"using (select "
        f"'{element_id}' id, "
        f"'{version_id}' version_id, "
        f"'{user_id}' user_id from dual) t2 on (t1.id = t2.id and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, "
        f"version_id, evaluate_list, "
        f"is_enabled) values"
        f"(t2.id, "
        f"t2.user_id, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, "
        f":1, "
        f"{Enabled.Yes.value}) "
        f"when matched then "
        f"update set t1.evaluate_list=:2, "
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
        f"insert into ml_regression_evaluate_element"
        f"(id, create_user, create_time, "
        f"version_id, evaluate_list, "
        f"is_enabled)"
        f"values"
        f"('{element_id}', "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
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
        f"insert into ml_regression_evaluate_element "
        f"select id, create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', evaluate_list, is_enabled "
        f"from ml_regression_evaluate_element "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql
