import json

from enum_type.user_data_type import UserDataType
from helper.data_store_helper import output_port_record_sql
from helper.fields_helper import generate_fields
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


def init_element(version_id, element_id, user_id, node_type):
    """
    初始化算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param node_type: 节点类型
    :return: 配置成功/失败
    """
    UNUSED(node_type)
    output_configuration_store_sql = output_port_record_sql(version_id, element_id, user_id)
    fields = [
        generate_fields("ci", nick_name="置信度", data_type=UserDataType.Number.value),
        generate_fields("value", nick_name="值", data_type=UserDataType.Number.value),
    ]
    fields_arr = [fields]
    fields_arr_json = json.dumps(fields_arr)
    role_arr_json = json.dumps([])
    return db_helper1.execute_arr([output_configuration_store_sql], {0: [fields_arr_json, role_arr_json]})
