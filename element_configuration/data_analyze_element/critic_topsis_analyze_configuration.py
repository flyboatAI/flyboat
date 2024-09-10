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
    fields_d = [
        generate_fields("项", nick_name="项", data_type=UserDataType.Varchar2.value),
        generate_fields("正理想解D+", nick_name="正理想解D+", data_type=UserDataType.Number.value),
        generate_fields("负理想解D-", nick_name="负理想解D-", data_type=UserDataType.Number.value),
        generate_fields("相对接近度C", nick_name="相对接近度C", data_type=UserDataType.Number.value),
        generate_fields("排序结果", nick_name="排序结果", data_type=UserDataType.Number.value),
    ]
    fields_a = [
        generate_fields("正理想解A+", nick_name="正理想解A+", data_type=UserDataType.Number.value),
        generate_fields("负理想解A-", nick_name="负理想解A-", data_type=UserDataType.Number.value),
    ]
    fields_arr = [fields_d, fields_d, fields_a]
    fields_arr_json = json.dumps(fields_arr)
    role_arr_json = json.dumps([])
    return db_helper1.execute_arr([output_configuration_store_sql], {0: [fields_arr_json, role_arr_json]})
