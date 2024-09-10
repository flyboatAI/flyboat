import copy
import json

from config.dag_config import (
    PIPELINING_ELEMENT,
    SYNC_OUTPUT,
    TABLE_NAME,
    element_info_dict,
)
from enum_type.element_config_type import ElementConfigType
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from helper.pipelining_helper import get_sync_element_list
from helper.sql_helper.init_sql_helper import db_helper1


def port(num, arr):
    """
    获取端口数据
    :param num: 端口号
    :param arr: 数组
    :return: 该端口在数组中的数据
    """
    if isinstance(num, str):
        num = int(num)
    if arr and len(arr) > num:
        return copy.deepcopy(arr[num])
    return None


class PortResult:
    def __init__(self, code, fields, role):
        """
        初始化方法
        :param code: 结果码
        :param fields: 某一端口字段信息
        :param role: 某一端口角色信息
        """
        self.code = code
        self.fields = fields
        self.role = role
        pass


def get_fields_and_role(prev_node_output_port, prev_node_type, prev_element_id, version_id, helper):
    if prev_node_type == PIPELINING_ELEMENT:
        """
        特殊处理管道算子
        当前置算子时管道算子时, 取得该管道算子原始流水线的同步输出算子的 fields 和 roles
        将传入参数
        prev_element_id 变为原始流水线的同步输出算子的 id
        prev_node_output_port = 0
        prev_node_type = SYNC_OUTPUT
        version_id = origin_version_id
        """
        sql = (
            f"select origin_version_id from ml_pipelining_models "
            f"where id in (select pipelining_element_id from ml_pipelining_element "
            f"where id='{prev_element_id}')"
        )
        origin_version_id_dict = db_helper1.fetchone(sql)
        if not origin_version_id_dict:
            return PortResult(code=ResultCode.Error.value, fields=[], role=None)
        origin_version_id = origin_version_id_dict["origin_version_id"]
        sync_output_element_list = get_sync_element_list(origin_version_id, ElementConfigType.Output.value)
        sync_output_element = sync_output_element_list[prev_node_output_port]
        prev_element_id = sync_output_element["element_id"]
        prev_node_output_port = 0
        prev_node_type = SYNC_OUTPUT
        version_id = origin_version_id

    if not prev_element_id:
        return PortResult(code=ResultCode.Error.value, fields=[], role=None)
    element = element_info_dict.get(prev_node_type, None)
    if not element:
        return PortResult(code=ResultCode.Error.value, fields=[], role=None)
    table_name = element.get(TABLE_NAME)
    if not table_name:
        prev_output_sql = (
            f"select t1.fields_arr, t1.role_arr "
            f"from ml_element_output_record t1 "
            f"where t1.version_id='{version_id}' "
            f"and t1.element_id='{prev_element_id}' "
            f"order by t1.create_time desc"
        )
        prev_output_dict = helper.fetchone(prev_output_sql, [])
    else:
        # noinspection SqlResolve
        prev_output_sql = (
            f"select t1.fields_arr, t1.role_arr "
            f"from ml_element_output_record t1 "
            f"left join {table_name} t2 "
            f"on t1.element_id=t2.id "
            f"and t1.version_id=t2.version_id "
            f"where t1.version_id='{version_id}' "
            f"and t1.element_id='{prev_element_id}' "
            f"and t2.is_enabled={Enabled.Yes.value} "
            f"order by t1.create_time desc"
        )
        prev_output_dict = helper.fetchone(prev_output_sql, [])
    if (
        not prev_output_dict
        or "fields_arr" not in prev_output_dict
        or not prev_output_dict["fields_arr"]
        or "role_arr" not in prev_output_dict
    ):
        return PortResult(code=ResultCode.Error.value, fields=[], role=None)

    fields_arr = json.loads(prev_output_dict["fields_arr"])
    if prev_output_dict["role_arr"]:
        role_arr = json.loads(prev_output_dict["role_arr"])
    else:
        role_arr = []
    fields = port(prev_node_output_port, fields_arr)
    role = port(prev_node_output_port, role_arr)
    if not fields:
        return PortResult(code=ResultCode.Error.value, fields=[], role=None)
    return PortResult(code=ResultCode.Success.value, fields=fields, role=role)
