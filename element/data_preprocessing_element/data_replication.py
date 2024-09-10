import json
import sys

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from enum_type.store_type import StoreType
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class DataReplication(AbstractElement):
    def element_process(
        self,
        process_id,
        dependency_id_arr,
        data_arr,
        fields_arr,
        role_arr,
        model_arr,
        scaler_arr,
        **kwargs,
    ):
        """
        动态数据复制算子实现
        输入端口: 一个D(data)端口
        输出端口: 多个D(data)端口
        +--------+
        |        |
        |        D
        D        |
        |        D
        |        |
        +--------+
        :param process_id: 流水标识
        :param dependency_id_arr: 依赖的算子标识数组
        :param data_arr: 接收的数据数组
        :param fields_arr: 接收的数据列头数据数组
        :param model_arr: 接收的模型数组
        :param scaler_arr: 接收的缩放器数组
        :param role_arr: 接收的角色数组
        :return: 数据节点数组
        """
        element_name = f'{kwargs.get("element_name")}算子'

        UNUSED(model_arr)
        if not data_arr:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}接收的数据为空") from None
        data_replication_sql = (
            f"select output_port_num, is_enabled "
            f"from ml_data_replication_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        data_replication_dict = db_helper1.fetchone(data_replication_sql, [])

        if not data_replication_dict or "output_port_num" not in data_replication_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None

        if not data_replication_dict["output_port_num"]:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}数量字段未配置") from None

        output_port_num_str = str(data_replication_dict["output_port_num"])
        if not output_port_num_str.isdigit():
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}数量字段为非数字类型") from None
        output_port_num = int(output_port_num_str)
        output_data = port(0, data_arr)
        output_fields = port(0, fields_arr)
        output_data_arr = [output_data] * output_port_num
        output_fields_arr = [output_fields] * output_port_num
        role = port(0, role_arr)
        output_role_arr = [role] * output_port_num if role is not None else []
        scaler = port(0, scaler_arr)
        output_scaler_arr = [scaler] * output_port_num if scaler is not None else []
        store_sql = process_data_store(process_id, self.v_id, self.e_id, self.u_id, StoreType.DataReplication.value)
        relative_path = self.create_csv_file(output_data, output_fields)
        if output_fields is None:
            output_fields = []
        output_fields_json = json.dumps(output_fields)
        store_result = self.insert_process_pipelining(store_sql, [relative_path, output_fields_json])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        return process_success(
            data_arr=output_data_arr,
            fields_arr=output_fields_arr,
            role_arr=output_role_arr,
            scaler_arr=output_scaler_arr,
        )

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
