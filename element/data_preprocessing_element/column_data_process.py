import json
import sys

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from enum_type.user_data_type import UserDataType
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.column_data_process_helper import column_data_process
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class ColumnDataProcess(AbstractElement):
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
        列数据处理算子实现
        输入端口: 一个D(data)端口
        输出端口: 一个D(data)端口
        +--------+
        |        |
        |        |
        D        D
        |        |
        |        |
        +--------+
        :param process_id: 流水标识
        :param dependency_id_arr: 依赖的算子标识数组
        :param data_arr: 接收的数据数组
        :param fields_arr: 接收的数据列头数据数组
        :param model_arr: 接收的模型数组
        :param scaler_arr: 接收的缩放器数组
        :param role_arr: 接收的角色数组
        :return: 数据节点数组、模型节点数组、字段数组
        """
        element_name = f'{kwargs.get("element_name")}算子'

        UNUSED(model_arr)
        if not data_arr or not fields_arr:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}接收的数据或字段为空") from None
        data_process_sql = (
            f"select process_fields "
            f"from ml_column_data_process_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and is_enabled={Enabled.Yes.value}"
        )

        data_process_dict = db_helper1.fetchone(data_process_sql, [])

        if not data_process_dict or "process_fields" not in data_process_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None

        process_fields_json = data_process_dict["process_fields"]
        if not process_fields_json:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}过滤字段未配置") from None
        # [{"name": "", "process_type": "boxcox"}]
        process_fields = json.loads(process_fields_json)
        non_number_count = len([field for field in process_fields if field["data_type"] != UserDataType.Number.value])
        if non_number_count:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}处理字段存在非数值类型") from None
        data = port(0, data_arr)
        fields = port(0, fields_arr)
        # 处理后的数据
        processed_data = column_data_process(data, process_fields)
        store_sql = process_data_store(
            process_id,
            self.v_id,
            self.e_id,
            self.u_id,
        )
        relative_path = self.create_csv_file(processed_data, fields)
        if fields is None:
            fields = []
        fields_json = json.dumps(fields)
        store_result = self.insert_process_pipelining(store_sql, [relative_path, fields_json])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        role = port(0, role_arr)
        return process_success(
            data_arr=[processed_data],
            fields_arr=[fields],
            role_arr=[role] if role is not None else [],
            scaler_arr=scaler_arr,
        )

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
