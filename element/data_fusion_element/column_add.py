import json
import sys

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.column_add_helper import merge_data
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.fields_helper import merge_fields
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class ColumnAdd(AbstractElement):
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
        列新增算子实现
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
        if not data_arr:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}接收的数据为空") from None
        column_add_sql = (
            f"select add_fields, is_enabled "
            f"from ml_column_add_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        column_add_dict = db_helper1.fetchone(column_add_sql, [])

        if not column_add_dict or "add_fields" not in column_add_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None

        add_fields_json = column_add_dict["add_fields"]
        if not add_fields_json:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}新增字段未配置") from None
        # [{"name": "", "nick_name": "", "data_type": "NUMBER", "expression": ""}]
        add_fields = json.loads(add_fields_json)

        data = port(0, data_arr)
        fields = port(0, fields_arr)
        merged_fields = merge_fields(fields, add_fields)
        # 过滤后的数据
        merged_data = merge_data(data, add_fields)
        store_sql = process_data_store(
            process_id,
            self.v_id,
            self.e_id,
            self.u_id,
        )
        relative_path = self.create_csv_file(merged_data, merged_fields)
        if merged_fields is None:
            merged_fields = []
        merged_fields_json = json.dumps(merged_fields)
        store_result = self.insert_process_pipelining(store_sql, [relative_path, merged_fields_json])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        role = port(0, role_arr)
        return process_success(
            data_arr=[merged_data],
            fields_arr=[merged_fields],
            role_arr=[role] if role is not None else [],
            scaler_arr=scaler_arr,
        )

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
