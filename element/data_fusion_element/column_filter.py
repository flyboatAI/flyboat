import json
import sys

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.column_filter_helper import column_filter, role_filter
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.min_max_helper import scaler_filter
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class ColumnFilter(AbstractElement):
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
        列过滤算子实现
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
        column_filter_sql = (
            f"select filter_fields, is_enabled "
            f"from ml_column_filter_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        column_filter_dict = db_helper1.fetchone(column_filter_sql, [])

        if not column_filter_dict or "filter_fields" not in column_filter_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None

        filter_fields_json = column_filter_dict["filter_fields"]
        if not filter_fields_json:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}过滤字段未配置") from None
        # [{"name": "", "nick_name": "", "data_type": "NUMBER"}]
        filter_fields = json.loads(filter_fields_json)

        data = port(0, data_arr)
        # 过滤后的数据
        filter_data = column_filter(data, filter_fields)
        store_sql = process_data_store(
            process_id,
            self.v_id,
            self.e_id,
            self.u_id,
        )
        relative_path = self.create_csv_file(filter_data, filter_fields)
        if filter_fields is None:
            filter_fields = []
        filter_fields_json = json.dumps(filter_fields)
        store_result = self.insert_process_pipelining(store_sql, [relative_path, filter_fields_json])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        role = port(0, role_arr)
        filter_role = None
        if role:
            filter_role = role_filter(role, filter_fields)
        scaler = port(0, scaler_arr)
        filter_scaler = None
        if scaler:
            filter_scaler = scaler_filter(scaler, filter_fields)
        return process_success(
            data_arr=[filter_data],
            fields_arr=[filter_fields],
            role_arr=[filter_role] if filter_role is not None else [],
            scaler_arr=[filter_scaler] if filter_scaler is not None else [],
        )

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
