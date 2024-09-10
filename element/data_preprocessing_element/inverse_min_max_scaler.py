import json
import sys

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from enum_type.user_data_type import UserDataType
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.min_max_helper import (
    concat_min_max_data,
    inverse_min_max_scaler,
    min_max_filter_column_data,
)
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class InverseMinMaxScalerElement(AbstractElement):
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
        逆归一化算子实现
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
        inverse_min_max_scaler_sql = (
            f"select inverse_min_max_fields "
            f"from ml_inverse_min_max_scaler_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and is_enabled={Enabled.Yes.value}"
        )

        inverse_min_max_scaler_dict = db_helper1.fetchone(inverse_min_max_scaler_sql, [])

        if not inverse_min_max_scaler_dict or "inverse_min_max_fields" not in inverse_min_max_scaler_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None

        inverse_min_max_fields_json = inverse_min_max_scaler_dict["inverse_min_max_fields"]
        if not inverse_min_max_fields_json:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}逆归一字段未配置") from None
        inverse_min_max_fields = json.loads(inverse_min_max_fields_json)
        non_number_count = len(
            [field for field in inverse_min_max_fields if field["data_type"] != UserDataType.Number.value]
        )
        if non_number_count:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}逆归一字段存在非数值类型",
            ) from None
        data = port(0, data_arr)
        fields = port(0, fields_arr)
        scaler = port(0, scaler_arr)
        if not scaler:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}前置算子不存在归一化操作, "
                f"或者前置算子的输出字段与配置归一化操作时的配置字段"
                f"已经发生变化, "
                f"不允许逆归一化操作",
            ) from None
        # 根据数据库配置的逆归一化字段, 处理数据
        filter_data = min_max_filter_column_data(data, inverse_min_max_fields)

        if not filter_data or len(filter_data[0].keys()) != len(scaler.keys()):
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}配置字段与归一化操作配置字段个数不匹配",
            ) from None
        # 逆归一化数据
        inverse_min_max_data = inverse_min_max_scaler(filter_data, scaler)

        # 重新组合数据
        inverse_transform_data = concat_min_max_data(inverse_min_max_data, data)

        store_sql = process_data_store(
            process_id,
            self.v_id,
            self.e_id,
            self.u_id,
        )
        relative_path = self.create_csv_file(inverse_transform_data, fields)
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
            data_arr=[inverse_transform_data],
            fields_arr=[fields],
            role_arr=[role] if role is not None else [],
        )

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
