import json
import sys

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.error_helper import translate_error_message
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1


class CustomFormula(AbstractElement):
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
        自定义公式算子实现
        输入端口: 多个D(data)端口
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
        :return: 生产的模型
        """
        element_name = f'{kwargs.get("element_name")}算子'

        if not data_arr:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}接收的数据为空") from None

        custom_formula_sql = (
            f"select formula_content "
            f"from ml_custom_formula_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        custom_formula_dict = db_helper1.fetchone(custom_formula_sql, [])
        if not custom_formula_dict or "formula_content" not in custom_formula_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        formula_content = custom_formula_dict["formula_content"]
        if not formula_content:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        connect_id = kwargs.get("connect_id")
        websocket = kwargs.get("websocket")
        params = {
            "connect_id": connect_id,
            "websocket": websocket,
            "data_arr": data_arr,
            "fields_arr": fields_arr,
            "output_data": None,
            "output_fields": None,
        }
        try:
            exec(formula_content, params)
        except Exception as e:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}内部: {translate_error_message(str(e))}",
            ) from None
        output_data = params["output_data"]
        output_fields = params["output_fields"]
        if output_data is None:
            output_data = []

        store_sql = process_data_store(
            process_id,
            self.v_id,
            self.e_id,
            self.u_id,
        )
        relative_path = self.create_csv_file(output_data, output_fields)
        if not output_fields:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}内未配置输出字段, 请重新配置",
            ) from None
        fields_json = json.dumps(output_fields)
        store_result = self.insert_process_pipelining(store_sql, [relative_path, fields_json])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None

        return process_success(data_arr=[output_data], fields_arr=[output_fields])

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
