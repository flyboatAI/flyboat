import json
import sys

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.polynomial_processing_helper import PolynomialProcessing, data_conversion
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class PolynomialProcessingElement(AbstractElement):
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
        多项式特征
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
        polynomial_sql = (
            f"select degree, interaction_only "
            f"from ml_polynomial_processing_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and is_enabled={Enabled.Yes.value}"
        )

        polynomial_processing_dict = db_helper1.fetchone(polynomial_sql, [])
        data = port(0, data_arr)
        fields = port(0, fields_arr)

        degree = polynomial_processing_dict["degree"]
        interaction_only = polynomial_processing_dict["interaction_only"]

        # 转换数据格式   ------------  -----------  --------------------
        features_col, df = data_conversion(label_role=fields, label_data=data)

        # 创建新的
        new_df, new_fields = PolynomialProcessing(
            features_col=[col["name"] for col in features_col], data_set=df
        ).run_processing(degree=degree, interaction_only=False if not interaction_only else True)
        relative_path = self.create_csv_file(new_df, new_fields)
        store_sql = process_data_store(
            process_id,
            self.v_id,
            self.e_id,
            self.u_id,
        )
        if new_fields is None:
            new_fields = []
        new_fields_json = json.dumps(new_fields)
        store_result = self.insert_process_pipelining(store_sql, [relative_path, new_fields_json])

        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        role = port(0, role_arr)

        return process_success(
            data_arr=[new_df],
            fields_arr=[new_fields],
            role_arr=[role] if role is not None else [],
            scaler_arr=scaler_arr,
        )

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
