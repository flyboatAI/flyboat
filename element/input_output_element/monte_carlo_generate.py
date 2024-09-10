import json
import sys

import numpy as np
import pandas as pd

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from enum_type.user_data_type import UserDataType
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.fields_helper import generate_fields
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class MonteCarloGenerateElement(AbstractElement):
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
        蒙卡数据生成
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
        monte_carlo_generate_sql = (
            f"select num_simulations, feature_value "
            f"from ml_monte_carlo_generate_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and is_enabled={Enabled.Yes.value}"
        )

        monte_carlo_generate_dict = db_helper1.fetchone(monte_carlo_generate_sql, [])

        num_simulations = monte_carlo_generate_dict["num_simulations"]
        feature_value = json.loads(monte_carlo_generate_dict["feature_value"])

        x_data = {}
        # 格式: { "name": "nick_name" }
        column_name = {}
        for temp_d in feature_value:
            col_name = temp_d["name"]
            column_name[temp_d["name"]] = temp_d["nick_name"]
            x_data[col_name] = np.random.uniform(temp_d["value"]["min"], temp_d["value"]["max"], num_simulations)
        x_train = pd.DataFrame(x_data)

        # 转换数据格式
        new_fields = []
        for col in x_train.columns:
            name = col
            nick_name = column_name.get(col, col)
            new_fields.append(generate_fields(name, nick_name, data_type=UserDataType.Number.value))
        new_df = x_train.to_dict("records")

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
