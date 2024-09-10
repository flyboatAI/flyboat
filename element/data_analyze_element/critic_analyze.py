import json
import sys

import numpy as np
import pandas as pd

from element.abstract_element import AbstractElement
from enum_type.result_code import ResultCode
from enum_type.store_type import StoreType
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.matrix_helper import matrix_data_format_conversion
from helper.polynomial_processing_helper import data_conversion
from helper.result_helper import process_success


class CriticAnalyze(AbstractElement):
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
        critic分析算子实现
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
        :return: 数据节点数组、模型节点数组、字段数组、角色数组
        """
        element_name = f'{kwargs.get("element_name")}算子'

        if not data_arr or not fields_arr:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}接收的数据或字段为空") from None

        data = port(0, data_arr)
        fields = port(0, fields_arr)
        # 转换dataframe格式
        features_col, df = data_conversion(label_role=fields, label_data=data)
        md = Critic(data=df)
        # critic权重矩阵
        critic_weight_matrix = md.critic_matrix()
        store_sql = process_data_store(process_id, self.v_id, self.e_id, self.u_id, StoreType.Critic.value)
        fields_json = json.dumps(critic_weight_matrix[1])
        print(critic_weight_matrix[1])

        critic_weight_matrix_relative_path = self.create_csv_file(critic_weight_matrix[0], critic_weight_matrix[1])

        store_result = self.insert_process_pipelining(store_sql, [critic_weight_matrix_relative_path, fields_json])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        return process_success(data_arr=[critic_weight_matrix[0]], fields_arr=[critic_weight_matrix[1]])

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)


class Critic:
    def __init__(self, data):
        self.data = data

    def critic_matrix(self):
        critic_weight = {
            "指标变异性": np.around(np.std(self.data, ddof=1, axis=0), 3),
            "指标冲突性": np.around(np.sum(1 - self.data.corr("kendall").fillna(0), axis=1), 3),
        }
        critic_matrix = pd.DataFrame(critic_weight)
        critic_matrix["信息量"] = critic_matrix["指标变异性"] * critic_matrix["指标冲突性"]

        critic_matrix["权重(%)"] = round(critic_matrix["信息量"] / np.sum(critic_matrix["信息量"]) * 100, 2)
        critic_matrix["特征"] = self.data.columns
        critic_matrix = pd.concat([critic_matrix["特征"], critic_matrix.drop(columns="特征")], axis=1)
        critic_matrix = matrix_data_format_conversion(critic_matrix)
        return critic_matrix
