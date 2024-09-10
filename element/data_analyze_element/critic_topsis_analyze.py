import copy
import json
import math
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


class CriticTopsisAnalyze(AbstractElement):
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
        critic topsis分析算子实现
        输入端口: 一个D(data)端口
        输出端口: 三个D(data)端口
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
        :return: 数据节点数组、模型节点数组、字段数组、角色数组
        """
        element_name = f'{kwargs.get("element_name")}算子'

        if not data_arr or not fields_arr:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}接收的数据或字段为空") from None

        data = port(0, data_arr)
        fields = port(0, fields_arr)
        # 转换dataframe格式
        features_col, df = data_conversion(label_role=fields, label_data=data)

        md = CriticTopsis(data=df)
        # topsis 矩阵
        topsis_matrix, best_result = md.calc_topsis_matrix()
        # 正负理想解
        topsis_ideal_solutions = md.calc_topsis_ideal_solutions()
        store_sql = process_data_store(process_id, self.v_id, self.e_id, self.u_id, StoreType.CriticTopsis.value)
        topsis_matrix_relative_path = self.create_csv_file(topsis_matrix[0], topsis_matrix[1])
        best_result_matrix_relative_path = self.create_csv_file(best_result[0], best_result[1])
        topsis_ideal_solutions_matrix_relative_path = self.create_csv_file(
            topsis_ideal_solutions[0], topsis_ideal_solutions[1]
        )
        path_dict = {
            "topsis_matrix": topsis_matrix_relative_path,
            "best_result": best_result_matrix_relative_path,
            "topsis_ideal_solutions": topsis_ideal_solutions_matrix_relative_path,
        }
        topsis_fields = {
            "topsis_matrix": topsis_matrix[1],
            "best_result": best_result[1],
            "topsis_ideal_solutions": topsis_ideal_solutions[1],
        }
        topsis_fields_json = json.dumps(topsis_fields)

        store_result = self.insert_process_pipelining(store_sql, [json.dumps(path_dict), topsis_fields_json])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        return process_success(
            data_arr=[topsis_matrix[0], best_result[0], topsis_ideal_solutions[0]],
            fields_arr=[topsis_matrix[1], best_result[1], topsis_ideal_solutions[1]],
        )

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)


class CriticTopsis:
    def __init__(self, data):
        self.df = data
        # 计算加权矩阵
        _, self.data = self.calc_weight()
        self.positive_ideal = np.max(self.data, axis=0)
        self.negative_ideal = np.min(self.data, axis=0)
        self.D_plus = np.sqrt(np.sum((self.data - self.positive_ideal) ** 2, axis=1))
        self.D_minus = np.sqrt(np.sum((self.data - self.negative_ideal) ** 2, axis=1))
        self.C = self.D_minus / (self.D_plus + self.D_minus)

    def calc_weight(self):
        df = copy.deepcopy(self.df)
        critic_weight = {
            "指标变异性": np.around(np.std(df, ddof=1, axis=0), 3),
            "指标冲突性": np.around(np.sum(1 - df.corr("kendall").fillna(0), axis=1), 3),
        }
        critic_matrix = pd.DataFrame(critic_weight, index=df.columns)
        critic_matrix["信息量"] = critic_matrix["指标变异性"] * critic_matrix["指标冲突性"]

        critic_matrix["权重(%)"] = round(critic_matrix["信息量"] / np.sum(critic_matrix["信息量"]), 5)

        # 加权赋值
        for _ in range(len(df.columns)):
            df[df.columns[_]] = df[df.columns[_]] * critic_matrix["权重(%)"][_]
        return critic_matrix, df

    def calc_topsis_matrix(self):
        topsis_value = {
            "项": ["评价对象" + str(x) for x in range(1, len(self.data) + 1)],
            "正理想解D+": np.around(self.D_plus, 3),
            "负理想解D-": np.around(self.D_minus, 3),
            "相对接近度C": np.around(self.C, 3),
        }
        topsis_matrix = pd.DataFrame(topsis_value)
        topsis_matrix["排序结果"] = topsis_matrix["相对接近度C"].rank(ascending=False, method="first")
        x = topsis_matrix["相对接近度C"].idxmax()
        if math.isnan(x):
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                "相对接近度C为 nan, 无法进行计算, 请检查数据情况",
            ) from None
        best_result = topsis_matrix[topsis_matrix["相对接近度C"].idxmax() : topsis_matrix["相对接近度C"].idxmax() + 1]
        topsis_matrix = matrix_data_format_conversion(topsis_matrix)
        best_result = matrix_data_format_conversion(best_result)
        return topsis_matrix, best_result

    def calc_topsis_ideal_solutions(self):
        topsis_ideal_solution_value = {
            "正理想解A+": self.positive_ideal,
            "负理想解A-": self.negative_ideal,
        }
        topsis_ideal_solution_matrix = pd.DataFrame(topsis_ideal_solution_value)
        topsis_ideal_solution_matrix = matrix_data_format_conversion(topsis_ideal_solution_matrix)
        return topsis_ideal_solution_matrix
