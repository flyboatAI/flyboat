import json
import sys

import numpy as np
import pandas as pd
from factor_analyzer import FactorAnalyzer, calculate_bartlett_sphericity, calculate_kmo

from element.abstract_element import AbstractElement
from enum_type.result_code import ResultCode
from enum_type.store_type import StoreType
from enum_type.user_data_type import UserDataType
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.fields_helper import generate_fields
from helper.matrix_helper import matrix_data_format_conversion
from helper.polynomial_processing_helper import data_conversion
from helper.result_helper import process_success


class PCAAnalyze(AbstractElement):
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
        pca分析算子实现
        输入端口: 一个D(data)端口
        输出端口: 五个D(data)端口
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
            raise ExecuteError(sys._getframe().f_code.co_name, f"主{element_name}接收的数据或字段为空") from None

        data = port(0, data_arr)
        fields = port(0, fields_arr)
        # 转换dataframe格式
        features_col, df = data_conversion(label_role=fields, label_data=data)
        md = PCAAnalysis(data=df)
        # kmo 和 bartlett
        kmo_bartlett_table = md.kmo_bartlett()
        # 方差解释率表格
        variance_table = md.variance_table()
        # 载荷系数表格
        loading_table = md.loading_table()
        # 线性组合系数矩阵
        liner_table = md.liner_coefficient_table()
        eigenvalue = md.get_eigenvalue()
        store_sql = process_data_store(process_id, self.v_id, self.e_id, self.u_id, StoreType.PCA.value)
        eigenvalue_fields = [
            generate_fields("x", nick_name="成分", data_type=UserDataType.Number.value),
            generate_fields("y", nick_name="特征根", data_type=UserDataType.Number.value),
        ]
        kmo_bartlett_table_relative_path = self.create_csv_file(kmo_bartlett_table[0], kmo_bartlett_table[1])
        variance_table_relative_path = self.create_csv_file(variance_table[0], variance_table[1])
        loading_table_relative_path = self.create_csv_file(loading_table[0], loading_table[1])
        liner_table_relative_path = self.create_csv_file(liner_table[0], liner_table[1])
        eigenvalue_relative_path = self.create_csv_file(eigenvalue, eigenvalue_fields)
        path_dict = {
            "kmo": kmo_bartlett_table_relative_path,
            "variance_table": variance_table_relative_path,
            "loading_table": loading_table_relative_path,
            "liner_table": liner_table_relative_path,
            "eigenvalue": eigenvalue_relative_path,
        }
        pca_fields = {
            "kmo": kmo_bartlett_table[1],
            "variance_table": variance_table[1],
            "loading_table": loading_table[1],
            "liner_table": liner_table[1],
            "eigenvalue": eigenvalue_fields,
        }
        fields_json = json.dumps(pca_fields)

        store_result = self.insert_process_pipelining(store_sql, [json.dumps(path_dict), fields_json])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        return process_success(
            data_arr=[
                kmo_bartlett_table[0],
                variance_table[0],
                loading_table[0],
                liner_table[0],
                eigenvalue,
            ],
            fields_arr=[
                kmo_bartlett_table[1],
                variance_table[1],
                loading_table[1],
                liner_table[1],
                eigenvalue_fields,
            ],
        )

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)


class PCAAnalysis:
    def __init__(self, data):
        self.data = data
        self.std_data = data.apply(lambda x: x - x.mean())
        # 对DataFrame进行归一化
        self.featValue, self.n = self.calc_n()

    def calc_n(self):
        covX = np.around(np.corrcoef(self.data.T), decimals=3)
        _featValue, featVec = np.linalg.eig(covX.T)
        featValue = sorted(_featValue)[::-1]
        return featValue, len([x for x in featValue if x > 1])

    def kmo_bartlett(self):
        kmo_all, kmo_model = calculate_kmo(self.data)
        chi_square_value, p_value = calculate_bartlett_sphericity(self.data)
        temp_d = {
            "KMO": [round(kmo_model, 3)],
            "Bartlett 球形度检验 近似卡方": [round(chi_square_value, 3)],
            "Bartlett 球形度检验 P值": [round(p_value, 3)],
        }
        kmo_bartlett_matrix = pd.DataFrame(temp_d)
        kmo_bartlett_matrix = matrix_data_format_conversion(kmo_bartlett_matrix)
        return kmo_bartlett_matrix

    def variance_table(self):
        variance_value = {
            "特征根": [],
            "方差解释率": [],
            "累积方差解释率": [],
            "主成分_特征根": [],
            "主成分_方差解释率": [],
            "主成分_累积方差解释率": [],
        }
        variance_data = pd.DataFrame(variance_value)
        covX = np.around(np.corrcoef(self.data.T), decimals=3)
        _featValue, featVec = np.linalg.eig(covX.T)
        featValue = sorted(_featValue)[::-1]
        variance_data["特征根"] = featValue
        variance_data["方差解释率"] = np.round((featValue / np.sum(featValue)) * 100, 3)
        variance_data["累积方差解释率"] = np.cumsum(np.round((featValue / np.sum(featValue)) * 100, 3))

        # 主成分数量
        n_components = 0
        for _ in range(len(variance_data)):
            if variance_data["特征根"][_] >= 1:
                variance_data["主成分_特征根"][_] = variance_data["特征根"][_]
                variance_data["主成分_方差解释率"][_] = variance_data["方差解释率"][_]
                variance_data["主成分_累积方差解释率"][_] = variance_data["累积方差解释率"][_]
                n_components += 1

        variance_data = matrix_data_format_conversion(variance_data)
        return variance_data

    def loading_table(self):
        fa = FactorAnalyzer(n_factors=self.n, rotation=None, method="principal")
        fa.fit(self.data)
        col_name_list = ["主成分" + str(x + 1) for x in range(self.n)]

        loading_matrix = pd.DataFrame(np.around(fa.loadings_, 3), columns=col_name_list)
        loading_matrix["特征"] = self.data.columns
        loading_matrix["共同度（公因子方差）"] = fa.get_communalities()
        # 调换特征名称顺序
        loading_matrix = pd.concat([loading_matrix["特征"], loading_matrix.drop(columns="特征")], axis=1)
        loading_matrix = matrix_data_format_conversion(loading_matrix)
        return loading_matrix

    def liner_coefficient_table(self):
        fa = FactorAnalyzer(n_factors=self.n, rotation=None, method="principal")
        fa.fit(self.data)

        col_name_list = ["主成分" + str(x + 1) for x in range(self.n)]
        liner_coefficient_matrix = pd.DataFrame(
            np.around(fa.loadings_ / np.sqrt(fa.get_factor_variance()[0]).T, 4),
            columns=col_name_list,
            index=self.data.columns,
        )

        score = []
        for _ in range(len(liner_coefficient_matrix)):
            result = (
                np.sum((liner_coefficient_matrix[_ : _ + 1] * fa.get_factor_variance()[1]).values)
                / fa.get_factor_variance()[2][-1]
            )
            score.append(result)
        liner_coefficient_matrix["综合得分系数"] = score
        liner_coefficient_matrix["权重(%)"] = np.around(
            (liner_coefficient_matrix["综合得分系数"] / np.sum(liner_coefficient_matrix["综合得分系数"])) * 100,
            2,
        )
        liner_coefficient_matrix["特征"] = self.data.columns
        liner_coefficient_matrix = pd.concat(
            [
                liner_coefficient_matrix["特征"],
                liner_coefficient_matrix.drop(columns="特征"),
            ],
            axis=1,
        )
        liner_coefficient_matrix = matrix_data_format_conversion(liner_coefficient_matrix)
        return liner_coefficient_matrix

    def get_eigenvalue(self):
        covX = np.around(np.corrcoef(self.data.T), decimals=3)
        _featValue, featVec = np.linalg.eig(covX.T)
        featValue = sorted(_featValue)[::-1]

        eigenvalue = []
        for _ in range(len(featValue)):
            eigenvalue.append({"x": (_ + 1), "y": round(featValue[_], 3)})
        return eigenvalue
