import copy
import json
import sys

import numpy as np
import pandas as pd

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from enum_type.store_type import StoreType
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.matrix_helper import matrix_data_format_conversion
from helper.polynomial_processing_helper import data_conversion
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class GreyRelationAnalyze(AbstractElement):
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
        灰色关联度算子实现
        输入端口: 一个D(data)端口
        输出端口: 两个D(data)端口
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
        :return: 生产的模型
        """
        element_name = f'{kwargs.get("element_name")}算子'

        UNUSED(fields_arr, model_arr, process_id)
        if not data_arr or not role_arr or not fields_arr:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}接收的数据或角色或字段为空",
            ) from None

        grey_relation_analyze_sql = (
            f"select rho, weight "
            f"from ml_grey_relation_analyze_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        # 根据角色数据组装训练数据
        # role_setting_arr = port(0, role_arr)

        data = port(0, data_arr)
        fields = port(0, fields_arr)

        grey_relation_analyze_dict = db_helper1.fetchone(grey_relation_analyze_sql, [])
        if (
            not grey_relation_analyze_dict
            or "rho" not in grey_relation_analyze_dict
            or "weight" not in grey_relation_analyze_dict
        ):
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None

        # 转换dataframe格式
        features_col, df = data_conversion(label_role=fields, label_data=data)
        """
        rho: 区分度系数
        weight: 灰色关联度的特征权重
        """
        weight = json.loads(grey_relation_analyze_dict["weight"])
        rho = grey_relation_analyze_dict["rho"]
        # [{"name":xxx,"weight":xx},{}]
        w = []
        for col in df.columns:
            for d in weight:
                if d["name"] == col:
                    w.append(d["weight"])
        col_name = []
        for d in weight:
            col_name.append(d["name"])
        df = df[col_name]
        md = GreyRelationAnalysis(data=df, rho=rho, weight=w)
        grey_matrix_ranking, score_matrix = md.calc_rank()

        store_sql = process_data_store(process_id, self.v_id, self.e_id, self.u_id, StoreType.GreyRelation.value)
        grey_matrix_ranking_relative_path = self.create_csv_file(grey_matrix_ranking[0], grey_matrix_ranking[1])
        score_matrix_relative_path = self.create_csv_file(score_matrix[0], score_matrix[1])

        path_dict = {
            "grey_matrix_ranking": grey_matrix_ranking_relative_path,
            "score_matrix": score_matrix_relative_path,
        }

        grey_fields = {
            "grey_matrix_ranking": grey_matrix_ranking[1],
            "score_matrix": score_matrix[1],
        }
        grey_fields_json = json.dumps(grey_fields)

        store_result = self.insert_process_pipelining(store_sql, [json.dumps(path_dict), grey_fields_json])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        return process_success(
            data_arr=[grey_matrix_ranking[0], score_matrix[0]],
            fields_arr=[grey_matrix_ranking[1], score_matrix[1]],
        )

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)


class GreyRelationAnalysis:
    def __init__(self, data, rho=0.5, weight=None):
        self.rho = rho

        self.data = self.normalization_data(data)
        self.dim1, self.dim2 = self.data.shape
        self.sequence = self.compare_sequence(self.data)
        self.weight = weight

    @staticmethod
    def normalization_data(data):
        return (data - data.min()) / (data.max() - data.min())

    # noinspection PyUnresolvedReferences
    @staticmethod
    def compare_sequence(data):
        base_sequence = np.max(data, axis=0).values
        return base_sequence

    def grey_relation_coefficient(self):
        base_data = copy.deepcopy(self.data).values
        min1 = np.min(np.abs(self.sequence - base_data))
        max1 = np.max(np.abs(self.sequence - base_data))
        grey_relation_matrix = np.zeros((self.dim1, self.dim2))
        for i in range(self.dim1):
            for j in range(self.dim2):
                d = np.abs(self.sequence[j] - base_data[i, j])
                grey_relation_matrix[i, j] = (min1 + self.rho * max1) / (d + self.rho * max1)
        return grey_relation_matrix

    def grey_relation_degree(self, relation_matrix, w=None):
        if w is None:
            w = np.ones(self.dim2) / self.dim2
        grey_matrix = np.dot(relation_matrix, w)
        return grey_matrix

    def calc_rank(self):
        g_relation_matrix = self.grey_relation_coefficient()
        rank = self.grey_relation_degree(g_relation_matrix, w=self.weight)
        sort_index = np.argsort(-rank)
        grey_matrix_ranking = {"ranking": [], "样本": [], "灰色关联度": []}
        for _ in range(len(sort_index)):
            grey_matrix_ranking["ranking"].append(_ + 1)
            grey_matrix_ranking["样本"].append(sort_index[_] + 1)
            grey_matrix_ranking["灰色关联度"].append(round(rank[sort_index[_]], 3))
        # 计算分数
        score_list = []
        for _ in range(self.dim2):
            score_list.append(np.average(g_relation_matrix[:, _]) * self.weight[_])
        score = sum(score_list) / self.dim2
        grey_matrix_ranking = pd.DataFrame(grey_matrix_ranking)
        grey_matrix_ranking = matrix_data_format_conversion(grey_matrix_ranking)

        score_matrix = pd.DataFrame({"score": [score]})
        score_matrix = matrix_data_format_conversion(score_matrix)
        return grey_matrix_ranking, score_matrix
