import copy
import json
import sys
from typing import List

import pandas as pd

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from error.predict_error import PredictError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.error_helper import translate_error_message
from helper.matrix_helper import data_format_conversion
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class AnalogyEstimationAlgorithm(AbstractElement):
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
        类比法算法算子实现
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
        :return: 生产的模型
        """
        element_name = f'{kwargs.get("element_name")}算子'

        UNUSED(fields_arr, model_arr, process_id)
        if not data_arr or not role_arr or not fields_arr:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}接收的数据或角色或字段为空",
            ) from None
        analogy_estimation_sql = (
            f"select L, weight "
            f"from ml_analogy_estimation_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        # 根据角色数据组装训练数据
        train = port(0, data_arr)
        fields = port(0, fields_arr)

        pre_data = port(1, data_arr)
        pre_fields = port(1, fields_arr)
        role = port(0, role_arr)

        if not role:
            raise ExecuteError(sys._getframe().f_code.co_name, f"执行{element_name}前, 未进行角色配置") from None
        # 类比法格式转换dataframe
        x_role_arr, y_role_arr, train_data = data_format_conversion(train, role)
        # 追加数据
        if len(pre_fields) == len(fields) - 1:
            pre_fields_names = [field["name"] for field in pre_fields]
            fields_names = [field["name"] for field in fields]
            if set(pre_fields_names).issubset(set(fields_names)):
                for pre_dict in pre_data:
                    pre_dict[y_role_arr[0]] = 0
                    pre_df = pd.DataFrame([pre_dict])
                    train_data = pd.concat([train_data, pre_df], ignore_index=True, axis=0)
        elif len(pre_fields) == len(fields):
            pre_fields_names = [field["name"] for field in pre_fields]
            fields_names = [field["name"] for field in fields]
            if set(pre_fields_names) == set(fields_names):
                pre_df = pd.DataFrame(pre_data)
                train_data = pd.concat([train_data, pre_df], ignore_index=True, axis=0)
        else:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}输入数据不符, 请检查训练与测试数据是否匹配",
            ) from None
        if len(y_role_arr) > 1:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}配置因变量角色个数多余1") from None
        if len(y_role_arr) == 0:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置因变量角色") from None

        analogy_estimation_dict = db_helper1.fetchone(analogy_estimation_sql, [])
        if not analogy_estimation_dict or "l" not in analogy_estimation_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        """
        L: lambda 调整系数
        weight: 权重矩阵
        """
        L = analogy_estimation_dict["l"]
        # 传入的字符串转为字典类型
        w_matrix = json.loads(analogy_estimation_dict["weight"])
        # 区分训练数据和推理数据
        base_data_index = [x for x in range(len(train_data)) for y_role in y_role_arr if train_data[y_role][x] != 0]
        estimate_index = [x for x in range(len(train_data)) for y_role in y_role_arr if train_data[y_role][x] == 0]

        es = AnalogyEstimation(
            train_data=train_data,
            features_col=x_role_arr,
            label_col=y_role_arr,
            base_data_index=base_data_index,
            estimate_index=estimate_index,
        )

        try:
            # 计算beta
            beta = es.calc_distance(
                X_power=train_data,
                features_col=x_role_arr,
                label_col=y_role_arr,
                w_matrix=w_matrix,
            )
            # 预测数据
            budget = calc_budget(L=L, beta=beta)
            train.extend(pre_data)
            data = []
            for d in budget:
                if d in estimate_index:
                    y_role = y_role_arr[0]
                    result_data = copy.deepcopy(train[d])
                    result_data[y_role] = budget[d]
                    data.append(result_data)
            store_sql = process_data_store(
                process_id,
                self.v_id,
                self.e_id,
                self.u_id,
            )
            relative_path = self.create_csv_file(data, fields)
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
            return process_success(
                data_arr=[data],
                fields_arr=[fields],
                role_arr=[role] if role is not None else [],
                scaler_arr=scaler_arr,
            )
        except Exception as e:
            raise PredictError(translate_error_message(str(e))) from None

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)


# 基于类比法预测模型
def calc_budget(L, beta):
    # 从大到小排序beta
    result = {}
    for pre_i_set in beta:
        for k, v in pre_i_set.items():
            sorted_data = sorted(v, key=lambda x: list(x.keys())[0], reverse=True)

            if len(sorted_data) == 1:
                b1 = next(iter(sorted_data[0].keys()))
                e1 = sorted_data[0][b1]
                result[k] = L * (b1 * e1)
            elif len(sorted_data) == 2:
                b1 = next(iter(sorted_data[0].keys()))
                e1 = sorted_data[0][b1]

                b2 = next(iter(sorted_data[1].keys()))
                e2 = sorted_data[1][b2]

                result[k] = L * (b1 * e1 + (1 - b1) * b2 * e2)
            elif len(sorted_data) >= 3:
                b1 = next(iter(sorted_data[0].keys()))
                e1 = sorted_data[0][b1]

                b2 = next(iter(sorted_data[1].keys()))
                e2 = sorted_data[1][b2]

                b3 = next(iter(sorted_data[2].keys()))
                e3 = sorted_data[2][b3]
                result[k] = L * (
                    b1 * e1
                    + (1 - b1) * b2 * e2
                    + (1 - b1) * (1 - b2) * b3 * e3
                    + (1 / 3) * (1 - b1) * (1 - b2) * (1 - b3) * (e1 + e2 + e3)
                )

    return result


class AnalogyEstimation:
    def __init__(
        self,
        train_data,
        features_col,
        label_col,
        base_data_index: List[int],
        estimate_index: List[int],
    ):
        """
        基于类比法的预测模型
        :param train_data: 输入数据矩阵, 需包含基准装备数据、需预测的数据。
        :param label_col: 总费用列
        :param base_data_index: 基准数据的索引（例如：第一条数据为基准数据, 输入0即可）
        :param estimate_index: 需要估算的装备索引（例如：第2条数据为基准数据, 输入1即可）, 此变量为列表类型
        """
        if not isinstance(estimate_index, list):
            raise TypeError("输入参数必须为列表类型！") from None
        self.df = copy.deepcopy(train_data)
        self.features_col = features_col
        self.label_col = label_col
        self.base_index = base_data_index
        self.estimate_index = estimate_index

    def calc_weight_matrix(self):
        """
        计算基准装备的权重
        :return:
        """
        w_matrix = {}
        sum_FY = 0
        # 计算花费合计值
        for col in self.features_col:
            for b_index in self.base_index:
                sum_FY += self.df[col][b_index]

        # 计算分系统
        for col in self.features_col:
            w_matrix[col] = 0
            for b_index in self.base_index:
                w_matrix[col] += self.df[col][b_index] / sum_FY

        return w_matrix

    def calc_distance(self, X_power, features_col, label_col, w_matrix):
        """
        计算贴进度
        :return:
        """
        beta = []
        col_max = X_power.max()
        for b_index in self.base_index:
            for col in features_col:
                w = w_matrix[col]
                # X_power[col][b_index] = X_power[col][b_index] / col_max[col] * w
                X_power.loc[b_index, col] = X_power[col][b_index] / col_max[col] * w

        for estimate_i in self.estimate_index:
            beta_sub = []
            for b_index in self.base_index:
                A = 0
                B = 0
                for col in features_col:
                    A += min(
                        X_power[col][estimate_i] / col_max[col] * w_matrix[col],
                        X_power[col][b_index],
                    )
                    B += max(
                        X_power[col][estimate_i] / col_max[col] * w_matrix[col],
                        X_power[col][b_index],
                    )

                beta_sub.append({round(A / B, 5): float(X_power[label_col][b_index : b_index + 1].values)})

            beta.append({estimate_i: beta_sub})
        return beta
