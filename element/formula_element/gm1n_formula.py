import copy
import json
import math
import sys

import numpy as np
import pandas as pd

from element.abstract_element import AbstractElement
from enum_type.result_code import ResultCode
from enum_type.user_data_type import UserDataType
from error.execute_error import ExecuteError
from error.predict_error import PredictError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.error_helper import translate_error_message
from helper.fields_helper import generate_fields
from helper.matrix_helper import data_format_conversion
from helper.result_helper import process_success
from helper.warning_helper import UNUSED


class GM1NAlgorithm(AbstractElement):
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
        gm1n回归算法算子实现
        输入端口: 两个D(data)端口
        输出端口: 一个D(data)端口
        +--------+
        |        |
        D        |
        |        D
        D        |
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

        # 根据角色数据组装训练数据
        role_setting_arr = port(0, role_arr)

        train = port(0, data_arr)
        fields = port(0, fields_arr)

        test_data = port(1, data_arr)
        pre_fields = port(1, fields_arr)
        if not role_setting_arr:
            raise ExecuteError(sys._getframe().f_code.co_name, f"执行{element_name}前, 未进行角色配置") from None
        if not train:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}的训练数据为空, 请检查锚点连接配置",
            ) from None
        if not test_data:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}的推理数据为空, 请检查锚点连接配置",
            ) from None
        x_role_arr, y_role_arr, train_data = data_format_conversion(train, role_setting_arr)
        train_data = train_data[x_role_arr + y_role_arr]
        # 追加数据
        if len(pre_fields) == len(x_role_arr):
            pre_fields_names = [field["name"] for field in pre_fields]
            fields_names = [field["name"] for field in fields]
            if set(pre_fields_names).issubset(set(fields_names)):
                for pre_dict in test_data:
                    pre_dict[y_role_arr[0]] = 0
                    pre_df = pd.DataFrame([pre_dict])
                    train_data = pd.concat([train_data, pre_df], ignore_index=True, axis=0)
        elif len(pre_fields) == len(x_role_arr) + 1:
            pre_fields_names = [field["name"] for field in pre_fields]
            fields_names = [field["name"] for field in fields]
            if set(pre_fields_names) == set(fields_names):
                pre_df = pd.DataFrame(test_data)
                train_data = pd.concat([train_data, pre_df], ignore_index=True, axis=0)
        else:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}输入数据不符, 请检查训练与测试数据是否匹配",
            ) from None

        step = len(test_data)
        md = GM1N(data=train_data, feature_col=x_role_arr, label_col=y_role_arr, pre_step=step)
        try:
            _, _, y_pre_data, y_pre_fields = md.run()

            relative_path = self.create_csv_file(y_pre_data, y_pre_fields)
            store_sql = process_data_store(
                process_id,
                self.v_id,
                self.e_id,
                self.u_id,
            )
            if y_pre_fields is None:
                y_pre_fields = []
            new_fields_json = json.dumps(y_pre_fields)
            store_result = self.insert_process_pipelining(store_sql, [relative_path, new_fields_json])

            if store_result != ResultCode.Success.value:
                raise StoreError(
                    sys._getframe().f_code.co_name,
                    self.u_id,
                    f"{element_name}数据存储失败",
                ) from None

            # role = port(0, role_arr)
            return process_success(
                data_arr=[y_pre_data],
                fields_arr=[y_pre_fields],
                role_arr=[],
            )
        except Exception as e:
            raise PredictError(translate_error_message(str(e))) from None

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)


class GM1N:
    def __init__(self, data, feature_col, label_col, pre_step=1):
        self.data = data
        self.feature_col = feature_col
        self.label_col = label_col
        self.step = pre_step
        self.row_num, self.col_num = self.data.shape
        # 分割数据
        self.x_train = self.data[self.feature_col]
        self.y_train = self.data[self.label_col].iloc[: self.row_num - self.step]

        # 确定基础值
        self.base_y_data = float(self.y_train.values[0])

    def calc(self):
        # 计算累加X
        x_ago = copy.deepcopy(self.x_train)
        for col in x_ago.columns:
            x_ago[col] = np.cumsum(x_ago[col].values)
        # 计算累加Y
        y_ago = np.cumsum(self.y_train.values)

        # 计算紧邻序列Z
        Z = []
        for i in range(1, len(y_ago)):
            Z.append((y_ago[i] + y_ago[i - 1]) / 2)

        # 计算Y
        Y = np.array([self.y_train[1 : self.row_num - self.step].values]).reshape(-1, 1)

        # 计算B
        # 添加第一列B
        B = np.array([x * -1 for x in Z]).reshape(-1, 1)
        x_b = x_ago.iloc[1 : self.row_num - self.step, :]
        B = np.hstack((B, x_b))
        # 计算alpha 和beta
        y = np.matmul(np.linalg.pinv(np.matmul(B.T, B)), np.matmul(B.T, Y))
        alpha = float(y[:1, :].flatten()[0])
        beta = y[1:, :].flatten()
        # 计算U
        U = []
        for i in range(self.row_num):
            s = 0
            for col_i in range(self.col_num - 1):
                s += float(x_ago.iloc[i : i + 1, col_i : col_i + 1].values) * float(beta[col_i])
            U.append(s)

        return {"alpha": alpha, "beta": beta, "u": U}

    def pre(self, params):
        # 计算公式
        f = [self.base_y_data]
        for i in range(1, self.row_num + 1):
            r = self.formula(
                x=self.base_y_data,
                u=params["u"][i - 1],
                alpha=params["alpha"],
                pre_index=i,
            )
            f.append(r)
        del f[0]
        # 计算差值返回结果
        result = [f[0]]
        for i in range(1, self.row_num):
            result.append(float(f[i] - f[i - 1]))
        return result

    @staticmethod
    def formula(x, u, alpha, pre_index):
        return (x - u / alpha) / math.exp(alpha * pre_index) + u / alpha

    def run(self):
        params = self.calc()
        r = self.pre(params)
        y_pre = r[-self.step :]
        # 处理格式
        y_pre_data = pd.DataFrame({"gm1n_pre": list(np.array(r).round(3))})
        y_pre_data[self.feature_col] = self.data[self.feature_col]
        # y_pre_data['mse'] = [np.nan for _ in range(len(y_pre_data))]
        # y_pre_data['mse'] = list(np.around(np.mean(np.power(y_pre_data['灰度预测值'] - y_pre_data['原始值']), 2)))
        y_pre_fields = []
        for col in self.feature_col:
            y_pre_fields.append(generate_fields(str(col), data_type=UserDataType.Number.value))
        y_pre_fields.append(generate_fields("gm1n_pre", nick_name="灰度预测值", data_type=UserDataType.Number.value))
        # y_pre_data = [{"gm1n_pre": pre} for pre in list(np.array(r).round(3))]
        # y_pre_fields = [generate_fields('gm1n_pre', nick_name='灰度预测值', data_type=UserDataType.Number.value)]
        # for col in self.data.columns:
        #     y_pre_fields.append(generate_fields(col, nick_name=col, data_type=UserDataType.Number.value))
        # y_pre_data = self.data.to_dict(orient='records')

        return r, y_pre, y_pre_data[-self.step :], y_pre_fields
