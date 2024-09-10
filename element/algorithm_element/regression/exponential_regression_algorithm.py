import json
import sys

import numpy as np
from scipy.optimize import curve_fit

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.library_type import LibraryType
from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from error.predict_error import PredictError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.error_helper import translate_error_message
from helper.matrix_helper import get_x_count, get_y_count, train_matrix_build
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class ExponentialRegressionAlgorithm(AbstractElement):
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
        指数回归算子实现
        输入端口: 一个D(data)端口
        输出端口: 一个M(model)端口
        +--------+
        |        |
        |        |
        D        M
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
        exponential_regression_sql = (
            f"select a, b, c "
            f"from ml_exponential_regression_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        # 根据角色数据组装训练数据
        role_setting_arr = port(0, role_arr)
        train = port(0, data_arr)
        fields = port(0, fields_arr)
        if not role_setting_arr:
            raise ExecuteError(sys._getframe().f_code.co_name, f"执行{element_name}前, 未进行角色配置") from None

        x_count = get_x_count(role_setting_arr)
        y_count = get_y_count(role_setting_arr)
        if x_count != 1 or y_count != 1:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}自变量和因变量均只能配置一个",
            ) from None

        x_matrix, y_matrix = train_matrix_build(train, role_setting_arr)
        # 是否存在初始值标志
        initial_flag = True

        exponential_regression_dict = db_helper1.fetchone(exponential_regression_sql, [])
        if (
            not exponential_regression_dict
            or "a" not in exponential_regression_dict
            or "b" not in exponential_regression_dict
            or "c" not in exponential_regression_dict
        ):
            initial_flag = False
            if x_matrix.ndim > 1:
                raise ExecuteError(
                    sys._getframe().f_code.co_name,
                    f"{element_name}未配置完毕, 特征列过多",
                ) from None

        """
        a: a * np.exp(b * x) + c 中的a
        b: a * np.exp(b * x) + c 中的b
        c: a * np.exp(b * x) + c 中的c
        """
        a = exponential_regression_dict["a"]
        b = exponential_regression_dict["b"]
        c = exponential_regression_dict["c"]
        if initial_flag:
            initial_guess = [a, b, c]
        else:
            initial_guess = [1, 2, 3]
        md = ExponentModel()
        try:
            a, b, c = md.run_model(x_train=x_matrix, y_train=y_matrix, initial_guess=initial_guess)
            store_sql = process_data_store(
                process_id,
                self.v_id,
                self.e_id,
                self.u_id,
            )
            relative_path = self.create_csv_file(train, fields)
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
                model_arr=[
                    {
                        "model": {"a": a, "b": b, "c": c},
                        "library_type": LibraryType.ExponentialParameter.value,
                    }
                ]
            )
        except Exception as e:
            raise PredictError(translate_error_message(str(e))) from None

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)


class ExponentModel:
    # noinspection PyTupleAssignmentBalance
    def run_model(self, x_train, y_train, initial_guess):
        fit_result = curve_fit(
            self.exponential_func,
            np.array(x_train).flatten(),
            np.array(y_train).flatten(),
            p0=initial_guess,
        )
        popt, _ = fit_result
        a, b, c = popt
        return a, b, c

    @staticmethod
    def exponential_func(x, a, b, c):
        exp = np.exp(-b * x)
        if any(np.isinf(exp).tolist()):
            raise Exception("输入值过大导致数值溢出, 请利用归一化操作减小指数重新尝试") from None
        return a * np.exp(-b * x) + c

    def predict_model(self, x_data, a, b, c):
        # 使用拟合的参数进行预测
        predicted_y = self.exponential_func(x_data.flatten(), a, b, c)

        return predicted_y
