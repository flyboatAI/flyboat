import json
import sys

import numpy as np
import pandas as pd
from scipy.stats import norm

from element.abstract_element import AbstractElement
from enum_type.result_code import ResultCode
from enum_type.user_data_type import UserDataType
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.fields_helper import generate_fields
from helper.result_helper import process_success


class CIAnalyze(AbstractElement):
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
        置信度分析算子实现
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
        # fields = port(0, fields_arr)
        fields = [
            generate_fields("ci", nick_name="置信度", data_type=UserDataType.Number.value),
            generate_fields("value", nick_name="值", data_type=UserDataType.Number.value),
        ]
        train_data = pd.DataFrame(data)
        train_data = train_data[train_data.columns[-1]]
        ci_result = calc_ci(train_data)
        fields_json = json.dumps(fields)
        store_sql = process_data_store(
            process_id,
            self.v_id,
            self.e_id,
            self.u_id,
        )
        relative_path = self.create_csv_file(ci_result, fields)
        store_result = self.insert_process_pipelining(store_sql, [relative_path, fields_json])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        return process_success(data_arr=[ci_result], fields_arr=[fields])

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)


def calc_ci(y_pre):
    x_bar = np.mean(y_pre)
    s = np.std(y_pre, ddof=1)
    ci_result = []

    d = [
        0.05,
        0.1,
        0.15,
        0.2,
        0.25,
        0.3,
        0.35,
        0.4,
        0.45,
        0.5,
        0.55,
        0.6,
        0.65,
        0.7,
        0.75,
        0.8,
        0.85,
        0.9,
        0.95,
    ]

    for confidence_level in d:
        t = {}
        z_critical = norm.ppf(1 - (1 - confidence_level) / 2)
        margin_of_error = z_critical * (s / np.sqrt(len(y_pre)))
        # confidence_interval = (x_bar - margin_of_error, x_bar + margin_of_error)
        confidence_interval = x_bar + margin_of_error
        t["ci"] = round(float(confidence_level), 2)
        t["value"] = confidence_interval
        ci_result.append(t)
    return ci_result
