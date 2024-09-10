import json
import sys

from statsmodels.tsa.arima.model import ARIMA

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from enum_type.role_type import RoleType
from enum_type.user_data_type import UserDataType
from error.execute_error import ExecuteError
from error.predict_error import PredictError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.error_helper import translate_error_message
from helper.fields_helper import generate_fields
from helper.matrix_helper import get_y_count, train_matrix_build
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class ARIMARegressionAlgorithm(AbstractElement):
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
        ARIMA 回归算法算子实现
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

        arima_regression_sql = (
            f"select p, d, q "
            f"from ml_arima_regression_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        # 根据角色数据组装训练数据
        role = port(0, role_arr)

        train = port(0, data_arr)
        single_data = port(1, data_arr)
        fields = port(0, fields_arr)
        if not role:
            raise ExecuteError(sys._getframe().f_code.co_name, f"执行{element_name}前, 未进行角色配置") from None
        x_matrix, y_matrix = train_matrix_build(train, role)
        y_count = get_y_count(role)
        if y_count != 1:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}因变量只能配置一个") from None

        arima_regression_dict = db_helper1.fetchone(arima_regression_sql, [])
        if (
            not arima_regression_dict
            or "p" not in arima_regression_dict
            or "d" not in arima_regression_dict
            or "q" not in arima_regression_dict
        ):
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        """
        p: 自回归（AR）的阶数, 表示模型考虑过去的几个时刻的数据
        d: 差分次数, 用于使时间序列稳定, 通常通过观察趋势和季节性来确定
        q: 移动平均（MA）的阶数, 表示模型考虑前几个误差的滞后值
        """
        p = arima_regression_dict["p"]
        d = arima_regression_dict["d"]
        q = arima_regression_dict["q"]
        if len(single_data) != 1 and "value" not in single_data[0]:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}第二个数据锚点必须是单值类型数据",
            ) from None
        step = single_data[0]["value"]
        reg = ARIMA(y_matrix, order=(p, d, q))

        try:
            m = reg.fit()
            result = m.predict(start=len(y_matrix), end=len(y_matrix) + step - 1)
            y_role_name = [x.get("name") for x in role if x.get("role_type") == RoleType.Y.value]
            # 获取y_name
            arima_result = []
            pred_list = list(result)
            y_name = y_role_name[0]
            for i in range(len(pred_list)):
                # 遍历列名
                d = {"step": i + 1, y_name: pred_list[i]}
                arima_result.append(d)
            arima_fields = [
                generate_fields("step", nick_name="步长", data_type=UserDataType.Number.value),
                generate_fields(
                    y_name,
                    nick_name=fields[next((index for (index, d) in enumerate(fields) if d["name"] == y_name))][
                        "nick_name"
                    ],
                    data_type=UserDataType.Number.value,
                ),
            ]
            store_sql = process_data_store(
                process_id,
                self.v_id,
                self.e_id,
                self.u_id,
            )
            relative_path = self.create_csv_file(arima_result, arima_fields)
            if arima_fields is None:
                arima_fields = []
            arima_fields_json = json.dumps(arima_fields)
            store_result = self.insert_process_pipelining(store_sql, [relative_path, arima_fields_json])
            if store_result != ResultCode.Success.value:
                raise StoreError(
                    sys._getframe().f_code.co_name,
                    self.u_id,
                    f"{element_name}过程数据存储失败",
                ) from None
            return process_success(
                data_arr=[arima_result],
                fields_arr=[arima_fields],
                role_arr=[role] if role is not None else [],
                scaler_arr=scaler_arr,
            )
        except Exception as e:
            raise PredictError(translate_error_message(str(e))) from None

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
