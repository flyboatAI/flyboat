import json
import sys

from sklearn.svm import SVR

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
from helper.matrix_helper import train_matrix_build
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class SVRRegressionAlgorithm(AbstractElement):
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
        SVR回归算法算子实现
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

        svr_regression_sql = (
            f"select C, gamma, coef0, shrinking, max_iter, tol "
            f"from ml_svr_regression_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        # 根据角色数据组装训练数据
        role_setting_arr = port(0, role_arr)
        train = port(0, data_arr)
        # if not train or len(train) < 10:
        #     return process_error(message=f"执行该算法数据过少, 请重新配置数据")
        fields = port(0, fields_arr)
        if not role_setting_arr:
            raise ExecuteError(sys._getframe().f_code.co_name, f"执行{element_name}前, 未进行角色配置") from None
        x_matrix, y_matrix = train_matrix_build(train, role_setting_arr)

        svr_regression_dict = db_helper1.fetchone(svr_regression_sql, [])
        if (
            not svr_regression_dict
            or "c" not in svr_regression_dict
            or "gamma" not in svr_regression_dict
            or "coef0" not in svr_regression_dict
            or "shrinking" not in svr_regression_dict
            or "max_iter" not in svr_regression_dict
            or "tol" not in svr_regression_dict
        ):
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        """
        C: 惩罚项参数, 控制对误差的惩罚程度。较大的C表示模型对误差的容忍度较低。
        gamma: 核函数系数。可以是 scale, auto 或一个具体的值。
        coef0: 核函数的独立项。通常在多项式核和 sigmoid 核中使用。
        shrinking: 是否使用启发式收缩。如果设置为 True, 则会在每个步骤中考虑变量的数量, 从而加快运算速度。
        max_iter: 允许的误差容忍度。算法会在误差达到该值时停止迭代。
        tol: 求解器迭代的最大次数。-1表示没有限制。
        """
        C = svr_regression_dict["c"]
        gamma = svr_regression_dict["gamma"]
        coef0 = svr_regression_dict["coef0"]
        shrinking = svr_regression_dict["shrinking"]
        max_iter = svr_regression_dict["max_iter"]
        tol = svr_regression_dict["tol"]
        # noinspection PyBroadException
        try:
            if not (gamma == "scale" or gamma == "auto"):
                if "." in gamma:
                    gamma = float(gamma)
                else:
                    gamma = int(gamma)
        except Exception:
            raise ExecuteError(sys._getframe().f_code.co_name, "支持向量机核函数系数参数配置出错") from None
        parameters = {
            "C": C,
            "gamma": gamma,
            "coef0": coef0,
            "shrinking": False if not shrinking else True,
            "max_iter": max_iter,
            "tol": tol,
        }

        reg = SVR(kernel="linear")
        reg.set_params(**parameters)

        try:
            model = reg.fit(x_matrix, y_matrix)

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
            return process_success(model_arr=[{"model": model, "library_type": LibraryType.Sklearn.value}])
        except Exception as e:
            raise PredictError(translate_error_message(str(e))) from None

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
