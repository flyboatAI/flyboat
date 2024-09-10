import json
import sys

from sklearn.cross_decomposition import PLSRegression

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


class PLSRegressionAlgorithm(AbstractElement):
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
        偏最小二乘回归算法算子实现
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

        pls_regression_sql = (
            f"select n_components, max_iter, scale, tol "
            f"from ml_pls_regression_element "
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

        pls_regression_dict = db_helper1.fetchone(pls_regression_sql, [])
        if (
            not pls_regression_dict
            or "n_components" not in pls_regression_dict
            or "max_iter" not in pls_regression_dict
            or "scale" not in pls_regression_dict
            or "tol" not in pls_regression_dict
        ):
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        """
        n_components: 保留组件数
        max_iter: 最大迭代次数
        scale: 是否缩放
        tol: 公差
        """
        n_components = pls_regression_dict["n_components"]
        max_iter = pls_regression_dict["max_iter"]
        scale = pls_regression_dict["scale"]
        tol = pls_regression_dict["tol"]
        parameters = {
            "n_components": n_components,
            "max_iter": max_iter,
            "scale": False if not scale else True,
            "tol": tol,
        }
        reg = PLSRegression()
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
