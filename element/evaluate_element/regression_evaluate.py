import json
import sys

import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

import library_operator.library_operator
from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from enum_type.store_type import StoreType
from enum_type.user_data_type import UserDataType
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.fields_helper import generate_fields
from helper.matrix_helper import train_matrix_build
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class RegressionEvaluate(AbstractElement):
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
        回归评估算子实现, 指标支持:
        1. r2_score(R2)
        2. mean_squared_error(MSE 均方误差)
        3. root_mean_squared_error(RMSE 均方根误差)
        4. mean_absolute_error(MAE 平均绝对误差)
        输入端口: 一个M(model)端口、一个D(data)端口
        输出端口: 一个M(model)端口
        +--------+
        |        |
        M        M
        |        |
        D        D
        |        |
        +--------+
        :param process_id: 流水标识
        :param dependency_id_arr: 依赖的算子标识数组
        :param data_arr: 接收的数据数组
        :param fields_arr: 接收的数据列头数据数组
        :param model_arr: 接收的模型数组
        :param scaler_arr: 接收的缩放器数组
        :param role_arr: 接收的角色数组
        :return: 传递的模型、缩放器数组
        """
        element_name = f'{kwargs.get("element_name")}算子'

        UNUSED(fields_arr)
        role_settings = port(0, role_arr)
        data = port(0, data_arr)
        model_dict = port(0, model_arr)

        if not role_settings or not data or not model_dict:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}接收的数据或角色或字段为空",
            ) from None

        x_matrix, y_matrix = train_matrix_build(data, role_settings)

        model = model_dict["model"]
        library_type = model_dict["library_type"]
        y_prediction = library_operator.library_operator.predict(model, x_matrix, library_type).tolist()
        r2 = r2_score(y_matrix, y_prediction)
        mse = mean_squared_error(y_matrix, y_prediction)
        rmse = mean_squared_error(y_matrix, y_prediction, squared=False)
        mae = mean_absolute_error(y_matrix, y_prediction)
        icc_df, icc_fields = check_icc(y_matrix.flatten().tolist(), list(np.array(y_prediction).flatten()))
        evaluate_sql = (
            f"select evaluate_list "
            f"from ml_regression_evaluate_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and is_enabled={Enabled.Yes.value}"
        )
        evaluate_dict = db_helper1.fetchone(evaluate_sql, [])

        if not evaluate_dict or "evaluate_list" not in evaluate_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None

        evaluate_list_json = evaluate_dict["evaluate_list"]
        if not evaluate_list_json:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}输出标识未配置") from None
        evaluate_list = json.loads(evaluate_list_json)

        evaluate_fields = [
            generate_fields("evaluate", nick_name="指标", data_type=UserDataType.Varchar2.value),
            generate_fields("value", nick_name="值", data_type=UserDataType.Number.value),
        ]

        r2_evaluate_data = [{"evaluate": "r2", "value": r2}]

        mse_evaluate_data = [{"evaluate": "mse", "value": mse}]

        rmse_evaluate_data = [{"evaluate": "rmse", "value": rmse}]

        mae_evaluate_data = [{"evaluate": "mae", "value": mae}]

        r2_evaluate_relative_path = self.create_csv_file(r2_evaluate_data, evaluate_fields)
        mse_evaluate_relative_path = self.create_csv_file(mse_evaluate_data, evaluate_fields)
        rmse_evaluate_relative_path = self.create_csv_file(rmse_evaluate_data, evaluate_fields)
        mae_evaluate_relative_path = self.create_csv_file(mae_evaluate_data, evaluate_fields)

        icc_relative_path = self.create_csv_file(icc_df, icc_fields)
        evaluate_result = {
            "r2": r2_evaluate_relative_path,
            "mse": mse_evaluate_relative_path,
            "rmse": rmse_evaluate_relative_path,
            "mae": mae_evaluate_relative_path,
            "icc": icc_relative_path,
        }
        evaluate_result_fields = {
            "r2": evaluate_fields,
            "mse": evaluate_fields,
            "rmse": evaluate_fields,
            "mae": evaluate_fields,
            "icc": icc_fields,
        }

        required_evaluate_result = {}
        required_evaluate_result_fields = {}
        for evaluate in evaluate_list:
            required_evaluate_result[evaluate] = evaluate_result.get(evaluate)
            required_evaluate_result_fields[evaluate] = evaluate_result_fields.get(evaluate)

        store_sql = process_data_store(
            process_id,
            self.v_id,
            self.e_id,
            self.u_id,
            StoreType.RegressionEvaluate.value,
        )
        # fields_json = json.dumps(evaluate_fields)
        fields_json = json.dumps(required_evaluate_result_fields)
        store_result = self.insert_process_pipelining(store_sql, [json.dumps(required_evaluate_result), fields_json])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        all_data = {
            "r2": r2_evaluate_data,
            "mse": mse_evaluate_data,
            "rmse": rmse_evaluate_data,
            "mae": mae_evaluate_data,
            "icc": icc_df,
        }

        process_data_arr = [all_data[evaluate] for evaluate in evaluate_list]
        process_field_arr = [evaluate_result_fields[evaluate] for evaluate in evaluate_list]
        return process_success(
            data_arr=process_data_arr,
            fields_arr=process_field_arr,
            role_arr=[],
            scaler_arr=[],
            model_arr=model_arr,
        )

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)


# ICC组内相关系数
def check_icc(y_pre, y_true):
    import pandas as pd
    import pingouin as pg

    score = y_pre + y_true
    group = ["y_pre" for _ in range(len(y_pre))]
    group.extend(["y_true" for _ in range(len(y_true))])
    wine = [x + 1 for x in range(len(y_pre))]
    wine.extend([x + 1 for x in range(len(y_true))])
    data = {"wine": wine, "scores": score, "judge": group}
    df = pd.DataFrame(data)
    icc_df = pg.intraclass_corr(data=df, targets="wine", raters="judge", ratings="scores")
    field = {"Type": "类型", "Description": "描述", "ICC": "ICC值", "CI95%": "CI(95%)"}
    icc_fields = []
    for col_k in field.keys():
        icc_fields.append(generate_fields(col_k, nick_name=field[col_k], data_type=UserDataType.Varchar2.value))
    icc_df = icc_df[["Type", "Description", "ICC", "CI95%"]].to_dict("records")
    for icc in icc_df:
        icc["CI95%"] = icc["CI95%"].tolist()
    return icc_df, icc_fields
