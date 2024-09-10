import json
import sys

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.func_parameter_helper import type_convert
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1


class CustomAlgorithmFile(AbstractElement):
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
        自定义算法文件算子实现
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

        if not data_arr or not fields_arr:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}接收的数据或字段为空") from None

        data = port(0, data_arr)
        # if not data or len(data) < 10:
        #     return process_error(message=f"执行该算法数据过少, 请重新配置数据")
        fields = port(0, fields_arr)
        role = port(0, role_arr)
        custom_algorithm_sql = (
            f"select t2.algorithm_content as code, t2.library_type "
            f"from ml_custom_algorithm_element t1 "
            f"left join ml_algorithm t2 "
            f"on t1.algorithm_id=t2.id "
            f"where t1.id='{self.e_id}' and "
            f"t1.version_id='{self.v_id}' and "
            f"t1.is_enabled={Enabled.Yes.value}"
        )

        custom_algorithm_dict = db_helper1.fetchone(custom_algorithm_sql, [])
        if not custom_algorithm_dict or "code" not in custom_algorithm_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        code = custom_algorithm_dict["code"]
        library_type = custom_algorithm_dict["library_type"]
        if not code:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置代码") from None

        algorithm_params_sql = (
            f"select t2.name, t1.value, t2.is_array, t2.parameter_data_type "
            f"from ml_custom_algorithm_config t1 "
            f"left join ml_algorithm_params t2 "
            f"on t1.param_id=t2.id "
            f"where t1.version_id='{self.v_id}' "
            f"and t1.element_id='{self.e_id}'"
        )

        params_arr = db_helper1.fetchall(algorithm_params_sql)
        parameter = {}
        if params_arr:
            for param in params_arr:
                value = param.get("value")
                parameter_data_type = param.get("parameter_data_type", str)
                is_array = param.get("is_array")
                if is_array and value:
                    parameter[param["name"]] = value.split(",")
                elif is_array:
                    parameter[param["name"]] = []
                else:
                    parameter[param["name"]] = type_convert(value, parameter_data_type)

        params = {
            "data": data,
            "role": role,
            "parameter": parameter,
            "model": None,
        }

        exec(code, params)

        model = params["model"]
        if not model:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}生成模型失败") from None

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
        return process_success(model_arr=[{"model": model, "library_type": library_type}])

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
