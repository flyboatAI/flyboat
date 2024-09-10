import json
import sys

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from enum_type.role_type import RoleType
from enum_type.user_data_type import UserDataType
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.fields_helper import generate_fields
from helper.result_helper import process_error, process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED
from library_operator import library_operator


class SobolAnalyze(AbstractElement):
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
        sobol敏感度检测
        输入端口: 一个M(model)端口、一个D(data)端口
        输出端口: 一个D(data)端口
        +--------+
        |        |
        M        |
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

        sobol_analyze_sql = (
            f"select feature_value "
            f"from ml_sobol_analyze_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        # 根据角色数据组装训练数据

        model_dict = port(0, model_arr)

        role_setting_arr = port(0, role_arr)

        # [{"name":xxx,"max":xx,"min":xx}]
        sobol_analyze_dict = db_helper1.fetchone(sobol_analyze_sql, [])
        if not sobol_analyze_dict or "feature_value" not in sobol_analyze_dict:
            return process_error(message=f"{element_name}未配置完毕")

        feature_value = json.loads(sobol_analyze_dict["feature_value"])

        # 获取所有的数据列名
        feature_col = [x.get("name") for x in role_setting_arr if x.get("role_type") == RoleType.X.value]

        bounds = []
        for col in feature_col:
            for d in feature_value:
                if d.get("name") == col:
                    bounds.append([d.get("value").get("min"), d.get("value").get("max")])

        md = SobolSensitivityAnalysis(
            model=model_dict,
            num_vars=len(feature_col),
            names=feature_col,
            bounds=bounds,
        )
        md_result = md.run_analysis()

        fields = [
            generate_fields("name", nick_name="特征名称", data_type=UserDataType.Varchar2.value),
            generate_fields("value", nick_name="值", data_type=UserDataType.Number.value),
        ]

        fields_json = json.dumps(fields)
        store_sql = process_data_store(
            process_id,
            self.v_id,
            self.e_id,
            self.u_id,
        )
        relative_path = self.create_csv_file(md_result, fields)
        store_result = self.insert_process_pipelining(store_sql, [relative_path, fields_json])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        return process_success(data_arr=[md_result], fields_arr=[fields])

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)


# 灵敏度分析
class SensitivityAnalysis:
    def __init__(self, model, num_vars, names, bounds):
        self.model_dict = model
        self.num_vars = num_vars
        self.names = names
        self.bounds = bounds

    def run_analysis(self):
        pass

    def get_problem(self):
        # num_vars: 输入变量的数量。
        # names: 输入变量的名称列表，用于标识每个变量。
        # bounds: 每个输入变量的取值范围，通常表示为二维列表，其中每个子列表包含变量的最小值和最大值。
        problem = {
            "num_vars": self.num_vars,
            "names": list(self.names),
            "bounds": self.bounds,
        }
        return problem


# sobol灵敏度分析
class SobolSensitivityAnalysis(SensitivityAnalysis):
    def run_analysis(self):
        from SALib.analyze import sobol
        from SALib.sample import saltelli

        problem = self.get_problem()
        # 生成样例数据
        param_values = saltelli.sample(problem, 1000)
        # 使用模型计算x
        model = self.model_dict["model"]
        library_type = self.model_dict["library_type"]
        y_pre = library_operator.predict(model, param_values, library_type)
        y_pre = y_pre.reshape(-1)
        # 计算敏感度
        Si = sobol.analyze(problem, y_pre, calc_second_order=True)

        S1_result = []
        i = 0
        for col in problem["names"]:
            S1_result.append({"name": col, "value": Si["S1"][i]})
            i += 1
        return S1_result
