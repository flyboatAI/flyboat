import json
import sys

from sklearn.tree import DecisionTreeRegressor

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


class DecisionTreesRegressionAlgorithm(AbstractElement):
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
        决策树回归算法算子实现
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

        decision_trees_regression_sql = (
            f"select max_depth, min_samples_split, "
            f"min_samples_leaf, max_leaf_nodes, "
            f"max_features,criterion "
            f"from ml_decision_trees_regression_element "
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

        decision_trees_regression_dict = db_helper1.fetchone(decision_trees_regression_sql, [])
        if (
            not decision_trees_regression_dict
            or "max_depth" not in decision_trees_regression_dict
            or "min_samples_split" not in decision_trees_regression_dict
            or "min_samples_leaf" not in decision_trees_regression_dict
            or "max_features" not in decision_trees_regression_dict
            or "criterion" not in decision_trees_regression_dict
            or "max_leaf_nodes" not in decision_trees_regression_dict
        ):
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        """
        max_depth: 控制树的最大深度
        min_samples_split: 节点分裂所需的最小样本数
        min_samples_leaf: 叶子节点所需的最小样本数
        max_leaf_nodes: 最大叶子节点数量
        max_features: 分裂时考虑的最大特征数量（以2为底的对数）
        criterion: 衡量分裂质量的指标，均方误差
        """
        max_depth = decision_trees_regression_dict["max_depth"]
        min_samples_split = decision_trees_regression_dict["min_samples_split"]
        min_samples_leaf = decision_trees_regression_dict["min_samples_leaf"]
        max_leaf_nodes = decision_trees_regression_dict["max_leaf_nodes"]
        max_features = decision_trees_regression_dict["max_features"]
        criterion = decision_trees_regression_dict["criterion"]

        if 1 < min_samples_split < 2:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                "决策树分裂时考虑的节点分裂所需的最小样本数配置出错，" "取值范围不能在 1 到 2 之间",
            ) from None
        # 判断max_features
        # noinspection PyBroadException
        try:
            if not (max_features == "sqrt" or max_features == "log2"):
                if "." in max_features:
                    max_features = float(max_features)
                else:
                    max_features = int(max_features)
        except Exception:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                "决策树分裂时考虑的最大特征数量参数配置出错",
            ) from None
        parameters = {
            "max_depth": max_depth,
            "min_samples_split": int(min_samples_split) if min_samples_split >= 2 else float(min_samples_split),
            "min_samples_leaf": int(min_samples_leaf) if min_samples_leaf >= 1 else float(min_samples_leaf),
            "max_leaf_nodes": int(max_leaf_nodes),
            "max_features": max_features,
            "criterion": criterion,
        }

        reg = DecisionTreeRegressor()
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
