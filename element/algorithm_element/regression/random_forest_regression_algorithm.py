import json
import sys

from sklearn.ensemble import RandomForestRegressor

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


class RandomForestRegressionAlgorithm(AbstractElement):
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
        随机森林回归算法算子实现
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

        if not data_arr or not role_arr or not fields_arr:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}接收的数据或角色或字段为空",
            ) from None

        random_forest_regression_sql = (
            f"select n_estimators, criterion, max_depth, min_samples_split, "
            f"min_samples_leaf, "
            f"min_weight_fraction_leaf, max_features, "
            f"max_leaf_nodes, min_impurity_decrease, "
            f"bootstrap, random_state "
            f"from ml_random_forest_regression_element "
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

        random_forest_regression_dict = db_helper1.fetchone(random_forest_regression_sql, [])
        if (
            not random_forest_regression_dict
            or "n_estimators" not in random_forest_regression_dict
            or "criterion" not in random_forest_regression_dict
            or "max_depth" not in random_forest_regression_dict
            or "min_samples_split" not in random_forest_regression_dict
            or "min_samples_leaf" not in random_forest_regression_dict
            or "min_weight_fraction_leaf" not in random_forest_regression_dict
            or "max_features" not in random_forest_regression_dict
            or "max_leaf_nodes" not in random_forest_regression_dict
            or "min_impurity_decrease" not in random_forest_regression_dict
            or "random_state" not in random_forest_regression_dict
            or "bootstrap" not in random_forest_regression_dict
        ):
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        """
        n_estimators: 决策树的数量，一般来说数量越多越好，但是会增加计算时间
        criterion: 用于衡量分裂质量的指标，一般用均方误差
        max_depth: 每棵树的最大深度，设为 None 表示树可以无限生长，小心过拟合
        min_samples_split: 进行划分所需的最小样本数，可以控制过拟合
        min_samples_leaf: 叶子节点所需的最小样本数，可以控制过拟合
        min_weight_fraction_leaf: 叶子节点所需的最小加权分数
        max_features: 搜索最佳分割时考虑的特征数量，一般设置为 'auto' 表示考虑所有特征
        max_leaf_nodes: 叶子节点的最大数量，用于防止过拟合
        min_impurity_decrease: 如果分裂导致杂质减少大于或等于这个值，则进行分裂
        bootstrap: 是否使用放回抽样
        random_state: 随机种子，设为固定的值可以复现随机性
        """
        n_estimators = random_forest_regression_dict["n_estimators"]
        criterion = random_forest_regression_dict["criterion"]
        max_depth = random_forest_regression_dict["max_depth"]
        min_samples_split = random_forest_regression_dict["min_samples_split"]
        min_samples_leaf = random_forest_regression_dict["min_samples_leaf"]
        min_weight_fraction_leaf = random_forest_regression_dict["min_weight_fraction_leaf"]
        max_features = random_forest_regression_dict["max_features"]
        max_leaf_nodes = random_forest_regression_dict["max_leaf_nodes"]
        min_impurity_decrease = random_forest_regression_dict["min_impurity_decrease"]
        bootstrap = random_forest_regression_dict["bootstrap"]
        random_state = random_forest_regression_dict["random_state"]
        # 判断max_features
        # noinspection PyBroadException
        try:
            if max_features != "sqrt" and max_features != "log2":
                if "." in max_features:
                    max_features = float(max_features)
                else:
                    max_features = int(max_features)
        except Exception:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}特征数量配置出错") from None
        if 1 < min_samples_split < 2:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                "随机森林分裂时考虑的节点分裂所需的最小样本数配置出错，" "取值范围不能在 1 到 2 之间",
            ) from None
        parameters = {
            "n_estimators": n_estimators,
            "criterion": criterion,
            "max_depth": max_depth,
            "min_samples_split": int(min_samples_split) if min_samples_split >= 2 else float(min_samples_split),
            "min_samples_leaf": int(min_samples_leaf) if min_samples_leaf >= 1 else float(min_samples_leaf),
            "min_weight_fraction_leaf": min_weight_fraction_leaf,
            "max_features": max_features,
            "max_leaf_nodes": int(max_leaf_nodes),
            "min_impurity_decrease": min_impurity_decrease,
            "bootstrap": False if not bootstrap else True,
            "random_state": int(random_state),
        }

        reg = RandomForestRegressor()
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
