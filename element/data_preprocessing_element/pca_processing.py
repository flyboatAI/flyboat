import json
import sys

import pandas as pd
from sklearn.decomposition import PCA

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
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class PCAProcessingElement(AbstractElement):
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
        PCA降维
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
        :return: 数据节点数组、模型节点数组、字段数组
        """
        element_name = f'{kwargs.get("element_name")}算子'

        UNUSED(model_arr)
        if not data_arr or not fields_arr:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}接收的数据或字段为空") from None
        pca_sql = (
            f"select n_components "
            f"from ml_pca_decomposition_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and is_enabled={Enabled.Yes.value}"
        )

        pca_processing_dict = db_helper1.fetchone(pca_sql, [])
        data = port(0, data_arr)
        role_setting_arr = port(0, role_arr)
        if not role_setting_arr:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}算子前, 未进行角色配置") from None

        n_components = pca_processing_dict["n_components"]
        if not pca_processing_dict or "n_components" not in pca_processing_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None

        # 获取x和y，将label从训练数据中提取
        feature_col = [x.get("name") for x in role_setting_arr if x.get("role_type") == RoleType.X.value]
        label_col = [x.get("name") for x in role_setting_arr if x.get("role_type") == RoleType.Y.value]
        df = pd.DataFrame(data)

        # 计算模型
        md = PCA(n_components=n_components)
        pca_data = md.fit_transform(df[feature_col])
        new_df = pd.DataFrame(pca_data, columns=[f"x_{i}" for i in range(1, n_components + 1)])
        new_df = pd.concat((new_df, df[label_col]), axis=1, ignore_index=True)
        new_fields = []
        for col in new_df.columns:
            new_fields.append(generate_fields(str(col), data_type=UserDataType.Number.value))

        relative_path = self.create_csv_file(new_df, new_fields)
        store_sql = process_data_store(
            process_id,
            self.v_id,
            self.e_id,
            self.u_id,
        )
        if new_fields is None:
            new_fields = []
        new_fields_json = json.dumps(new_fields)
        store_result = self.insert_process_pipelining(store_sql, [relative_path, new_fields_json])

        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None

        role = port(0, role_arr)
        return process_success(
            data_arr=[new_df],
            fields_arr=[new_fields],
            role_arr=[role] if role is not None else [],
        )

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
