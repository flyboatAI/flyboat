import json
import sys

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from enum_type.store_type import StoreType
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1


class TreePlotAnalyze(AbstractElement):
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
        树图分析算子实现
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

        # [{name, nick_name, data_type}]
        if not data_arr or not fields_arr:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}接收的数据或字段为空") from None

        column_sql = (
            f"select id_column, pid_column, name_column, value_column, is_enabled "
            f"from ml_tree_plot_analyze_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        column_sql_dict = db_helper1.fetchone(column_sql, [])
        if (
            "id_column" not in column_sql_dict
            or "pid_column" not in column_sql_dict
            or "name_column" not in column_sql_dict
            or "value_column" not in column_sql_dict
        ):
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        id_column = column_sql_dict["id_column"]
        pid_column = column_sql_dict["pid_column"]
        name_column = column_sql_dict["name_column"]
        value_column = column_sql_dict["value_column"]
        if not id_column or not pid_column or not name_column or not value_column:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}主键列或父子关系列或名称列或值列未配置",
            ) from None

        data = port(0, data_arr)

        store_sql = process_data_store(process_id, self.v_id, self.e_id, self.u_id, StoreType.Tree.value)
        relative_path = self.create_csv_file(data)
        fields_json = json.dumps(
            {
                "id_column": id_column,
                "pid_column": pid_column,
                "name_column": name_column,
                "value_column": value_column,
            }
        )
        store_result = self.insert_process_pipelining(store_sql, [relative_path, fields_json])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        return process_success(
            data_arr=data_arr,
            fields_arr=fields_arr,
            role_arr=role_arr,
            scaler_arr=scaler_arr,
        )

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
