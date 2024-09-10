import json
import sys

from element.abstract_element import AbstractElement
from enum_type.deleted import Deleted
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.result_helper import fetch_error, fetch_success, process_success
from helper.sql_helper.init_sql_helper import db_helper1, db_helper2
from helper.warning_helper import UNUSED


class DataModel(AbstractElement):
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
        数据模型算子实现
        输入端口: 无
        输出端口: 一个D(data)端口
        +--------+
        |        |
        |        |
        |        D
        |        |
        |        |
        +--------+
        :param process_id: 标识
        :param dependency_id_arr: 依赖的算子标识数组
        :param data_arr: 接收的数据数组
        :param fields_arr: 接收的数据列头数据数组
        :param model_arr: 接收的模型数组
        :param scaler_arr: 接收的缩放器数组
        :param role_arr: 接收的角色数组
        :return: 数据节点数组、模型节点数组、字段数组、角色数组
        """
        element_name = f'{kwargs.get("element_name")}算子'

        UNUSED(model_arr, scaler_arr, role_arr)
        fetch_result = self.fetch_table_name_and_id()
        if fetch_result.code != ResultCode.Success.value:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}获取数据表失败") from None
        table_name, _ = fetch_result.result

        # 获取列数据
        fetch_fields = db_helper2.fetch_fields_format(table_name)
        # 获取数据集
        data_sql = f"select * from {table_name}"
        origin_fetch_data = db_helper2.fetchall(data_sql)
        field_name_list = [f["name"] for f in fetch_fields]
        fetch_data = []
        for d in origin_fetch_data:
            fetch_data.append({key: d[key] for key in field_name_list if key in d})
        # 保存运行信息到记录表中，用于洞察中回看
        store_sql = process_data_store(
            process_id,
            self.v_id,
            self.e_id,
            self.u_id,
        )
        relative_path = self.create_csv_file(fetch_data, fetch_fields)
        if fetch_fields is None:
            fetch_fields = []
        fields_json = json.dumps(fetch_fields)
        store_result = self.insert_process_pipelining(store_sql, [relative_path, fields_json])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        return process_success(data_arr=[fetch_data], fields_arr=[fetch_fields])

    def fetch_table_name_and_id(self):
        """
        获取数据模型算子配置好的数据表名称和数据表标识
        :return: (code, result)
        """

        data_model_sql = (
            f"select data_model_id, data_model_type "
            f"from ml_data_model_element where "
            f"version_id='{self.v_id}' and "
            f"id='{self.e_id}' and is_enabled={Enabled.Yes.value}"
        )

        data_model_dict = db_helper1.fetchone(data_model_sql, [])
        if (
            not data_model_dict
            or data_model_dict.get("data_model_id") is None
            or data_model_dict.get("data_model_type") is None
        ):
            return fetch_error()
        data_model_id = data_model_dict.get("data_model_id")
        data_model_type = data_model_dict.get("data_model_type")
        sample_table_name_sql = (
            f"select remark as table_name from blade_dict_biz " f"where dict_key='{data_model_type}'"
        )
        sample_table_name_dict = db_helper1.fetchone(sample_table_name_sql)
        if not sample_table_name_dict or "table_name" not in sample_table_name_dict:
            return fetch_error()
        sample_table_name = sample_table_name_dict.get("table_name")

        # noinspection SqlResolve
        table_sql = (
            f"select t1.table_id, t2.name as table_name "
            f"from {sample_table_name} t1 "
            f"left join ml_ddm_table t2 on "
            f"t1.table_id=t2.id "
            f"where t1.id='{data_model_id}' and "
            f"t1.is_deleted={Deleted.No.value}"
        )

        table_dict = db_helper1.fetchone(table_sql)
        if not table_dict:
            return fetch_error()
        table_name = table_dict.get("table_name")
        table_id = table_dict.get("table_id")
        if not table_name or not table_id:
            return fetch_error()
        return fetch_success(result=(table_name, table_id))

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
