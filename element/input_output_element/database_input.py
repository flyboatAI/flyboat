import json
import sys

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED
from service.data_source.data_source_service import get_transient_sql_helper


class DatabaseInput(AbstractElement):
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
        数据源输入算子实现
        输入端口: 无
        输出端口: 一个D(data)端口
        +--------+
        |        |
        |        |
        |        D
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
        :return: 处理后/验证后的同步输入数据节点数组、模型节点数组、字段数组、角色数组
        """
        element_name = f'{kwargs.get("element_name")}算子'

        UNUSED(model_arr, scaler_arr, role_arr)
        database_input_sql = (
            f"select t1.datasource_id , t1.data_table_name "
            f"from ml_database_input_element t1 "
            f"where t1.id='{self.e_id}' and "
            f"t1.version_id='{self.v_id}' and "
            f"t1.is_enabled={Enabled.Yes.value}"
        )

        database_input_dict = db_helper1.fetchone(database_input_sql)
        if (
            not database_input_dict
            or "datasource_id" not in database_input_dict
            or "data_table_name" not in database_input_dict
        ):
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        datasource_id = database_input_dict["datasource_id"]
        # 选择表
        data_table_name = database_input_dict["data_table_name"]
        if not data_table_name:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置数据表字段") from None

        conn = get_transient_sql_helper(datasource_id)
        # 获取列数据
        fields = conn.fetch_fields_format(data_table_name)
        # 获取数据集
        data_sql = f"select * from {data_table_name}"
        data = conn.fetchall(data_sql)

        if data is None:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}获取数据失败") from None
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
        return process_success(data_arr=[data], fields_arr=[fields])

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
