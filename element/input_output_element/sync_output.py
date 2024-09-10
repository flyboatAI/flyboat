import json
import sys

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class SyncOutput(AbstractElement):
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
        同步输出算子实现
        输入端口: 一个D(data)端口
        输出端口: 无
        +--------+
        |        |
        |        |
        D        |
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
        :return: 处理后/验证后的同步输出数据节点数组、模型节点数组、字段数组、角色数组
        """
        element_name = f'{kwargs.get("element_name")}算子'

        UNUSED(process_id, model_arr, scaler_arr, role_arr)
        data = port(0, data_arr)
        fields = port(0, fields_arr)
        sync_output_sql = (
            f"select t2.json_key as key "
            f"from ml_sync_output_element t1 "
            f"left join ml_sync_element_config t2 on "
            f"t1.id=t2.element_id "
            f"and t1.version_id=t2.version_id "
            f"where t1.id='{self.e_id}' and "
            f"t1.version_id='{self.v_id}' and t1.is_enabled={Enabled.Yes.value}"
        )
        sync_output_dict = db_helper1.fetchone(sync_output_sql, [])

        if not sync_output_dict or "key" not in sync_output_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        key = sync_output_dict["key"]
        if not key:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}输出标识未配置") from None
        # 由于 data 类型不确定，因此屏蔽在洞察页查看同步输出的数据，只能通过接口查看
        # 保存运行信息到记录表中，用于洞察中回看
        store_sql = process_data_store(process_id, self.v_id, self.e_id, self.u_id)

        if isinstance(data, dict):
            relative_path_dict = {}
            for k in data.keys():
                field = fields.get(k)
                if not field:
                    raise ExecuteError(
                        sys._getframe().f_code.co_name,
                        f"{element_name}输出数据与输出字段不匹配",
                    ) from None
                relative_path_dict[k] = self.create_csv_file(data[k], fields[k])
            relative_path = json.dumps(relative_path_dict)
        else:
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
        return process_success(data_arr=[{key: data}], fields_arr=[{key: fields}])

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
