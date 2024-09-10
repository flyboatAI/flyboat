import asyncio
import json
import sys

from core.pipelining_engine import machine_learning_execute_engine
from element.abstract_element import AbstractElement
from enum_type.deleted import Deleted
from enum_type.element_config_type import ElementConfigType
from enum_type.result_code import ResultCode
from enum_type.store_type import StoreType
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.dag_helper import get_dag
from helper.data_store_helper import process_data_store
from helper.result_helper import ExecuteResult, process_cancel, process_success
from helper.sql_helper.init_sql_helper import db_helper1


class PipeliningElement(AbstractElement):
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
        管道算子实现
        输入端口: 多个D(data)端口
        输出端口: 多个D(data)端口，多个M(model)端口
        +--------+
        |        |
        D        D
        |        |
        D        M
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
        dag_dict = kwargs.get("dag_dict")
        element_name = f'{kwargs.get("element_name")}算子'

        # 根据配置获取 version_id
        version_sql = (
            f"select t2.origin_version_id "
            f"from ml_pipelining_element t1 "
            f"left join ml_pipelining_models t2 "
            f"on t1.pipelining_element_id = t2.id "
            f"where t1.version_id='{self.v_id}'"
        )

        version_dict = db_helper1.fetchone(version_sql)
        if not version_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"未找到{element_name}对应版本") from None
        version_id = version_dict["origin_version_id"]

        # 根据 dag_dict 整理 sync_input_data
        sync_input_sql = (
            f"select t1.json_key from ml_sync_element_config t1 "
            f"where t1.version_id='{version_id}' "
            f"and t1.element_type='{ElementConfigType.Input.value}' "
            f"and t1.is_deleted={Deleted.No.value} "
            f"order by t1.sort"
        )
        sync_input_arr = db_helper1.fetchall(sync_input_sql)
        sync_input_data = {}
        # 可能左侧数据锚点未全部连接, 需要处理
        if len(data_arr) != len(sync_input_arr) or not all(data_arr):
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}左侧数据未完全配置, " "请配置完毕重新执行",
            ) from None

        for i in range(len(sync_input_arr)):
            sync_input_data[sync_input_arr[i]["json_key"]] = data_arr[i]

        user_id = kwargs.get("user_id")
        dag_arr = get_dag(version_id)
        connect_id = kwargs.get("connect_id")
        websocket = kwargs.get("websocket")
        shared_mem = kwargs.get("shared_mem")
        serial_number = kwargs.get("serial_number")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        exec_result: ExecuteResult = loop.run_until_complete(
            machine_learning_execute_engine(
                sync_input_data,
                version_id,
                user_id,
                process_id,
                dag_arr,
                connect_id,
                websocket,
                shared_mem,
                dag_dict,
                child=True,
                serial_number=serial_number,
            )
        )
        loop.close()
        if exec_result.code == ResultCode.Error.value:
            raise ExecuteError(sys._getframe().f_code.co_name, exec_result.message) from None
        if exec_result.code == ResultCode.Cancel.value:
            return process_cancel()
        output = exec_result.data
        d = output.get("data")
        m = output.get("model")
        f = output.get("fields")
        # 处理返回结果
        sync_output_sql = (
            f"select t1.json_key from ml_sync_element_config t1 "
            f"where t1.version_id='{version_id}' "
            f"and t1.element_type='{ElementConfigType.Output.value}' "
            f"and t1.is_deleted={Deleted.No.value} "
            f"order by t1.sort"
        )
        sync_output_arr = db_helper1.fetchall(sync_output_sql)
        new_data_arr = []
        new_fields_arr = []

        path_dict = {}
        fields_dict = {}

        if sync_output_arr:
            for sync_output_key in sync_output_arr:
                k = sync_output_key["json_key"]
                if d and f and k in d and k in f:
                    new_data_arr.append(d[k])
                    new_fields_arr.append(f[k])
                    path_dict[k] = self.create_csv_file(d[k], f[k])
                    fields_dict[k] = f[k]

        sync_model_sql = (
            f"select t1.json_key from ml_sync_element_config t1 "
            f"where t1.version_id='{version_id}' "
            f"and t1.element_type='{ElementConfigType.ModelOutput.value}' "
            f"and t1.is_deleted={Deleted.No.value} "
            f"order by t1.sort"
        )
        sync_model_arr = db_helper1.fetchall(sync_model_sql)
        new_model_arr = []
        if sync_model_arr:
            for sync_model_key in sync_model_arr:
                k = sync_model_key["json_key"]
                if m and k in m:
                    new_model_arr.append(m[k])

        store_sql = process_data_store(
            process_id,
            self.v_id,
            self.e_id,
            self.u_id,
            StoreType.PipeliningElement.value,
        )
        path_dict_json = json.dumps(path_dict)
        fields_dict_json = json.dumps(fields_dict)
        store_result = self.insert_process_pipelining(store_sql, [path_dict_json, fields_dict_json])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None

        return process_success(data_arr=new_data_arr, fields_arr=new_fields_arr, model_arr=new_model_arr)

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
