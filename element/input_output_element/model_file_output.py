import json
import sys

from element.abstract_element import AbstractElement
from enum_type.deleted import Deleted
from enum_type.enabled import Enabled
from enum_type.model_type import ModelType
from enum_type.registered import Registered
from enum_type.result_code import ResultCode
from enum_type.store_type import StoreType
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.folder_helper import model_folder
from helper.generate_helper import generate_uuid, uuid_and_now
from helper.model_version_helper import version_handle
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED
from library_operator import library_operator


class ModelFileOutput(AbstractElement):
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
        模型文件输出算子实现
        输入端口: 一个M(model)端口
        输出端口: 无
        +--------+
        |        |
        |        |
        M        |
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
        :return: 输出成功 code=0；输出失败 code=1；
        """
        element_name = f'{kwargs.get("element_name")}算子'

        UNUSED(data_arr, fields_arr, role_arr, process_id)
        model_file_output_sql = (
            f"select model_name "
            f"from ml_model_output_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )
        model_file_output_dict = db_helper1.fetchone(model_file_output_sql, [])
        if not model_file_output_dict or "model_name" not in model_file_output_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        # 保存模型文件
        model_name = model_file_output_dict["model_name"]
        model_dict = port(0, model_arr)
        if not model_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}接收的模型为空") from None
        model_file_id, now = uuid_and_now()
        relative_path = model_folder()
        model = model_dict["model"]
        library_type = model_dict["library_type"]
        key = relative_path + model_file_id
        library_operator.save(model, key, library_type)
        # 插入模型仓库数据表
        model_count_sql = f"select count(*) as count from ml_models " f"where name='{model_name}'"
        model_count_dict = db_helper1.fetchone(model_count_sql, [])
        if not model_count_dict and "count" not in model_count_dict:
            model_count = 0
        else:
            model_count = model_count_dict["count"]
        new_version = version_handle(model_count)
        uid = generate_uuid()

        insert_model_sql = (
            f"insert into ml_models values('{uid}', "
            f"{Deleted.No.value}, "
            f"'{self.u_id}', "
            f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
            f"'{model_name}', "
            f"'{new_version}', "
            f"'{key}', "
            f"'{ModelType.Generate.value}', "
            f"'{library_type}', "
            f"{Registered.No.value}, "
            f"'{self.v_id}', "
            f"'{uid}', "
            f"'')"
        )

        insert_result = db_helper1.execute_arr([insert_model_sql])

        if insert_result != ResultCode.Success.value:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}插入模型表失败") from None

        store_sql = process_data_store(process_id, self.v_id, self.e_id, self.u_id, StoreType.ModelFileOutput.value)

        store_result = self.insert_process_pipelining(
            store_sql,
            [
                json.dumps({"model_name": model_name, "model_version": new_version}),
                None,
            ],
        )
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        return process_success()

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
