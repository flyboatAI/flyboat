import json
import sys

from element.abstract_element import AbstractElement
from enum_type.result_code import ResultCode
from enum_type.store_type import StoreType
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED
from library_operator import library_operator


class ModelFile(AbstractElement):
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
        自定义模型文件算子实现
        输入端口: 无
        输出端口: 一个M(model)端口
        +--------+
        |        |
        |        |
        |        M
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
        :return: 模型
        """
        element_name = f'{kwargs.get("element_name")}算子'

        UNUSED(model_arr, fields_arr, role_arr, process_id)
        model_sql = (
            f"select t1.id, t1.name as model_name, t1.oss_path as model_path, "
            f"t1.library_type "
            f"from ml_models t1 "
            f"left join ml_model_file_element t5 "
            f"on t1.id=t5.model_id "
            f"where t5.id='{self.e_id}' and "
            f"t5.version_id='{self.v_id}'"
        )
        model_file_dict = db_helper1.fetchone(model_sql, [])
        if not model_file_dict or "id" not in model_file_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        model_id = model_file_dict["id"]
        model_name = model_file_dict["model_name"]
        if not model_id:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        key = model_file_dict["model_path"]
        library_type = model_file_dict["library_type"]
        model = library_operator.load(key, library_type)

        store_sql = process_data_store(process_id, self.v_id, self.e_id, self.u_id, StoreType.ModelFile.value)
        store_result = self.insert_process_pipelining(store_sql, [json.dumps({"model_name": model_name}), None])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        return process_success(model_arr=[{"model": model, "library_type": library_type}] if model is not None else [])

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
