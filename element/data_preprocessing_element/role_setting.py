import json
import sys

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from enum_type.store_type import StoreType
from enum_type.user_data_type import UserDataType
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.fields_helper import generate_fields
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class RoleSetting(AbstractElement):
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
        角色设置算子实现
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

        UNUSED(model_arr, role_arr)
        role_setting_sql = (
            f"select role_setting_fields, "
            f"is_enabled "
            f"from ml_role_setting_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        role_setting_dict = db_helper1.fetchone(role_setting_sql, [])
        if not role_setting_dict or "role_setting_fields" not in role_setting_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        role_json = role_setting_dict["role_setting_fields"]
        if not role_json:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置角色") from None
        role = json.loads(role_json)
        store_sql = process_data_store(process_id, self.v_id, self.e_id, self.u_id, StoreType.Role.value)
        fields = [
            generate_fields("name", "字段", data_type=UserDataType.Varchar2.value),
            generate_fields("nick_name", "别名", data_type=UserDataType.Varchar2.value),
            generate_fields("role_type", "角色", data_type=UserDataType.Varchar2.value),
        ]

        fields_json = json.dumps(fields)
        store_result = self.insert_process_pipelining(store_sql, [role_json, fields_json])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        return process_success(
            data_arr=list(data_arr),
            fields_arr=list(fields_arr),
            role_arr=[role] if role_json is not None else [],
            scaler_arr=list(scaler_arr),
        )

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
