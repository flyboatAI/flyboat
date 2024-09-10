import sys

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from error.execute_error import ExecuteError
from helper.element_port_helper import port
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class ModelSyncOutput(AbstractElement):
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
        模型同步输出算子实现
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
        model_sync_output_sql = (
            f"select  t2.json_key, t2.nick_name "
            f"from ml_model_sync_output_element t1 "
            f"left join ml_sync_element_config t2 "
            f"on t1.id=t2.element_id "
            f"where t1.id='{self.e_id}' and "
            f"t1.version_id='{self.v_id}' and "
            f"t1.is_enabled={Enabled.Yes.value}"
        )
        model_sync_output_dict = db_helper1.fetchone(model_sync_output_sql, [])
        if (
            not model_sync_output_dict
            or "json_key" not in model_sync_output_dict
            or "nick_name" not in model_sync_output_dict
        ):
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None

        json_key = model_sync_output_dict["json_key"]
        model_dict = port(0, model_arr)
        if not model_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}接收的模型为空") from None
        return process_success(model_arr=[{json_key: model_dict}])

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
