import json
import sys

from element.abstract_element import AbstractElement
from enum_type.deleted import Deleted
from enum_type.element_config_type import ElementConfigType
from enum_type.enabled import Enabled
from enum_type.input_type import ValueType
from enum_type.result_code import ResultCode
from enum_type.store_type import StoreType
from enum_type.user_data_type import UserDataType
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_fields_match_helper import match_fields, reorder_key_data
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.fields_helper import generate_fields
from helper.range_helper import (
    build_date_range_list,
    build_int_range_list,
    build_single_value,
)
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class SyncInput(AbstractElement):
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
        同步输入算子实现
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
        sync_input_sql = (
            f"select t2.id as config_id, t2.json_key, t2.value_type, "
            f"t2.start_key, "
            f"t2.end_key, t2.step_key, t2.single_key, t2.single_value_data_type, "
            f"t2.nick_name "
            f"from ml_sync_input_element t1 "
            f"left join ml_sync_element_config t2 on "
            f"t1.id=t2.element_id and "
            f"t1.version_id=t2.version_id "
            f"where t1.id='{self.e_id}' and "
            f"t1.version_id='{self.v_id}' and "
            f"t2.is_deleted={Deleted.No.value} and "
            f"t2.element_type='{ElementConfigType.Input.value}' and "
            f"t1.is_enabled={Enabled.Yes.value}"
        )

        sync_input_dict = db_helper1.fetchone(sync_input_sql)
        if not sync_input_dict or "json_key" not in sync_input_dict or "value_type" not in sync_input_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        # JSON key 字段
        json_key = sync_input_dict["json_key"]
        nick_name = sync_input_dict["nick_name"]
        if not json_key:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置服务接口标识字段") from None

        sync_input_data = port(0, data_arr)
        if not sync_input_data or not isinstance(sync_input_data, dict):
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}未传递服务接口标识字段对应数据",
            ) from None
        key_data = sync_input_data.get(json_key)
        if key_data is None:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}未传递服务接口标识字段对应数据",
            ) from None
        value_type = sync_input_dict["value_type"]
        start_key = sync_input_dict.get("start_key")
        end_key = sync_input_dict.get("end_key")
        step_key = sync_input_dict.get("step_key")
        single_key = sync_input_dict.get("single_key")
        single_value_data_type = sync_input_dict.get("single_value_data_type")
        if value_type == ValueType.Table.value:
            config_id = sync_input_dict["config_id"]
            sync_input_column_sql = (
                f"select column_name as nick_name, "
                f"column_code as name, "
                f"data_type, sort, remark "
                f"from ml_sync_element_column "
                f"where element_config_id='{config_id}' "
                f"and is_deleted={Deleted.No.value} order by sort"
            )
            fields = db_helper1.fetchall(sync_input_column_sql)
            match_result = match_fields(key_data, fields)
            if not match_result:
                raise ExecuteError(
                    sys._getframe().f_code.co_name,
                    f"{element_name}传递字段与数据字段不对应",
                ) from None
            # 处理接口传递的字典 Key 乱序问题
            data = reorder_key_data(key_data, fields)
        elif value_type == ValueType.IntRange.value:
            start_value = key_data.get(start_key)
            end_value = key_data.get(end_key)
            if step_key is None:
                raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置步长标识字段") from None
            step_value = key_data.get(step_key)
            if start_value is None or end_value is None or step_value is None:
                raise ExecuteError(
                    sys._getframe().f_code.co_name,
                    f"{element_name}未传递对应的起始值数据、" f"结束值数据、步长值数据",
                ) from None
            data = build_int_range_list(start_value, end_value, step_value, json_key)
            fields = [generate_fields(json_key, nick_name, UserDataType.Number.value)]
        elif value_type == ValueType.YearRange.value:
            start_value = key_data.get(start_key)
            end_value = key_data.get(end_key)
            if start_value is None or end_value is None:
                raise ExecuteError(
                    sys._getframe().f_code.co_name,
                    f"{element_name}未传递对应的起始值数据、" f"结束值数据",
                ) from None
            key = "year"
            data = build_int_range_list(start_value, end_value, 1, key)
            fields = [generate_fields(key, "年份", UserDataType.Number.value)]
        elif value_type == ValueType.SingleValue.value:
            if not single_value_data_type:
                raise ExecuteError(
                    sys._getframe().f_code.co_name,
                    f"{element_name}未配置 " f"单值标识字段",
                ) from None
            single_value = key_data.get(single_key)
            if single_value is None:
                raise ExecuteError(
                    sys._getframe().f_code.co_name,
                    f"{element_name}未传递对应的单值数据",
                ) from None
            key = "value"
            data = build_single_value(single_value, key)
            fields = [generate_fields(key, "值", single_value_data_type)]
        else:
            start_value = key_data.get(start_key)
            end_value = key_data.get(end_key)
            if start_value is None or end_value is None:
                raise ExecuteError(
                    sys._getframe().f_code.co_name,
                    f"{element_name}未传递对应的起始值数据、" f"结束值数据",
                ) from None
            key = "month" if value_type == ValueType.MonthRange.value else "day"
            data = build_date_range_list(start_value, end_value, value_type, key)
            if not data:
                raise ExecuteError(
                    sys._getframe().f_code.co_name,
                    f"{element_name}区间数据传递错误，请检查",
                ) from None
            fields = [
                generate_fields(
                    key,
                    "月份" if value_type == ValueType.MonthRange.value else "日期",
                    UserDataType.Varchar2.value,
                )
            ]
        if data is None:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未传递对应数据") from None
        store_sql = process_data_store(
            process_id,
            self.v_id,
            self.e_id,
            self.u_id,
            StoreType.Range.value if value_type != ValueType.Table.value else StoreType.Table.value,
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
