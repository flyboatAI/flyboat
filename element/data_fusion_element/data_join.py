import json
import sys

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.join_type import JoinType
from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper import join_helper
from helper.data_store_helper import process_data_store
from helper.dictionary_rename_helper import dict_rename, field_rename, scaler_rename
from helper.element_port_helper import port
from helper.join_helper import distinct_list_of_dict
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class DataJoin(AbstractElement):
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
        数据连接算子实现
        输入端口: 两个D(data)端口
        输出端口: 一个D(data)端口
        +--------+
        |        |
        D        |
        |        D
        D        |
        |        |
        +--------+
        :param process_id: 流水标识
        :param dependency_id_arr: 依赖的算子标识数组
        :param data_arr: 接收的数据数组
        :param fields_arr: 接收的数据列头数据数组
        :param model_arr: 接收的模型数组
        :param scaler_arr: 接收的缩放器数组
        :param role_arr: 接收的角色数组
        :return: 数据节点数组、模型节点数组、字段数组
        """
        element_name = f'{kwargs.get("element_name")}算子'

        UNUSED(model_arr)
        if not data_arr or not fields_arr or not dependency_id_arr or len(data_arr) == 1 or len(fields_arr) == 1:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}接收的数据或字段或依赖为空",
            ) from None

        join_sql = (
            f"select join_type, join_field, fields, "
            f"is_enabled "
            f"from ml_data_join_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        join_sql_dict = db_helper1.fetchone(join_sql, [])
        if (
            not join_sql_dict
            or "join_type" not in join_sql_dict
            or "join_field" not in join_sql_dict
            or "fields" not in join_sql_dict
        ):
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        join_type = join_sql_dict["join_type"]
        # ["x.1234-5678-000000", "y.2345-5678-000000"]
        join_field_json = join_sql_dict["join_field"]
        fields_json = join_sql_dict["fields"]
        if not join_field_json or not fields_json:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}连接字段未配置") from None

        display_fields = json.loads(fields_json)
        display_field_names = [field["name"] for field in display_fields]
        join_field = json.loads(join_field_json)
        if len(join_field) != 2:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}连接字段未配置") from None
        join_field_port_0 = port(0, join_field)
        join_field_port_1 = port(1, join_field)

        data_port_0 = port(0, data_arr)
        data_port_1 = port(1, data_arr)

        fields_port_0 = port(0, fields_arr)
        fields_port_1 = port(1, fields_arr)

        dependency_id_0 = port(0, dependency_id_arr)
        dependency_id_1 = port(1, dependency_id_arr)

        dependency_id_0 += "-0"
        dependency_id_1 += "-1"

        convert_data_port_0 = dict_rename(data_port_0, dependency_id_0)
        convert_data_port_1 = dict_rename(data_port_1, dependency_id_1)

        convert_fields_port_0 = field_rename(fields_port_0, dependency_id_0)
        convert_fields_port_1 = field_rename(fields_port_1, dependency_id_1)

        fields = [x for x in convert_fields_port_0 if x["name"] in display_field_names]
        fields.extend([x for x in convert_fields_port_1 if x["name"] in display_field_names])
        if fields:
            fields = distinct_list_of_dict(fields, "name")

        # 角色处理
        role = None
        role_port_0 = port(0, role_arr)
        role_port_1 = port(1, role_arr)

        if len(role_arr) == 1:
            convert_role_port_x = None
            if role_port_0:
                convert_role_port_x = field_rename(role_port_0, dependency_id_0)
            elif role_port_1:
                convert_role_port_x = field_rename(role_port_1, dependency_id_1)
            if convert_role_port_x:
                role = [x for x in convert_role_port_x if x["name"] in display_field_names]
        elif len(role_arr) == 2:
            convert_role_port_0 = field_rename(role_port_0, dependency_id_0)
            convert_role_port_1 = field_rename(role_port_1, dependency_id_1)
            if convert_role_port_0:
                role = [x for x in convert_role_port_0 if x["name"] in display_field_names]
            if role:
                if convert_role_port_1:
                    role.extend([x for x in convert_role_port_1 if x["name"] in display_field_names])
            elif convert_role_port_1:
                role = [x for x in convert_role_port_1 if x["name"] in display_field_names]
        if role:
            role = distinct_list_of_dict(role, "name")

        # 归一化字段处理
        scaler_port_0 = port(0, scaler_arr)
        scaler_port_1 = port(1, scaler_arr)
        scaler = {}
        if len(scaler_arr) == 1:
            convert_scaler_port_x = None
            if scaler_port_0:
                convert_scaler_port_x = scaler_rename(scaler_port_0, dependency_id_0)
                # [x for x in convert_scaler_port_0 if x["name"] in display_field_names]
            elif scaler_port_1:
                convert_scaler_port_x = scaler_rename(scaler_port_1, dependency_id_1)
            if convert_scaler_port_x:
                for k in convert_scaler_port_x.keys():
                    if k in display_field_names:
                        scaler[k] = convert_scaler_port_x[k]
        elif len(scaler_arr) == 2:
            if scaler_port_0 and scaler_port_1:
                convert_scaler_port_0 = scaler_rename(scaler_port_0, dependency_id_0)
                convert_scaler_port_1 = scaler_rename(scaler_port_1, dependency_id_1)
                if convert_scaler_port_0:
                    for k in convert_scaler_port_0.keys():
                        if k in display_field_names:
                            scaler[k] = convert_scaler_port_0[k]
                if scaler:
                    if convert_scaler_port_1:
                        for k in convert_scaler_port_1.keys():
                            if k in display_field_names:
                                scaler[k] = convert_scaler_port_1[k]
                elif convert_scaler_port_1:
                    for k in convert_scaler_port_1.keys():
                        if k in display_field_names:
                            scaler[k] = convert_scaler_port_1[k]

        store_sql = process_data_store(
            process_id,
            self.v_id,
            self.e_id,
            self.u_id,
        )
        if convert_data_port_0 is None or convert_data_port_1 is None:
            relative_path = self.create_csv_file([], fields)
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
            return process_success(
                data_arr=[[]],
                fields_arr=[fields],
                role_arr=[role] if role is not None else [],
                scaler_arr=[scaler],
            )
        if join_type == JoinType.InnerJoin.value:
            data = join_helper.inner_join(
                convert_data_port_0,
                convert_data_port_1,
                join_field_port_0,
                join_field_port_1,
                display_field_names,
            )
        elif join_type == JoinType.LeftJoin.value:
            data = join_helper.left_join(
                convert_data_port_0,
                convert_data_port_1,
                join_field_port_0,
                join_field_port_1,
                display_field_names,
            )
        else:
            data = join_helper.full_join(
                convert_data_port_0,
                convert_data_port_1,
                join_field_port_0,
                join_field_port_1,
                display_field_names,
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
        return process_success(
            data_arr=[data],
            fields_arr=[fields],
            role_arr=[role] if role is not None else [],
            scaler_arr=[scaler],
        )

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
