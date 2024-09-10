import json
import sys

import numpy as np

import library_operator.library_operator
from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.library_type import LibraryType
from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.dictionary_rename_helper import rename
from helper.element_port_helper import port
from helper.matrix_helper import dict_array_build
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class ModelApply(AbstractElement):
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
        模型应用算子实现
        输入端口: 一个M(model)端口, 一个D(data)端口
        输出端口: 一个D(data)端口
        +--------+
        |        |
        M        |
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
        :return: 预测数据
        """
        element_name = f'{kwargs.get("element_name")}算子'

        UNUSED(fields_arr, role_arr, process_id)
        model_apply_sql = (
            f"select fields "
            f"from ml_model_apply_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        model_apply_dict = db_helper1.fetchone(model_apply_sql, [])
        if not model_apply_dict or "fields" not in model_apply_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置输出字段") from None
        fields_json = model_apply_dict["fields"]
        fields = None
        if fields_json:
            fields = json.loads(fields_json)
        # [{"name": "", "nick_name": "", "data_type": "NUMBER"}]

        data = port(0, data_arr)
        prev_fields = port(0, fields_arr)
        model_dict = port(0, model_arr)
        if not model_dict or not data:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}接收的模型或数据为空") from None
        # 将字典数组 -> 矩阵，供模型使用
        # matrix = prediction_matrix_build(data)
        model = model_dict["model"]
        library_type = model_dict["library_type"]
        prediction_output_matrix = library_operator.library_operator.predict(model, data, library_type, prev_fields)
        output_list = prediction_output_matrix.tolist()
        # 特殊处理当 output_list 为单值时，处理为数组格式
        if not isinstance(output_list, list):
            output_list = [output_list]
        if not output_list:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}输出矩阵为空") from None

        # TODO: 特殊处理威布尔输出结果
        # if library_type == LibraryType.WeibullParameter.value:
        #     output_list = [[output_data] for output_data in output_list]

        shapes = np.asarray(output_list).shape
        if len(shapes) == 1:  # 一维数组处理
            shape = 1
        elif library_type == LibraryType.WeibullParameter.value:  # 特殊处理威布尔输出结果
            shape = 2
        else:  # 二维数组处理
            shape = shapes[-1]

        # 如果用户未配置模型应用算子，自动生成列名称
        if library_type != LibraryType.WeibullParameter.value:
            fields_length = shape + len(prev_fields)
        else:
            fields_length = shape
        if len(fields) != fields_length:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}输出字段与配置字段数量不匹配, "
                f"输出字段个数{fields_length}, "
                f"配置字段个数{len(fields)}",
            ) from None

        # if not fields:
        #     fields = generate_default_column(fields_length)
        #
        # if len(fields) > fields_length:
        #     fields = fields[:fields_length]
        # elif len(fields) < fields_length:
        #     generate_fields = generate_default_column(fields_length - len(fields))
        #     generate_fields.extend(fields)
        #     fields = generate_fields[:]

        # 推理结果数据处理
        if library_type != LibraryType.WeibullParameter.value:
            output_list_fields = fields[len(prev_fields) : :]
        else:
            output_list_fields = fields
        output_data = dict_array_build(output_list, output_list_fields)
        # 推理结果数据拼接 Data 端口传入的数据
        if library_type != LibraryType.WeibullParameter.value:
            rename_dict_fields = fields[: len(prev_fields) :]
            rename_data = rename(data, rename_dict_fields)

            rename_dict_fields.extend(output_list_fields)
            rename_output_data = []
            for i, d in enumerate(rename_data):
                rename_output_data.append(dict(rename_data[i], **output_data[i]))
        else:
            rename_output_data = output_data
            rename_dict_fields = fields
        store_sql = process_data_store(
            process_id,
            self.v_id,
            self.e_id,
            self.u_id,
        )
        relative_path = self.create_csv_file(rename_output_data, rename_dict_fields)
        if rename_dict_fields is None:
            rename_dict_fields = []
        fields_json = json.dumps(rename_dict_fields)
        store_result = self.insert_process_pipelining(store_sql, [relative_path, fields_json])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None

        return process_success(data_arr=[rename_output_data], fields_arr=[rename_dict_fields])

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
