import json
import sys

from jieba import analyse

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from enum_type.user_data_type import UserDataType
from error.execute_error import ExecuteError
from error.predict_error import PredictError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.error_helper import translate_error_message
from helper.fields_helper import generate_fields
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


class WordTagAlgorithm(AbstractElement):
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
        关键词提取算子实现
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
        :return: 生产的模型
        """
        element_name = f'{kwargs.get("element_name")}算子'

        UNUSED(fields_arr, model_arr, process_id)
        if not data_arr or not fields_arr:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}接收的数据或字段为空") from None
        word_tag_sql = (
            f"select top_k, column_name "
            f"from ml_text_tag_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        # 根据角色数据组装训练数据
        data = port(0, data_arr)
        fields = port(0, fields_arr)

        word_tag_dict = db_helper1.fetchone(word_tag_sql, [])
        if (
            not word_tag_dict
            or "top_k" not in word_tag_dict
            or not word_tag_dict["top_k"]
            or "column_name" not in word_tag_dict
            or not word_tag_dict["column_name"]
        ):
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}未配置提取个数或提取字段",
            ) from None

        try:
            top_k = word_tag_dict["top_k"]
            column_name = word_tag_dict["column_name"]
            for d in data:
                v = d.get(column_name)
                tag_result = None
                if v:
                    tag_tuple_of_list = analyse.extract_tags(str(v), topK=top_k, withWeight=True, allowPOS=())
                    tag_list = [t[0] for t in tag_tuple_of_list]
                    tag_result = "、".join(tag_list)
                d["tag_result"] = tag_result
            store_sql = process_data_store(
                process_id,
                self.v_id,
                self.e_id,
                self.u_id,
            )
            if fields is None:
                fields = []
            fields.append(generate_fields("tag_result", "关键词", UserDataType.Varchar2.value))
            relative_path = self.create_csv_file(data, fields)

            fields_json = json.dumps(fields)
            store_result = self.insert_process_pipelining(store_sql, [relative_path, fields_json])
            if store_result != ResultCode.Success.value:
                raise StoreError(
                    sys._getframe().f_code.co_name,
                    self.u_id,
                    f"{element_name}过程数据存储失败",
                ) from None
            return process_success(data_arr=[data], fields_arr=[fields], role_arr=[], scaler_arr=scaler_arr)
        except Exception as e:
            raise PredictError(translate_error_message(str(e))) from None

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
