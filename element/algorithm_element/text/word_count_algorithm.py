import json
import sys

import jieba

from config import setting
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
from helper.generate_helper import generate_uuid
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED
from helper.word_helper import stopwords


class WordCountAlgorithm(AbstractElement):
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
        词频算子实现
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
        cut_word_sql = (
            f"select column_name "
            f"from ml_text_count_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        # 根据角色数据组装训练数据
        data = port(0, data_arr)
        fields = port(0, fields_arr)
        role = port(0, role_arr)

        cut_word_dict = db_helper1.fetchone(cut_word_sql, [])
        if not cut_word_dict or "column_name" not in cut_word_dict or not cut_word_dict["column_name"]:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置分词字段") from None

        try:
            column_name = cut_word_dict["column_name"]
            new_data = []
            for d in data:
                uuid_ = generate_uuid()
                d[f"id_{setting.ZMS_MAGIC}"] = uuid_
                d[f"pid_{setting.ZMS_MAGIC}"] = None
                d["tag_result"] = None
                d["word_count"] = None
                v = d.get(column_name)
                new_data.append(d)
                if v:
                    seg_list = jieba.cut(str(v), cut_all=False)
                    no_stopwords_seg_list = stopwords()(seg_list)
                    word_count = {}
                    for word in no_stopwords_seg_list:
                        word_count[word] = word_count.get(word, 0) + 1

                    for k, v in word_count.items():
                        child_dict = {k: None for k, v in d.items()}
                        child_dict[f"id_{setting.ZMS_MAGIC}"] = generate_uuid()
                        child_dict["tag_result"] = k
                        child_dict["word_count"] = v
                        child_dict[f"pid_{setting.ZMS_MAGIC}"] = uuid_
                        new_data.append(child_dict)

            store_sql = process_data_store(
                process_id,
                self.v_id,
                self.e_id,
                self.u_id,
            )
            if fields is None:
                fields = []
            fields.append(generate_fields("tag_result", "词语", UserDataType.Varchar2.value))
            fields.append(generate_fields("word_count", "词频", UserDataType.Number.value))
            relative_path = self.create_csv_file(new_data, fields)

            fields_json = json.dumps(fields)
            store_result = self.insert_process_pipelining(store_sql, [relative_path, fields_json])
            if store_result != ResultCode.Success.value:
                raise StoreError(
                    sys._getframe().f_code.co_name,
                    self.u_id,
                    f"{element_name}过程数据存储失败",
                ) from None
            return process_success(
                data_arr=[new_data],
                fields_arr=[fields],
                role_arr=[role] if role is not None else [],
                scaler_arr=scaler_arr,
            )
        except Exception as e:
            raise PredictError(translate_error_message(str(e))) from None

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
