import json
import sys

from sklearn.model_selection import train_test_split

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from enum_type.split_type import SplitType
from enum_type.store_type import StoreType
from error.data_process_error import DataProcessError
from error.execute_error import ExecuteError
from error.split_error import SplitError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.error_helper import translate_error_message
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED
from service.data_model.data_model_service import get_split_sample_data


class DataSplit(AbstractElement):
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
        数据切分算子实现
        输入端口: 一个D(data)端口
        输出端口: 两/三个S(data)端口
        +--------+
        |        |
        |        D
        D        |
        |        D
        |        |
        +--------+
        :param process_id: 流水标识
        :param dependency_id_arr: 依赖的算子标识数组
        :param data_arr: 接收的数据数组
        :param fields_arr: 接收的数据列头数据数组
        :param model_arr: 接收的模型数组
        :param scaler_arr: 接收的缩放器数组
        :param role_arr: 接收的角色数组
        :return: 切分后的数据节点数组、模型节点数组、字段数组、角色数组
        """
        element_name = f'{kwargs.get("element_name")}算子'

        UNUSED(model_arr)
        # 先判断是否配置了样本数据, 如果配置了，则直接使用配置
        split_sample_data = get_split_sample_data(self.v_id, self.e_id)
        if split_sample_data and split_sample_data.get("data") and split_sample_data.get("fields"):
            train = split_sample_data["data"].get("train", [])
            test = split_sample_data["data"].get("test", [])
            valid = split_sample_data["data"].get("valid", [])
            train_fields = split_sample_data["fields"].get("train", [])
            test_fields = split_sample_data["fields"].get("test", [])
            valid_fields = split_sample_data["fields"].get("valid", [])
            store_sql = process_data_store(process_id, self.v_id, self.e_id, self.u_id, StoreType.Split.value)
            train_relative_path = self.create_csv_file(train, train_fields)
            test_relative_path = self.create_csv_file(test, test_fields)
            if valid:  # 3组
                valid_relative_path = self.create_csv_file(valid, valid_fields)
                path_dict = {
                    "train": train_relative_path,
                    "test": test_relative_path,
                    "valid": valid_relative_path,
                }
                fields_dict = {
                    "train": train_fields,
                    "test": test_fields,
                    "valid": valid_fields,
                }
            else:
                path_dict = {
                    "train": train_relative_path,
                    "test": test_relative_path,
                }
                fields_dict = {
                    "train": train_fields,
                    "test": test_fields,
                }
            fields_json = json.dumps(fields_dict)
            store_result = self.insert_process_pipelining(store_sql, [json.dumps(path_dict), fields_json])
            if store_result != ResultCode.Success.value:
                raise StoreError(
                    sys._getframe().f_code.co_name,
                    self.u_id,
                    f"{element_name}过程数据存储失败",
                ) from None
            role = port(0, role_arr)
            scaler = port(0, scaler_arr)
            return process_success(
                data_arr=[train, test, valid],
                fields_arr=[train_fields, test_fields, valid_fields],
                role_arr=[role, role, role] if role is not None else [],
                scaler_arr=[scaler, scaler, scaler] if scaler is not None else [],
            )
        # 未配置样本数据
        if not data_arr or not fields_arr:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}接收的数据或字段为空") from None
        data_split_sql = (
            f"select split_type, random_seed, "
            f"train_percent, test_percent, valid_percent, "
            f"is_enabled "
            f"from ml_data_split_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        data_split_dict = db_helper1.fetchone(data_split_sql, [])
        if not data_split_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None

        # 获取切分节点的配置数据
        split_type = data_split_dict.get("split_type", SplitType.TrainTest.value)
        random_seed = data_split_dict.get("random_seed")
        if random_seed and isinstance(random_seed, str):
            try:
                random_seed = int(random_seed)
            except Exception:
                raise DataProcessError(f"{element_name}随机种子无法进行转换") from None
        # 各集合所占比例，int 类型，合计为 100
        # 例如
        # 训练集 60
        # 测试集 20
        # 验证集 20
        train_percent = data_split_dict.get("train_percent", 80)
        test_percent = data_split_dict.get("test_percent", 20)
        valid_percent = data_split_dict.get("valid_percent", 0)

        if train_percent + test_percent + valid_percent != 100:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}训练集、测试集、验证集合计不为 100",
            ) from None

        test_size = test_percent / (train_percent + test_percent + valid_percent)
        data = port(0, data_arr)
        fields = port(0, fields_arr)
        # 切分为 2 个集合(训练集、测试集)
        if split_type == SplitType.TrainTest.value:
            try:
                train, test = train_test_split(data, test_size=test_size, random_state=random_seed)
            except Exception as e:
                raise SplitError(translate_error_message(str(e))) from None
            store_sql = process_data_store(process_id, self.v_id, self.e_id, self.u_id, StoreType.Split.value)
            train_relative_path = self.create_csv_file(train, fields)
            test_relative_path = self.create_csv_file(test, fields)
            path_dict = {"train": train_relative_path, "test": test_relative_path}
            fields_dict = {"train": fields, "test": fields}
            fields_json = json.dumps(fields_dict)
            store_result = self.insert_process_pipelining(store_sql, [json.dumps(path_dict), fields_json])
            if store_result != ResultCode.Success.value:
                raise StoreError(
                    sys._getframe().f_code.co_name,
                    self.u_id,
                    f"{element_name}过程数据存储失败",
                ) from None
            role = port(0, role_arr)
            scaler = port(0, scaler_arr)
            return process_success(
                data_arr=[train, test],
                fields_arr=[fields, fields],
                role_arr=[role, role] if role is not None else [],
                scaler_arr=[scaler, scaler] if scaler is not None else [],
            )
        # 切分为 3 个集合(训练集、测试集、验证集)
        else:
            # 因为 train_test_split 只支持切分 2 个集合，因此切分两次实现切分为 3 个集合
            t_v = train_percent + valid_percent
            if not t_v:
                raise ExecuteError(
                    sys._getframe().f_code.co_name,
                    f"{element_name}训练集、验证集合计为 0",
                ) from None
            train_size = train_percent / t_v
            try:
                train_valid, test = train_test_split(data, test_size=test_size, random_state=random_seed)
                train, valid = train_test_split(train_valid, test_size=train_size, random_state=random_seed)
            except Exception as e:
                raise SplitError(translate_error_message(str(e))) from None
            store_sql = process_data_store(process_id, self.v_id, self.e_id, self.u_id, StoreType.Split.value)
            train_relative_path = self.create_csv_file(train, fields)
            test_relative_path = self.create_csv_file(test, fields)
            valid_relative_path = self.create_csv_file(valid, fields)
            path_dict = {
                "train": train_relative_path,
                "test": test_relative_path,
                "valid": valid_relative_path,
            }
            fields_dict = {"train": fields, "test": fields, "valid": fields}
            fields_json = json.dumps(fields_dict)
            store_result = self.insert_process_pipelining(store_sql, [json.dumps(path_dict), fields_json])
            if store_result != ResultCode.Success.value:
                raise StoreError(
                    sys._getframe().f_code.co_name,
                    self.u_id,
                    f"{element_name}过程数据存储失败",
                ) from None
            role = port(0, role_arr)
            scaler = port(0, scaler_arr)
            return process_success(
                data_arr=[train, test, valid],
                fields_arr=[fields, fields, fields],
                role_arr=[role, role, role] if role is not None else [],
                scaler_arr=[scaler, scaler, scaler] if scaler is not None else [],
            )

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
