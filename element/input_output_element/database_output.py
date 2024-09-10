import json
import sys

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED
from service.data_source.data_source_service import get_transient_sql_helper


class DatabaseOutput(AbstractElement):
    """
    数据库输出算子
    """

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
        同步输出算子实现
        输入端口: 一个D(data)端口
        输出端口: 无
        +--------+
        |        |
        |        |
        D        |
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
        :return: 处理后/验证后的同步输出数据节点数组、模型节点数组、字段数组、角色数组
        """
        element_name = f'{kwargs.get("element_name")}算子'

        UNUSED(process_id, model_arr, scaler_arr, role_arr)
        data = port(0, data_arr)
        fields = port(0, fields_arr)

        # 获取算子的配置
        database_output_sql = (
            f"select datasource_id, data_table_name "
            f"from ml_database_output_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )
        database_output_dict = db_helper1.fetchone(database_output_sql, [])

        if (
            not database_output_dict
            or "datasource_id" not in database_output_dict
            or "data_table_name" not in database_output_dict
        ):
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        datasource_id = database_output_dict["datasource_id"]
        target_table_name = database_output_dict["data_table_name"]
        if not datasource_id or not target_table_name:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None

        # 获取算子列的配置
        database_output_column_sql = (
            f"select  source_column_code, target_column_code "
            f"from ml_database_output_element_column "
            f"where version_id='{self.v_id}' and "
            f"element_id='{self.e_id}' "
            f"order by sort"
        )
        database_output_column_arr = db_helper1.fetchall(database_output_column_sql, [])
        if not database_output_column_arr or len(database_output_column_arr) == 0:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}列信息未配置完毕") from None
        # 过滤没有目标字段的数据
        database_output_column_arr = list(filter(lambda x: x.get("target_column_code"), database_output_column_arr))

        db_helper_target = get_transient_sql_helper(datasource_id)
        target_fields_arr = db_helper_target.fetch_fields(target_table_name)
        if not target_fields_arr or len(target_fields_arr) == 0:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"未查询到{element_name}目标数据表的列信息",
            ) from None

        # 拼装数据字段的对照关系
        if fields is None:
            fields = []
        if data is None:
            data = []

        # 遍历算子列配置（字段对照），得到源列名/程序数据类型、目标列名/数据库数据类型/列说明
        for column in database_output_column_arr:
            source_column_code = column.get("source_column_code")
            target_column_code = column.get("target_column_code")
            source_column_data_type = None  # 源数据类型
            target_column_info = None  # 目标字段信息

            # 与接收字段对比，得到接收字段数据类型（程序中的数据类型）
            in_find_list = list(filter(lambda x: x.get("name") == source_column_code, fields))
            if len(in_find_list) > 0:
                first = in_find_list[0]
                if first:
                    source_column_data_type = first.get("data_type")
            if source_column_data_type is None:
                raise ExecuteError(
                    sys._getframe().f_code.co_name,
                    f"{element_name}列配置未能与接收数据字段匹配，源列名：" + source_column_code,
                ) from None

            # 与目标的物理表字段对比，得到目标物理表字段信息
            target_find_list = list(
                filter(
                    lambda x: x.get("column_name") == target_column_code,
                    target_fields_arr,
                )
            )
            if len(target_find_list) > 0:
                target_column_info = target_find_list[0]
            if target_column_info is None or target_column_info is None:
                raise ExecuteError(
                    sys._getframe().f_code.co_name,
                    f"{element_name}列配置未能与物理表匹配，目标列名：" + target_column_code,
                ) from None

            # 将源和目标的数据类型拼装到算子配置列的字典里
            column["source_column_data_type"] = source_column_data_type
            column["target_column_info"] = target_column_info

        # 数据进入目标库，并拼装洞察数据
        store_data = []
        store_fields = []
        # 列
        for column in database_output_column_arr:
            store_fields.append(
                {
                    "name": column.get("target_column_code"),  # 目标字段名
                    "nick_name": column.get("target_column_code"),  # 目标字段中文
                    "data_type": column.get("source_column_data_type"),  # 源数据类型
                }
            )
        # 数据
        for s_row in data:
            t_row = {}  # 目标行数据，存洞察数据用
            target_fields = []
            for column in database_output_column_arr:
                source_column_code = column.get("source_column_code")  # 源字段名
                target_column_code = column.get("target_column_code")  # 目标字段名
                value = s_row.get(source_column_code)  # 值
                t_row[target_column_code] = value  # 记录洞察数据值
                target_fields.append(column.get("target_column_info"))  # 目标列信息

            db_helper_target.insert_by_fields(target_table_name, target_fields, t_row)  # 入库
            store_data.append(t_row)  # 记录洞察数据

        # 保存运行信息到记录表中，用于洞察中回看
        relative_path = self.create_csv_file(store_data, store_fields)
        store_sql = process_data_store(process_id, self.v_id, self.e_id, self.u_id)
        store_result = self.insert_process_pipelining(store_sql, [relative_path, json.dumps(store_fields)])
        if store_result != ResultCode.Success.value:
            raise StoreError(
                sys._getframe().f_code.co_name,
                self.u_id,
                f"{element_name}过程数据存储失败",
            ) from None
        return process_success()

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)
