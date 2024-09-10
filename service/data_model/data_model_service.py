import json
import math
import sys

from config import setting
from config.dynamic_table_setting import TS_COLUMN_NAME
from entity.database.database_field_standard import DatabaseFieldStandard
from enum_type.database_field_data_type import DatabaseFieldStandardDataType
from enum_type.deleted import Deleted
from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from helper.generate_helper import generate_uuid, uuid_and_now
from helper.oss_helper.oss_helper import oss_helper1
from helper.result_helper import execute_success
from helper.sql_helper.init_sql_helper import db_helper1, db_helper2
from helper.time_helper import current_time_m
from parameter_entity.data_model.manual_data_model import ManualDataModel
from parameter_entity.sample_data.sample_data import SampleDataModel
from service.data_source.data_source_service import get_transient_sql_helper


def get_sample_data_fields_from_datasource(datasource_id, table_name):
    conn = get_transient_sql_helper(datasource_id)
    # 获取列数据
    return conn.fetch_fields_format(table_name)


def get_sample_data_from_datasource(datasource_id, table_name):
    conn = get_transient_sql_helper(datasource_id)
    # 获取数据集
    data_sql = f"select * from {table_name}"
    page = conn.fetchpage(data_sql, 1, 10)
    return page.data


def get_sample_data_from_dt_table_by_name(table_name, contains_rowid=False, search_key=None, current=1, size=10):
    """
    根据 table_name 获取 对应表的样本数据
    :param table_name: 数据表名称
    :param contains_rowid: 是否包含 rowid
    :param search_key: 检索关键字
    :param current: 当前页
    :param size: 每页数量
    :return: 动态数据表结构
    """
    if not table_name:
        return []
    rowid_sql = f"dt_1.rowid as row_id_{setting.ZMS_MAGIC}," if contains_rowid else ""
    order_sql = f"order by {TS_COLUMN_NAME}, rowid" if contains_rowid else ""
    condition = ""
    if search_key:
        fields = get_sample_data_fields_from_dt_table_by_name(table_name)
        condition = " where " + "or ".join([f'dt_1.{f.get("name")} like \'%{search_key}%\'' for f in fields])
    data_sql = f"select {rowid_sql} dt_1.* from {table_name} dt_1 {condition} {order_sql}"
    return db_helper2.fetchpage(data_sql, current, size)


def get_sample_data_fields_from_dt_table_by_name(table_name):
    """
    根据 table_id 获取 对应表的结构，用于数据模型算子弹窗数据联动
    :param table_name: 数据表名称
    :return: [
                {
                    "name": "",
                    "nick_name": "",
                    "data_type": "NUMBER",
                }
              ]
    """
    if not table_name:
        return []
    return db_helper2.fetch_fields_format(table_name)


def get_sample_data_from_dt_table_by_id(table_id, search_key, current, size):
    """
    根据 table_id 获取 对应表的样本数据
    :param table_id: 数据表标识
    :param search_key: 检索
    :param current: 当前页
    :param size: 每页数量
    :return: 动态数据表结构
    """
    table_name = get_table_name_by_id(table_id)
    return get_sample_data_from_dt_table_by_name(
        table_name,
        contains_rowid=True,
        search_key=search_key,
        current=current,
        size=size,
    )


def get_sample_data_fields_from_dt_table_by_id(table_id):
    """
    根据 table_id 获取 对应表的结构，用于数据模型算子弹窗数据联动
    :param table_id: 数据表标识
    :return: [
                {
                    "name": "",
                    "nick_name": "",
                    "data_type": "NUMBER",
                }
              ]
    """
    table_name = get_table_name_by_id(table_id)
    return get_sample_data_fields_from_dt_table_by_name(table_name)


def get_table_name_by_id(table_id: str):
    sql = f"select name as table_name from ml_ddm_table where id='{table_id}'"
    table_name_dict = db_helper1.fetchone(sql)
    if not table_name_dict:
        return None
    return table_name_dict["table_name"]


def get_all_data_models(user_id, data_model_table_name):
    """
    按照用户创建数据模型的时间倒序返回数据模型列表
    :param data_model_table_name:
    :param user_id: 用户标识
    :return: [
                {
                    "data_model_name": "",
                    "table_id": "",
                    "dt_table_name": "",
                    "remark": ""
                 }
              ]
    """
    # noinspection SqlResolve
    data_models_sql = (
        f"select t1.id, t1.data_model_name, "
        f"t2.name as dt_table_name, "
        f"t1.table_id, "
        f"t1.remark from {data_model_table_name} t1 "
        f"left join ml_ddm_table t2 "
        f"on t1.table_id=t2.id "
        f"where t1.is_deleted={Deleted.No.value} and "
        f"t1.create_user='{user_id}' "
        f"order by t1.create_time desc"
    )
    data_models = db_helper1.fetchall(data_models_sql, [])
    return execute_success(data=data_models)


def fetch_fields_standard_from_datasource_id(datasource_id: str, table_name: str):
    conn = get_transient_sql_helper(datasource_id)
    # 获取数据集
    return conn.fetch_fields_standard(table_name)


def insert_dynamic_table_id(user_id: str, table_name: str):
    uuid_, now = uuid_and_now()
    sql = (
        f"insert into ml_ddm_table values('{uuid_}', '{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), '{table_name}', '{table_name}', "
        f"null)"
    )
    result = db_helper1.execute(sql)
    if result != ResultCode.Success.value:
        return None
    return uuid_


def insert_table_data_model(
    user_id: str,
    table_id: str,
    table_name: str,
    model_name: str,
    datasource_id: str,
    desc: str,
):
    uuid_, now = uuid_and_now()
    sql = (
        f"insert into ml_sample_table_model values('{uuid_}', {Deleted.No.value}, "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{model_name}', '{table_id}', '{table_name}', '{datasource_id}', '{desc}')"
    )
    return db_helper1.execute(sql)


def insert_file_data_model(user_id: str, table_id: str, model_name: str, file_type: str, desc: str):
    uuid_, now = uuid_and_now()
    sql = (
        f"insert into ml_sample_file_model values('{uuid_}', {Deleted.No.value}, "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{model_name}', '{table_id}', '{file_type}', '{file_type}', '{desc}')"
    )
    return db_helper1.execute(sql)


def insert_manual_data_model(user_id: str, table_id: str, model_name: str, desc: str):
    uuid_, now = uuid_and_now()
    sql = (
        f"insert into ml_sample_manual_model values('{uuid_}', {Deleted.No.value}, "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{model_name}', '{table_id}', '{desc}')"
    )
    return db_helper1.execute(sql)


def insert_formula_data_model(
    user_id: str,
    table_id: str,
    model_name: str,
    formula: str,
    generate_count: int,
    desc: str,
):
    uuid_, now = uuid_and_now()
    sql = (
        f"insert into ml_sample_formula_model values('{uuid_}', {Deleted.No.value}, "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{model_name}', '{table_id}', '{formula}', {generate_count}, '{desc}')"
    )
    return db_helper1.execute(sql)


def create_dynamic_table_and_insert_data(user_id: str, data_model: ManualDataModel):
    fields = data_model.fields
    data = data_model.data

    standard_fields: list[DatabaseFieldStandard] = [
        DatabaseFieldStandard(
            f.get("name"),
            DatabaseFieldStandardDataType.get_standard_data_type_from_user_data_type(f.get("data_type")),
            f.get("nick_name"),
        )
        for f in fields
    ]
    dynamic_table_name = f"dt_{generate_uuid()}"

    # 动态建表
    result = db_helper2.create_dynamic_table(dynamic_table_name, standard_fields)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "动态创建数据表失败") from None
    table_id = insert_dynamic_table_id(user_id, dynamic_table_name)
    if not table_id:
        raise ExecuteError(sys._getframe().f_code.co_name, "动态创建数据表失败") from None

    # 批量插入数据
    result = batch_insert_to_dynamic_table_by_data(dynamic_table_name, data, standard_fields)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "动态插入数据表失败") from None
    return table_id


def insert_field_to_dynamic_table_for_data_tag(table_id, field: dict):
    standard_field: DatabaseFieldStandard = DatabaseFieldStandard(
        field.get("name"),
        DatabaseFieldStandardDataType.get_standard_data_type_from_user_data_type(field.get("data_type")),
        field.get("nick_name"),
    )
    table_name = get_table_name_by_id(table_id)
    return db_helper2.insert_field_to_dynamic_table(table_name, standard_field)


def batch_insert_to_dynamic_table_by_datasource_id(
    dynamic_table_name,
    datasource_id,
    table_name,
    standard_fields: list[DatabaseFieldStandard],
    count=5000,
):
    """
    批量插入动态表
    :param dynamic_table_name: 动态表名
    :param datasource_id: 源数据源标识
    :param table_name: 源数据表名
    :param standard_fields: 标准字段列表
    :param count: 每批插入数量
    :return: 插入结果
    """
    conn = get_transient_sql_helper(datasource_id)
    sql = f"select count(*) as c from {table_name}"
    c_dict = conn.fetchone(sql)
    total = 0
    if c_dict and "c" in c_dict:
        total = c_dict.get("c", 0)
    page_size = math.ceil(total / count)
    custom_ts_name = TS_COLUMN_NAME
    for page in range(page_size):
        current = page + 1
        page_data = conn.fetchpage(f"select * from {table_name}", current, count)
        batch_data = page_data.data
        now = current_time_m()
        for d in batch_data:
            d[custom_ts_name] = now
        column_names = [f.column_name.upper() for f in standard_fields]
        if custom_ts_name not in column_names:
            standard_fields.append(DatabaseFieldStandard(custom_ts_name, DatabaseFieldStandardDataType.DateTime))
        result = db_helper2.batch_insert_to_dynamic_table(dynamic_table_name, batch_data, standard_fields)
        if result != ResultCode.Success.value:
            return ResultCode.Error.value
    return ResultCode.Success.value


def batch_insert_to_dynamic_table_by_data(
    dynamic_table_name, data, standard_fields: list[DatabaseFieldStandard], count=5000
):
    """
    批量插入动态表
    :param dynamic_table_name: 动态表名
    :param data: 数据
    :param standard_fields: 标准字段列表
    :param count: 每批插入数量
    :return: 插入结果
    """
    total = len(data)
    custom_ts_name = TS_COLUMN_NAME
    column_names = [f.column_name.upper() for f in standard_fields]
    if custom_ts_name not in column_names:
        standard_fields.append(DatabaseFieldStandard(custom_ts_name, DatabaseFieldStandardDataType.DateTime))
    for i in range(0, total, count):
        now = current_time_m()
        batch_data = data[i : i + count]
        for d in batch_data:
            d[custom_ts_name] = now
        result = db_helper2.batch_insert_to_dynamic_table(dynamic_table_name, batch_data, standard_fields)
        if result != ResultCode.Success.value:
            return ResultCode.Error.value
    return ResultCode.Success.value


def create_excel_file_by_table(table_id):
    table_name = get_table_name_by_id(table_id)
    data_sql = f"select * from {table_name}"
    data = db_helper2.fetchall(data_sql)
    fields = db_helper2.fetch_fields_format(table_name)
    return oss_helper1.create_excel_file(data, fields)


def update_data_model_sample_data(table_id: str, rowid: str, data: dict):
    table_name = get_table_name_by_id(table_id)
    return db_helper2.update_dynamic_table(table_name, data, rowid)


def delete_data_model_sample_data(table_id: str, rowid_list: list[str]):
    table_name = get_table_name_by_id(table_id)
    params = ", ".join([f"'{rowid}'" for rowid in rowid_list])
    delete_sql = f"delete from {table_name} where rowid in ({params})"
    return db_helper2.execute(delete_sql)


def append_data_model_sample_data(table_id: str, data: list[dict], fields: list[dict]):
    table_name = get_table_name_by_id(table_id)
    standard_fields: list[DatabaseFieldStandard] = [
        DatabaseFieldStandard(
            f.get("name"),
            DatabaseFieldStandardDataType.get_standard_data_type_from_user_data_type(f.get("data_type")),
            f.get("nick_name"),
        )
        for f in fields
    ]
    # 批量插入数据
    return batch_insert_to_dynamic_table_by_data(table_name, data, standard_fields)


def get_split_sample_data(version_id, element_id):
    sql = (
        f"select * from ml_data_split_sample_data " f"where element_id='{element_id}' and " f"version_id='{version_id}'"
    )
    split_dict = db_helper1.fetchone(sql)
    data = {"data": {}, "fields": {}}
    if split_dict and split_dict.get("field_path"):
        train_path = split_dict.get("train_path")
        test_path = split_dict.get("test_path")
        valid_path = split_dict.get("valid_path")
        field_path = split_dict.get("field_path")
        fields_str = oss_helper1.get_json_file_data(field_path)
        fields = json.loads(fields_str)
        if train_path:
            train_data = oss_helper1.read_csv_file(train_path, fields)
            data["data"]["train"] = train_data
            data["fields"]["train"] = fields
        else:
            data["data"]["train"] = []
            data["fields"]["train"] = fields
        if test_path:
            test_data = oss_helper1.read_csv_file(test_path, fields)
            data["data"]["test"] = test_data
            data["fields"]["test"] = fields
        else:
            data["data"]["test"] = []
            data["fields"]["test"] = fields
        if valid_path:
            valid_data = oss_helper1.read_csv_file(valid_path, fields)
            data["data"]["valid"] = valid_data
            data["fields"]["valid"] = fields
        else:
            data["data"]["valid"] = []
            data["fields"]["valid"] = fields

    return data


def rewrite_split_sample_data(user_id: str, sample_data_model: SampleDataModel):
    version_id = sample_data_model.version_id
    element_id = sample_data_model.element_id
    train = sample_data_model.train
    test = sample_data_model.test
    valid = sample_data_model.valid
    fields = sample_data_model.fields
    train_path = ""
    test_path = ""
    valid_path = ""
    fields_path = ""
    if train:
        train_path = oss_helper1.create_csv_file(train, fields)
    if test:
        test_path = oss_helper1.create_csv_file(test, fields)
    if valid:
        valid_path = oss_helper1.create_csv_file(valid, fields)
    if fields:
        fields_str = json.dumps(fields)
        fields_path = oss_helper1.create_json_file(fields_str)

    delete_sql = (
        f"delete from ml_data_split_sample_data " f"where element_id='{element_id}' and " f"version_id='{version_id}'"
    )
    uuid_, now = uuid_and_now()
    insert_sql = (
        f"insert into ml_data_split_sample_data values('{uuid_}', '{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"'{element_id}', "
        f"'{train_path}', '{test_path}', '{valid_path}', '{fields_path}')"
    )
    return db_helper1.execute_arr([delete_sql, insert_sql])
