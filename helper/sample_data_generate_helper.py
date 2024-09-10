import json
import random

from enum_type.deleted import Deleted
from enum_type.element_config_type import ElementConfigType
from enum_type.input_type import ValueType
from enum_type.user_data_type import UserDataType
from helper.data_fields_match_helper import match_fields
from helper.oss_helper.oss_helper import oss_helper1
from helper.result_helper import execute_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.time_helper import current_time


def generate_input_data(version_id, origin_input_data=None):
    sync_input_sql = (
        f"select t2.id, t2.element_id, t2.json_key, t2.nick_name, t2.value_type, "
        f"t2.start_key, t2.end_key, t2.step_key, t2.single_key, t2.single_value_data_type, "
        f"t2.description, t2.sort "
        f"from ml_sync_input_element t1 "
        f"left join ml_sync_element_config t2 on "
        f"t1.id=t2.element_id "
        f"and t1.version_id=t2.version_id "
        f"where t1.version_id='{version_id}' and "
        f"t2.is_deleted={Deleted.No.value} and "
        f"t2.element_type='{ElementConfigType.Input.value}' "
        f"order by t2.sort"
    )
    input_data = {}
    sync_input_params_arr = db_helper1.fetchall(sync_input_sql, [])
    for param in sync_input_params_arr:
        value_type = param.get("value_type")
        json_key = param.get("json_key")
        start_key = param.get("start_key")
        end_key = param.get("end_key")
        step_key = param.get("step_key")
        single_key = param.get("single_key")
        single_value_data_type = param.get("single_value_data_type")
        config_id = param.get("id")
        element_id = param.get("element_id")
        if not json_key:
            return None
        if origin_input_data and origin_input_data.get(json_key, None):
            input_data[json_key] = origin_input_data.get(json_key)
        else:
            if value_type == ValueType.Table.value:
                column_sql = (
                    f"select column_name as nick_name, column_code as name, data_type "
                    f"from ml_sync_element_column "
                    f"where element_config_id='{config_id}' "
                    f"and is_deleted={Deleted.No.value} "
                    f"order by sort"
                )
                columns = db_helper1.fetchall(column_sql)
                # 判断是否有测试数据, 并判断列是否完全匹配
                execute_result = get_test_data(version_id, element_id)
                test_data = execute_result.data
                if match_fields(test_data, columns):
                    input_data[json_key] = test_data
                else:
                    table_data = generate_table_data(columns)
                    input_data[json_key] = table_data
            elif value_type == ValueType.IntRange.value:
                if not start_key or not end_key or not step_key:
                    return None
                input_data[json_key] = {}
                input_data[json_key][start_key] = 1
                input_data[json_key][end_key] = 50
                input_data[json_key][step_key] = 1
            elif value_type == ValueType.YearRange.value:
                if not start_key or not end_key:
                    return None
                input_data[json_key] = {}
                input_data[json_key][start_key] = 2000
                input_data[json_key][end_key] = 2020
            elif value_type == ValueType.MonthRange.value:
                if not start_key or not end_key:
                    return None
                input_data[json_key] = {}
                input_data[json_key][start_key] = "2000-01"
                input_data[json_key][end_key] = "2002-01"
            elif value_type == ValueType.DayRange.value:
                if not start_key or not end_key:
                    return None
                input_data[json_key] = {}
                input_data[json_key][start_key] = "2000-01-01"
                input_data[json_key][end_key] = "2000-02-01"
            elif value_type == ValueType.SingleValue.value:
                if not single_key:
                    return None
                input_data[json_key] = {}
                if single_value_data_type == UserDataType.Number.value:
                    input_data[json_key][single_key] = 10
                elif single_value_data_type == UserDataType.Varchar2.value:
                    input_data[json_key][single_key] = (
                        "那一天我二十一岁，在我一生的黄金时代，我有好多奢望。"
                        "我想爱，想吃，还想在一瞬间变成天上半明半暗的云，"
                        "后来我才知道，生活就是个缓慢受锤的过程，人一天天老下去，奢望也一天天消逝，"
                        "最后变得像挨了锤的牛一样。可是我过二十一岁生日时没有预见到这一点。"
                        "我觉得自己会永远生猛下去，什么也锤不了我。"
                    )
                elif single_value_data_type == UserDataType.Date.value:
                    input_data[json_key][single_key] = "2022-06-30"
    return input_data


def generate_table_data(columns):
    table_data = []
    for i in range(20):
        d = {}
        for column in columns:
            name = column["name"]
            data_type = column["data_type"]
            if data_type == UserDataType.Number.value:
                random.seed(0)
                d[name] = round(random.uniform((i + 1) * 10, (i + 1) * 20), 3)
            elif data_type == UserDataType.Varchar2.value:
                random.seed(0)
                d[name] = str(round(random.uniform((i + 1) * 10, (i + 1) * 20), 3))
            elif data_type == UserDataType.Date.value:
                d[name] = f"2020-01-{i + 1}"
        table_data.append(d)
    return table_data


def create_test_data(user_id, version_id, element_id, json_data):
    """
    利用上传的测试数据创建 JSON 文件
    :param user_id: 用户标识
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param json_data: 测试数据
    :return: 成功、失败
    """
    json_data_str = json.dumps(json_data)
    data_path = oss_helper1.create_json_file(json_data_str)
    now = current_time()
    configuration_sql = (
        f"merge into ml_sync_input_test_data t1 "
        f"using (select "
        f"'{element_id}' element_id, "
        f"'{version_id}' version_id, "
        f"'{user_id}' create_user, "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss') create_time "
        f"from dual) t2 on (t1.element_id = t2.element_id and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, "
        f"element_id, version_id, data_path) values"
        f"(createguid(), "
        f"t2.create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.element_id, t2.version_id, '{data_path}') "
        f"when matched then "
        f"update set t1.data_path='{data_path}'"
    )
    return db_helper1.execute(configuration_sql)


def get_test_data(version_id, element_id):
    """
    获取上传的测试数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :return: 测试数据
    """
    sync_input_test_data_sql = (
        f"select data_path "
        f"from ml_sync_input_test_data "
        f"where version_id='{version_id}' "
        f"and element_id='{element_id}'"
    )
    sync_input_test_data_dict = db_helper1.fetchone(sync_input_test_data_sql, [])
    if not sync_input_test_data_dict:
        return execute_success(data=[])
    data_path = sync_input_test_data_dict.get("data_path")
    if not data_path:
        return execute_success(data=[])
    # 读取 minio
    json_data_str = oss_helper1.get_json_file_data(data_path)
    json_data = json.loads(json_data_str)
    return execute_success(data=json_data)


def delete_test_data(version_id, element_id):
    sql = f"delete from ml_sync_input_test_data " f"where element_id='{element_id}' " f"and version_id='{version_id}'"
    return db_helper1.execute(sql)
