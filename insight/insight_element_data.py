import json

from helper.oss_helper.oss_helper import oss_helper1
from helper.result_helper import execute_success
from helper.sql_helper.init_sql_helper import db_helper1


def insight_multi_tab_structured_data(process_id, version_id, element_id):
    """
    洞察数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param process_id: 过程表示
    :return: 洞察数据
    """
    sql = (
        f"select data, fields, store_type from ml_process "
        f"where process_id='{process_id}' and "
        f"version_id='{version_id}' and "
        f"element_id='{element_id}'"
    )
    process_dict = db_helper1.fetchone(sql)
    if process_dict:
        data_json = process_dict["data"]
        fields_json = process_dict["fields"]
        fields_dict = None
        if fields_json:
            fields_dict = json.loads(fields_json)
            process_dict["fields"] = fields_dict
        data = {}
        if data_json:
            data_dict = json.loads(data_json)
            for k in data_dict:
                path = data_dict[k]
                if not path:
                    continue
                fields = fields_dict.get(k)
                data[k] = oss_helper1.read_csv_file(path, fields)
        process_dict["data"] = data

    return execute_success(data=process_dict)


def insight_semi_structured_data(process_id, version_id, element_id):
    sql = (
        f"select data, fields, store_type from ml_process "
        f"where process_id='{process_id}' and "
        f"version_id='{version_id}' and "
        f"element_id='{element_id}'"
    )
    process_dict = db_helper1.fetchone(sql)
    if process_dict:
        data_json = process_dict["data"]
        if data_json:
            process_dict["data"] = json.loads(data_json)
        fields_json = process_dict["fields"]
        if fields_json:
            process_dict["fields"] = json.loads(fields_json)
    return execute_success(data=process_dict)


def insight_structured_data(process_id, version_id, element_id):
    sql = (
        f"select data, fields, store_type from ml_process "
        f"where process_id='{process_id}' and "
        f"version_id='{version_id}' and "
        f"element_id='{element_id}'"
    )
    process_dict = db_helper1.fetchone(sql)
    if process_dict:
        fields_json = process_dict["fields"]
        if fields_json:
            fields = json.loads(fields_json)
            process_dict["fields"] = fields
            path = process_dict["data"]
            if path:
                process_dict["data"] = oss_helper1.read_csv_file(path, fields)
            else:
                process_dict["data"] = []
    return execute_success(data=process_dict)


def recent_process_id(version_id):
    sql = f"select process_id from ml_pipelining_version " f"where id='{version_id}'"
    process_dict = db_helper1.fetchone(sql)
    if process_dict:
        process_id = process_dict["process_id"]
        return execute_success(data=process_id)
    return execute_success()
