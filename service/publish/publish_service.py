from enum_type.deleted import Deleted
from enum_type.element_config_type import ElementConfigType
from enum_type.input_type import ValueType
from helper.sql_helper.init_sql_helper import db_helper1


def get_sync_input_parameter_by_version(version_id):
    sql = (
        f"select * from ml_sync_element_config "
        f"where element_type='{ElementConfigType.Input.value}' "
        f"and version_id='{version_id}' "
        f"and is_deleted={Deleted.No.value} "
        f"order by sort"
    )
    parameters = []
    parameter_list = db_helper1.fetchall(sql)
    if parameter_list:
        for parameter in parameter_list:
            if parameter.get("value_type") == ValueType.Table.value:
                config_id = parameter.get("id")
                column_sql = (
                    f"select * from ml_sync_element_column "
                    f"where element_config_id='{config_id}' "
                    f"and is_deleted={Deleted.No.value} "
                    f"order by sort"
                )
                column_list = db_helper1.fetchall(column_sql)
                parameter["columns"] = column_list
            else:
                parameter["columns"] = []
            parameters.append(parameter)
    return parameters


def get_sync_output_parameter_by_version(version_id):
    sql = (
        f"select id, value_type, json_key, nick_name "
        f"from ml_sync_element_config "
        f"where element_type='{ElementConfigType.Output.value}' "
        f"and version_id='{version_id}' "
        f"and is_deleted={Deleted.No.value} "
        f"order by sort"
    )
    parameters = []
    parameter_list = db_helper1.fetchall(sql)
    if parameter_list:
        for parameter in parameter_list:
            if parameter.get("value_type") == ValueType.Table.value:
                config_id = parameter.get("id")
                column_sql = (
                    f"select column_code, column_name "
                    f"from ml_sync_element_column "
                    f"where element_config_id='{config_id}' "
                    f"and is_deleted={Deleted.No.value} "
                    f"order by sort"
                )
                column_list = db_helper1.fetchall(column_sql)
                parameter["columns"] = column_list
            else:
                parameter["columns"] = []
            parameters.append(parameter)
    return parameters
