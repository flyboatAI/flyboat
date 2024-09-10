import json
import sys

from enum_type.deleted import Deleted
from enum_type.element_config_type import ElementConfigType
from enum_type.enabled import Enabled
from enum_type.input_type import ValueType
from enum_type.result_code import ResultCode
from enum_type.user_data_type import UserDataType
from error.element_configuration_config_error import ElementConfigurationConfigError
from helper.data_store_helper import output_port_record_sql
from helper.fields_helper import generate_fields
from helper.generate_helper import generate_uuid, uuid_and_now
from helper.result_helper import execute_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.time_helper import current_time
from helper.warning_helper import UNUSED
from helper.wrapper_helper import valid_delete_sql, valid_init_sql


def valid_same_name_json_key(version_id: str, element_id: str, json_key: str):
    """
    验证是否存在同名 json_key
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param json_key: 服务标识
    :return:
    """
    sql = (
        f"select count(*) as c from ml_sync_element_config "
        f"where version_id='{version_id}' "
        f"and is_deleted={Deleted.No.value} "
        f"and json_key='{json_key}' "
        f"and element_type='{ElementConfigType.Input.value}' "
        f"and element_id <> '{element_id}'"
    )
    count_dict = db_helper1.fetchone(sql)
    return not (count_dict and count_dict["c"] > 0)


def get(version_id, element_id):
    """
    获取数据算子弹窗数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :return: {
                "json_key": "",
                "nick_name": "",
                "description": "",
                "value_type": "Table",
                "start_key": "",
                "end_key": "",
                "step_key": "",
                "single_key: "",
                "single_value_data_type": "",
                "fields": [
                    {
                        "name": "a",
                        "nick_name": "中文",
                        "data_type": "NUMBER",
                        "remark": "备注",
                        "sort": 1
                    }
                ]
              }
    """
    sync_input_sql = (
        f"select t2.id as config_id, t2.json_key, t2.nick_name, "
        f"t2.value_type, t2.start_key, t2.end_key, t2.step_key, "
        f"t2.single_key, t2.single_value_data_type, "
        f"t2.description, t2.sort "
        f"from ml_sync_input_element t1 "
        f"left join ml_sync_element_config t2 on "
        f"t1.id=t2.element_id "
        f"and t1.version_id=t2.version_id "
        f"where t1.id='{element_id}' "
        f"and t1.version_id='{version_id}' "
        f"and t2.is_deleted={Deleted.No.value} "
        f"and t2.element_type='{ElementConfigType.Input.value}'"
    )

    sync_input_params_dict = db_helper1.fetchone(sync_input_sql, [])
    if not sync_input_params_dict:
        return execute_success(
            data={
                "config_id": "",
                "json_key": "",
                "nick_name": "",
                "description": "",
                "value_type": "table",
                "start_key": "",
                "end_key": "",
                "step_key": "",
                "single_key": "",
                "single_value_data_type": "",
                "fields": [],
            }
        )
    value_type = sync_input_params_dict.get("value_type")
    if value_type is None:
        return execute_success()
    if value_type == ValueType.Table.value:
        config_id = sync_input_params_dict.get("config_id")
        sync_input_column_sql = (
            f"select column_name as nick_name, column_code as name, data_type, sort, remark "
            f"from ml_sync_element_column where element_config_id='{config_id}' "
            f"and is_deleted={Deleted.No.value} order by sort"
        )
        fields = db_helper1.fetchall(sync_input_column_sql)
        # fields_json = sync_input_params_dict['fields']
        # fields = json.loads(fields_json)
        sync_input_params_dict["fields"] = fields
    return execute_success(data=sync_input_params_dict)


def configuration(
    version_id,
    element_id,
    user_id,
    json_key,
    nick_name,
    description,
    value_type,
    start_key,
    end_key,
    step_key,
    single_key,
    single_value_data_type,
    fields,
    sort,
):
    """
    用于保存算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param json_key: JSON Key
    :param nick_name: 用于前台显示的名称
    :param description: 参数描述
    :param value_type: 输入类型 IntRange/YearRange/MonthRange/DayRange/Table
    :param start_key: 区间类型起始值 Key
    :param end_key: 区间类型终止值 Key
    :param step_key: Int区间类型步长 Key
    :param single_key: 单值 Key
    :param single_value_data_type: 单值数据类型
    :param fields: Table 类型配置字段数组
    :param sort: 排序
    :return: 配置成功/失败
    """
    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    # 验证是否有同名 JSON_KEY
    valid = valid_same_name_json_key(version_id, element_id, json_key)
    if not valid:
        raise ElementConfigurationConfigError(
            sys._getframe().f_code.co_name,
            "当前画布中已存在相同名称服务标识的同步输入算子",
        ) from None

    if value_type == ValueType.Table.value and not fields:
        raise ElementConfigurationConfigError(sys._getframe().f_code.co_name, "表类型未配置字段信息") from None

    elif value_type == ValueType.SingleValue.value:
        fields = [generate_fields("value", "值", single_value_data_type)]
    elif value_type == ValueType.YearRange.value:
        fields = [generate_fields("year", "年份", UserDataType.Number.value)]
    elif value_type == ValueType.IntRange.value:
        fields = [generate_fields(json_key, nick_name, UserDataType.Number.value)]
    elif value_type == ValueType.MonthRange.value:
        fields = [generate_fields("month", "月份", UserDataType.Varchar2.value)]
    elif value_type == ValueType.DayRange.value:
        fields = [generate_fields("day", "日期", UserDataType.Varchar2.value)]
    fields_arr = [fields]
    fields_arr_json = json.dumps(fields_arr)
    role_arr_json = json.dumps([])
    output_configuration_store_sql = output_port_record_sql(version_id, element_id, user_id)
    configuration_sql = sync_input_configuration_sql(version_id, element_id, user_id)
    config_id_dict = db_helper1.fetchone(get_config_id(version_id, element_id))
    if config_id_dict is None or config_id_dict.get("id") is None:
        config_id = generate_uuid()
    else:
        config_id = config_id_dict.get("id")
    config_configuration_sql = sync_element_config_configuration_sql(
        config_id,
        version_id,
        element_id,
        user_id,
        ElementConfigType.Input.value,
        json_key,
        nick_name,
        description,
        value_type,
        start_key,
        end_key,
        step_key,
        single_key,
        single_value_data_type,
        sort,
    )
    delete_sync_input_column_sql = delete_sync_element_column_configuration_sql(config_id)
    column_configuration_sql = sync_element_column_configuration_sql(user_id, config_id, fields)
    return db_helper1.execute_arr(
        [
            output_configuration_store_sql,
            configuration_sql,
            config_configuration_sql,
            delete_sync_input_column_sql,
            column_configuration_sql,
        ]
        if value_type == ValueType.Table.value
        else [
            output_configuration_store_sql,
            configuration_sql,
            config_configuration_sql,
        ],
        {0: [fields_arr_json, role_arr_json]},
    )


def get_config_id(version_id, element_id):
    """
    获取同步算子配置表标识，用于关联更新列配置表
    :param version_id: version_id
    :param element_id: element_id
    :return: 获取配置表标识 SQL
    """
    sql = (
        f"select id from ml_sync_element_config "
        f"where is_deleted={Deleted.No.value} and "
        f"version_id='{version_id}' and "
        f"element_id='{element_id}'"
    )
    return sql


def sync_input_configuration_sql(version_id, element_id, user_id):
    """
    生成算子配置初始化的 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :return: SQL
    """
    now = current_time()
    # 插入或更新
    configuration_sql = (
        f"merge into ml_sync_input_element t1 "
        f"using (select "
        f"'{element_id}' id, "
        f"'{version_id}' version_id, "
        f"'{user_id}' create_user, "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss') create_time "
        f"from dual) t2 on (t1.id = t2.id and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, "
        f"version_id, is_enabled) values"
        f"(t2.id, "
        f"t2.create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, {Enabled.Yes.value}) "
        f"when matched then "
        f"update set t1.is_enabled={Enabled.Yes.value}"
    )
    return configuration_sql


def sync_element_config_configuration_sql(
    config_id,
    version_id,
    element_id,
    user_id,
    element_type,
    json_key,
    nick_name=None,
    description=None,
    value_type=None,
    start_key=None,
    end_key=None,
    step_key=None,
    single_key=None,
    single_value_data_type=None,
    sort=0,
):
    """

    :param config_id: 配置表标识
    :param version_id: version_id
    :param element_id: element_id
    :param user_id: user_id
    :param element_type: 输入、输出配置
    :param json_key: 参数的 Key
    :param nick_name: 参数在页面显示的中文
    :param description: 参数在页面显示的描述信息
    :param value_type: 输入类型
    :param start_key: 起始 Key
    :param end_key: 终止 Key
    :param step_key: 步长 Key
    :param single_key: 单值 Key
    :param single_value_data_type: 单值数据类型
    :param sort: 排序，用于页面显示顺序
    :return: SQL
    """
    now = current_time()
    if not nick_name:
        nick_name = ""
    if not json_key:
        json_key = ""
    if not start_key:
        start_key = ""
    if not end_key:
        end_key = ""
    if not step_key:
        step_key = ""
    if not single_key:
        single_key = ""
    if not single_value_data_type:
        single_value_data_type = ""
    if not description:
        description = ""
    if value_type is None:
        value_type = ValueType.Table.value
    configuration_sql = (
        f"merge into ml_sync_element_config t1 "
        f"using (select "
        f"'{config_id}' id, "
        f"'{version_id}' version_id, "
        f"'{element_id}' element_id, "
        f"'{element_type}' element_type, "
        f"'{user_id}' user_id, "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss') create_time, "
        f"'{json_key}' json_key, "
        f"'{nick_name}' nick_name, "
        f"'{description}' description, "
        f"'{value_type}' value_type, "
        f"'{start_key}' start_key, "
        f"'{end_key}' end_key, "
        f"'{step_key}' step_key, "
        f"'{single_key}' single_key, "
        f"'{single_value_data_type}' single_value_data_type, "
        f"{sort} sort "
        f"from dual) t2 on ("
        f"t1.element_id = t2.element_id and "
        f"t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, is_deleted, create_user, create_time, element_id, version_id, "
        f"nick_name, description, json_key, value_type, "
        f"start_key, end_key, step_key, single_key, single_value_data_type, "
        f"element_type, sort) values"
        f"(t2.id, 0, "
        f"t2.user_id, "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.element_id, "
        f"t2.version_id, "
        f"t2.nick_name, "
        f"t2.description, "
        f"t2.json_key,"
        f"t2.value_type, "
        f"t2.start_key, "
        f"t2.end_key, "
        f"t2.step_key, "
        f"t2.single_key, "
        f"t2.single_value_data_type, "
        f"t2.element_type, "
        f"t2.sort) "
        f"when matched then "
        f"update set t1.nick_name=t2.nick_name, "
        f"t1.description=t2.description, "
        f"t1.json_key=t2.json_key, "
        f"t1.value_type=t2.value_type, "
        f"t1.start_key=t2.start_key, "
        f"t1.end_key=t2.end_key, "
        f"t1.step_key=t2.step_key, "
        f"t1.single_key=t2.single_key, "
        f"t1.single_value_data_type=t2.single_value_data_type, "
        f"t1.sort=t2.sort"
    )
    return configuration_sql


def delete_sync_element_column_configuration_sql(config_id):
    """
    删除已存在的配置列信息
    :param config_id: 配置表标识
    :return: SQL
    """
    configuration_sql = (
        f"update ml_sync_element_column "
        f"set is_deleted={Deleted.Yes.value} "
        f"where element_config_id='{config_id}'"
    )
    return configuration_sql


def sync_element_column_configuration_sql(user_id, config_id, fields):
    """
    列配置 SQL
    :param user_id: user_id
    :param config_id: 配置表标识
    :param fields: 字段数组信息
    :return: SQL
    """
    now = current_time()
    begin_configuration_sql = (
        "insert into ml_sync_element_column(id, is_deleted, "
        "create_user, create_time, "
        "element_config_id, column_name, column_code, "
        "data_type, sort, remark) "
    )
    configuration_sql_arr = []
    field_len = len(fields)
    for i in range(field_len):
        uuid_ = generate_uuid()
        field = fields[i]
        column_name = field.get("nick_name")
        column_code = field.get("name")
        data_type = field.get("data_type")
        remark = field.get("remark")
        configuration_sql = (
            f"select '{uuid_}', 0, '{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
            f"'{config_id}', '{column_name if column_name is not None else ''}', "
            f"'{column_code if column_code is not None else ''}', "
            f"'{data_type}', "
            f"{i}, "
            f"'{remark if remark is not None else ''}' from dual"
        )
        configuration_sql_arr.append(configuration_sql)
    union_sql = " union ".join(configuration_sql_arr)
    return begin_configuration_sql + " ( " + union_sql + " ) "


def sync_element_column_configuration_dict_sql(user_id, config_id, fields_dict):
    """
    列配置 SQL
    :param user_id: user_id
    :param config_id: 配置表标识
    :param fields_dict: 字段字典信息
    :return: SQL
    """
    now = current_time()
    begin_configuration_sql = (
        "insert into ml_sync_element_column(id, is_deleted, "
        "create_user, create_time, "
        "element_config_id, column_name, column_code, "
        "data_type, sort, remark) "
    )
    configuration_sql_arr = []
    for k in fields_dict.keys():
        fields = fields_dict[k]
        field_len = len(fields)
        for i in range(field_len):
            uuid_ = generate_uuid()
            field = fields[i]
            column_name = field.get("nick_name")
            column_code = field.get("name")
            data_type = field.get("data_type")
            configuration_sql = (
                f"select '{uuid_}', 0, '{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
                f"'{config_id}', '{column_name if column_name is not None else ''}', "
                f"'{column_code if column_code is not None else ''}', "
                f"'{data_type}', "
                f"{i}, "
                f"'{k}' from dual"
            )
            configuration_sql_arr.append(configuration_sql)
    union_sql = " union ".join(configuration_sql_arr)
    return begin_configuration_sql + " ( " + union_sql + " ) "


def init_element(version_id, element_id, user_id, node_type):
    """
    初始化算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param node_type: 节点类型
    :return: 配置成功/失败
    """
    init_sql, init_config_sql = generate_init_sql(version_id, element_id, user_id, node_type)
    if init_sql is None:
        return ResultCode.Success.value
    return db_helper1.execute_arr([init_sql, init_config_sql])


@valid_init_sql
def generate_init_sql(version_id, element_id, user_id, node_type):
    """
    生成算子配置初始化的 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param node_type: 节点类型
    :return: SQL
    """
    UNUSED(node_type)
    uuid_, now = uuid_and_now()
    # 初始化算子数据
    init_sql = (
        f"insert into ml_sync_input_element"
        f"(id, create_user, create_time, "
        f"version_id, is_enabled)"
        f"values"
        f"('{element_id}', "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"{Enabled.No.value})"
    )

    init_config_sql = (
        f"insert into ml_sync_element_config(id, is_deleted, create_user, create_time, "
        f"value_type, element_id, version_id, element_type)"
        f"values"
        f"('{uuid_}', {Deleted.No.value}, "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{ValueType.Table.value}', '{element_id}', '{version_id}', "
        f"'{ElementConfigType.Input.value}')"
    )
    return init_sql, init_config_sql


def copy_element(new_version_id, old_version_id):
    """
    拷贝算子数据
    :param new_version_id: 新版本标识
    :param old_version_id: 旧版本标识
    :return: 拷贝成功/失败
    """
    sql = (
        f"select id from ml_sync_element_config "
        f"where version_id='{old_version_id}' "
        f"and value_type='{ValueType.Table.value}' "
        f"and is_deleted={Deleted.No.value} "
        f"and element_type='{ElementConfigType.Input.value}'"
    )
    id_dict_list = db_helper1.fetchall(sql)
    id_list = [x.get("id") for x in id_dict_list]
    copy_sql_arr = generate_copy_sql(new_version_id, old_version_id, id_list)
    return db_helper1.execute_arr(copy_sql_arr)


def generate_copy_sql(new_version_id, old_version_id, id_list):
    """
    生成算子拷贝的 SQL 语句
    :param new_version_id: 新版本标识
    :param old_version_id: 旧版本标识
    :param id_list: 表类型的配置标识列表
    :return: SQL
    """
    now = current_time()
    # 拷贝算子数据
    copy_sql = (
        f"insert into ml_sync_input_element "
        f"select id, create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', is_enabled "
        f"from ml_sync_input_element "
        f"where version_id='{old_version_id}'"
    )
    copy_config_sql = (
        f"insert into ml_sync_element_config(id, is_deleted, "
        f"create_user, create_time, update_user, update_time, nick_name, "
        f"description, json_key, value_type, start_key, end_key, "
        f"step_key, single_key, single_value_data_type, version_id, "
        f"element_id, element_type, publish_id, sort) select CREATEGUID(), is_deleted, "
        f"create_user, create_time, update_user, update_time, nick_name, description, "
        f"json_key, value_type, start_key, end_key, step_key, single_key, "
        f"single_value_data_type, "
        f"'{new_version_id}', element_id, element_type, null, sort "
        f"from ml_sync_element_config "
        f"where version_id='{old_version_id}' "
        f"and is_deleted={Deleted.No.value} "
        f"and value_type <> '{ValueType.Table.value}' "
        f"and element_type='{ElementConfigType.Input.value}'"
    )
    sql_arr = [copy_sql, copy_config_sql]
    for c_id in id_list:
        uuid_ = generate_uuid()
        copy_table_config_sql = (
            f"insert into ml_sync_element_config(id, is_deleted, "
            f"create_user, create_time, update_user, update_time, nick_name, "
            f"description, json_key, value_type, start_key, end_key, "
            f"step_key, single_key, single_value_data_type, version_id, "
            f"element_id, element_type, publish_id, sort) select '{uuid_}', is_deleted, "
            f"create_user, create_time, update_user, update_time, "
            f"nick_name, description, "
            f"json_key, value_type, start_key, end_key, step_key, single_key, "
            f"single_value_data_type, "
            f"'{new_version_id}', element_id, element_type, null, sort "
            f"from ml_sync_element_config "
            f"where id='{c_id}'"
        )
        sql_arr.append(copy_table_config_sql)
        copy_column_sql = (
            f"insert into ml_sync_element_column select CREATEGUID(), is_deleted, tenant_id, "
            f"create_user, create_time, update_user, update_time, '{uuid_}', "
            f"column_name, column_code, data_type, sort, remark from ml_sync_element_column "
            f"where element_config_id='{c_id}'"
        )
        sql_arr.append(copy_column_sql)
    copy_test_data_sql = (
        f"insert into ml_sync_input_test_data "
        f"select createguid(), create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"element_id, '{new_version_id}', data_path from ml_sync_input_test_data "
        f"where version_id='{old_version_id}' "
    )
    sql_arr.append(copy_test_data_sql)
    return sql_arr


def delete_element(version_id, element_id, node_type):
    """
    删除算子操作
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param node_type: 节点类型
    :return: 删除结果
    """
    delete_sql, delete_config_sql = generate_delete_sql(version_id, element_id, node_type)
    if delete_sql is None:
        return ResultCode.Success.value
    return db_helper1.execute_arr([delete_sql, delete_config_sql])


@valid_delete_sql
def generate_delete_sql(version_id, element_id, node_type):
    """
    生成删除 SQL
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param node_type: 节点类型
    :return: SQL
    """
    UNUSED(node_type)

    # 删除算子数据
    # noinspection SqlResolve
    delete_sql = f"delete from ml_sync_input_element " f"where version_id='{version_id}' " f"and id='{element_id}'"

    delete_config_sql = delete_sync_element_configuration_sql(version_id, element_id)
    return delete_sql, delete_config_sql


def delete_sync_element_configuration_sql(version_id, element_id):
    configuration_sql = (
        f"update ml_sync_element_config "
        f"set is_deleted={Deleted.Yes.value} "
        f"where version_id='{version_id}' and element_id='{element_id}'"
    )
    return configuration_sql
