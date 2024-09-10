from helper.generate_helper import uuid_and_now


def output_port_record_sql(version_id, element_id, user_id):
    """
    各算子输出端口的信息保存, 目的是用于之后算子获取输入信息, 在该算子进行弹窗配置时使用
    :param version_id: 流水线版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :return: SQL
    """
    # if fields_arr is None:
    #     fields_arr = []
    # if role_arr is None:
    #     role_arr = []
    # fields_arr_json = json.dumps(fields_arr)
    # role_arr_json = json.dumps(role_arr)

    uuid_, now = uuid_and_now()
    insert_output_record_sql = (
        f"insert into ml_element_output_record values"
        f"('{uuid_}', "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"'{element_id}', "
        f":1, "
        f":2)"
    )
    return insert_output_record_sql


def process_data_store(
    process_id,
    version_id,
    element_id,
    user_id,
    store_type=None,
    uuid_and_now_tuple=None,
):
    """
    插入执行流水, 用于洞察回看
    :param process_id: 流水标识
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param store_type: 流经算子后的输出端口的数据类型信息
    :param uuid_and_now_tuple: uuid 和当前时间
    :return: SQL
    """
    if uuid_and_now_tuple is None:
        uuid_and_now_tuple = uuid_and_now()
    uuid_, now = uuid_and_now_tuple
    # store_type = data_type if data_type is not None else 'null'
    store_type_sql = f"'{store_type}'" if store_type is not None else "null"
    insert_json_data_sql = (
        f"insert into ml_process values"
        f"('{uuid_}', "
        f"'{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"'{process_id}', "
        f":1, "
        f":2, "
        f"'{element_id}', "
        f"{store_type_sql})"
    )
    return insert_json_data_sql
