from enum_type.pipelining_operation_type import PipeliningOperationType
from helper.generate_helper import uuid_and_now
from helper.sql_helper.init_sql_helper import db_helper1


def get_version(publish_id):
    """
    根据发布标识获取对应发布的流水线版本标识
    :param publish_id: 发布标识
    :return: 流水线版本标识
    """
    # noinspection PyBroadException
    try:
        if not publish_id:
            return None
        version_sql = f"select version_id " f"from ml_pipelining_publish " f"where id='{publish_id}'"
        version_dict = db_helper1.fetchone(version_sql)
        if not version_dict or "version_id" not in version_dict:
            return None
        return version_dict["version_id"]
    except Exception:
        return None


# #


def insert_publish(publish_id, user_id, process_id, status):
    """
    更新调用次数与失败率/成功率表，用于服务监控，审计
    :param publish_id:
    :param user_id:
    :param process_id:
    :param status:
    :return:
    """
    if not publish_id:
        return None
    pipelining_process_id, now = uuid_and_now()
    insert_publish_process_sql = (
        f"insert into ml_pipelining_process_status values("
        f"'{pipelining_process_id}', "
        f"'{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{publish_id}', "
        f"'{process_id}', "
        f"{status})"
    )
    operation_sql = (
        f"insert into ml_pipelining_audit values ("
        f"CREATEGUID(), "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{publish_id}', '{PipeliningOperationType.Call.value}', null)"
    )
    return db_helper1.execute_arr([insert_publish_process_sql, operation_sql])


def update_process(process_id, version_id):
    update_process_sql = f"update ml_pipelining_version " f"set process_id='{process_id}' " f"where id='{version_id}'"
    return db_helper1.execute(update_process_sql)
