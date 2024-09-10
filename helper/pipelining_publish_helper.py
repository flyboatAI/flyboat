from config.setting import PUBLISH_HOST
from enum_type.deleted import Deleted
from enum_type.pipelining_operation_type import PipeliningOperationType
from helper.generate_helper import generate_uuid
from helper.sql_helper.init_sql_helper import db_helper1
from helper.time_helper import current_time


def publish_list_by_tag(tag_id, user_id):
    tag_sql = f"and t2.tag_id='{tag_id}'" if tag_id else ""
    tag_join = "left join ml_publish_tag t2 on t1.id=t2.publish_id " if tag_id else ""
    tag_delete = f"and t2.is_deleted={Deleted.No.value} " if tag_id else ""
    publish_list_sql = (
        f"select t1.id as publish_id, t1.version_id, t1.publish_name, t1.description, "
        f"t3.version_name, t3.logic_flow, t4.pipelining_name, t4.id as pipelining_id, "
        f"t1.update_time, t5.name, concat('{PUBLISH_HOST}', t1.id) as url "
        f"from ml_pipelining_publish t1 "
        f"left join ml_pipelining_version t3 "
        f"on t1.version_id=t3.id "
        f"left join ml_pipelining t4 "
        f"on t4.id=t3.pipelining_id "
        f"left join blade_user t5 "
        f"on t5.id=t1.update_user "
        f"{tag_join} "
        f"where t1.create_user='{user_id}' "
        f"and t1.is_deleted={Deleted.No.value} "
        f"and t3.is_deleted={Deleted.No.value} "
        f"and t4.is_deleted={Deleted.No.value} "
        f"{tag_delete} "
        f"{tag_sql} "
        f"order by t1.update_time desc"
    )
    return db_helper1.fetchall(publish_list_sql)


def all_publish_list(experiment_id=None):
    experiment_sql = f"and t4.experiment_id='{experiment_id}'" if experiment_id else ""
    publish_list_sql = (
        f"select t1.id as publish_id, t1.version_id, t1.publish_name, t1.description, "
        f"t3.version_name, t3.logic_flow, t4.pipelining_name, t4.id as pipelining_id, "
        f"t1.update_time, t5.name, concat('{PUBLISH_HOST}', t1.id) as url "
        f"from ml_pipelining_publish t1 "
        f"left join ml_pipelining_version t3 "
        f"on t1.version_id=t3.id "
        f"left join ml_pipelining t4 "
        f"on t4.id=t3.pipelining_id "
        f"left join blade_user t5 "
        f"on t5.id=t1.update_user "
        f"where t1.is_deleted={Deleted.No.value} "
        f"and t3.is_deleted={Deleted.No.value} "
        f"and t4.is_deleted={Deleted.No.value} "
        f"{experiment_sql} "
        f"order by t1.update_time desc"
    )
    return db_helper1.fetchall(publish_list_sql)


def get_call_count(start_time, end_time, publish_id):
    count_sql = (
        f"select to_char(trunc(create_time), 'yyyy-mm-dd') as call_date, "
        f"count(1) as call_count "
        f"from ml_pipelining_process_status "
        f"where create_time "
        f"between to_date('{start_time}', 'yyyy-mm-dd hh24:mi:ss') "
        f"and to_date('{end_time}', 'yyyy-mm-dd hh24:mi:ss') "
        f"and publish_id='{publish_id}' "
        f"group by publish_id, trunc(create_time) "
        f"order by trunc(create_time)"
    )
    return db_helper1.fetchall(count_sql)


def get_status_count(start_time, end_time, publish_id):
    count_sql = (
        f"select "
        f"sum(case when status=0 then 1 else 0 end) as success_count, "
        f"sum(case when status=1 then 1 else 0 end) as failure_count "
        f"from ml_pipelining_process_status "
        f"where create_time "
        f"between to_date('{start_time}', 'yyyy-mm-dd hh24:mi:ss') "
        f"and to_date('{end_time}', 'yyyy-mm-dd hh24:mi:ss') "
        f"and publish_id='{publish_id}'"
    )
    return db_helper1.fetchone(count_sql)


def get_audit(publish_id):
    audit_sql = (
        f"select t1.*, t2.name from ml_pipelining_audit t1 "
        f"left join blade_user t2 "
        f"on t2.id=t1.create_user "
        f"where t1.publish_id='{publish_id}' "
        f"and t1.operation in "
        f"("
        f"'{PipeliningOperationType.Publish.value}', "
        f"'{PipeliningOperationType.CancelPublish.value}'"
        f") "
        f"order by t1.create_time desc"
    )
    return db_helper1.fetchall(audit_sql)


def change_publish_name_and_description(publish_id, publish_name, description):
    update_sql = (
        f"update ml_pipelining_publish "
        f"set publish_name='{publish_name}', "
        f"description='{description}' "
        f"where id='{publish_id}'"
    )
    return db_helper1.execute(update_sql)


def associate_client_with_publish_id_list(user_id, client_id, experiment_id, publish_id_list):
    delete_sql = f"delete from ml_publish_client where system_code='{client_id}'"
    now = current_time()
    if not experiment_id:
        experiment_id = ""
    begin_configuration_sql = (
        "insert into ml_publish_client(id, is_deleted, create_user, "
        "create_time, system_code, experiment_id, "
        "publish_id) "
    )
    configuration_sql_arr = []
    for publish_id in publish_id_list:
        uuid_ = generate_uuid()
        configuration_sql = (
            f"select '{uuid_}', {Deleted.No.value},'{user_id}', "
            f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
            f"'{client_id}', '{experiment_id}', '{publish_id}' from dual"
        )
        configuration_sql_arr.append(configuration_sql)
    union_sql = " union ".join(configuration_sql_arr)
    sql = begin_configuration_sql + " ( " + union_sql + " ) "
    return db_helper1.execute_arr([delete_sql, sql])


def get_all_client():
    sql = (
        "select system_code as client_id, system_name as additional_information "
        "from sys_system order by system_code desc"
    )
    return db_helper1.fetchall(sql)


def get_client_publish_list():
    sql = (
        "select t1.system_code as client_id, t1.system_name as client_name, "
        "t6.space_name, t2.experiment_id, "
        "t3.id as publish_id, t3.publish_name, t4.version_name, "
        "t5.NAME as user_name, t2.create_time from sys_system t1 "
        "inner join ml_publish_client t2 "
        "on t1.system_code=t2.system_code "
        "inner join ml_pipelining_publish t3 "
        "on t2.publish_id=t3.id "
        "inner join ml_pipelining_version t4 "
        "on t4.id=t3.version_id "
        "inner join blade_user t5 "
        "on t5.id=t2.create_user "
        "left join ml_experiment_space t6 "
        "on t6.id=t2.experiment_id"
    )
    return db_helper1.fetchall(sql)


def delete_client_publish_list(client_id):
    sql = f"delete from ml_publish_client where system_code='{client_id}'"
    return db_helper1.execute(sql)
