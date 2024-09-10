from enum_type.deleted import Deleted
from enum_type.element_config_type import ElementConfigType
from enum_type.registered import Registered
from helper.sql_helper.init_sql_helper import db_helper1
from helper.time_helper import current_time


def create_new_pipelining_element(user_id, name, description, origin_version_id, origin_pipelining_id):
    insert_sql = (
        f"insert into ml_pipelining_models "
        f"values(CREATEGUID(), {Deleted.No.value},"
        f"'{user_id}', to_date('{current_time()}', 'yyyy-mm-dd hh24:mi:ss'),"
        f"'{name}', '{description}', '{origin_version_id}', '{origin_pipelining_id}', "
        f"{Registered.No.value})"
    )
    return db_helper1.execute(insert_sql)


def register_pipelining_element_by_id(user_id, pipelining_element_id):
    register_sql = (
        f"update ml_pipelining_models "
        f"set registered={Registered.Yes.value} "
        f"where id='{pipelining_element_id}' and create_user='{user_id}'"
    )
    return db_helper1.execute(register_sql)


def unregister_pipelining_element_by_id(user_id, pipelining_element_id):
    register_sql = (
        f"update ml_pipelining_models "
        f"set registered={Registered.No.value} "
        f"where id='{pipelining_element_id}' and create_user='{user_id}'"
    )
    return db_helper1.execute(register_sql)


def get_registered_pipelining_element_list(current, size, user_id, name):
    condition_sql = f"and t1.name like '%{name}%'" if name else ""
    list_sql = (
        f"select t1.*, t2.pipelining_name as origin_pipelining_name, "
        f"(select count(*) from ml_sync_element_config "
        f"where version_id=t1.origin_version_id "
        f"and element_type='{ElementConfigType.Input.value}' "
        f"and is_deleted={Deleted.No.value}) as sync_input_count, "
        f"(select count(*) from ml_sync_element_config "
        f"where version_id=t1.origin_version_id "
        f"and element_type='{ElementConfigType.Output.value}' "
        f"and is_deleted={Deleted.No.value}) as sync_output_count, "
        f"(select count(*) from ml_sync_element_config "
        f"where version_id=t1.origin_version_id "
        f"and element_type='{ElementConfigType.ModelOutput.value}' "
        f"and is_deleted={Deleted.No.value}) as sync_model_count "
        f"from ml_pipelining_models t1 "
        f"left join ml_pipelining t2 on "
        f"t1.origin_pipelining_id=t2.id "
        f"where t1.create_user='{user_id}' "
        f"and t1.is_deleted={Deleted.No.value} "
        f"and t1.registered={Registered.Yes.value} "
        f"{condition_sql} "
        f"order by t1.create_time desc"
    )
    return db_helper1.fetchpage(list_sql, current, size)


def get_pipelining_element_list(current, size, user_id, name):
    condition_sql = f"and t1.name like '%{name}%'" if name else ""
    list_sql = (
        f"select t1.*, t2.pipelining_name as origin_pipelining_name, "
        f"(select count(*) from ml_sync_element_config "
        f"where version_id=t1.origin_version_id "
        f"and element_type='{ElementConfigType.Input.value}' "
        f"and is_deleted={Deleted.No.value}) as sync_input_count, "
        f"(select count(*) from ml_sync_element_config "
        f"where version_id=t1.origin_version_id "
        f"and element_type='{ElementConfigType.Output.value}' "
        f"and is_deleted={Deleted.No.value}) as sync_output_count, "
        f"(select count(*) from ml_sync_element_config "
        f"where version_id=t1.origin_version_id "
        f"and element_type='{ElementConfigType.ModelOutput.value}' "
        f"and is_deleted={Deleted.No.value}) as sync_model_count "
        f"from ml_pipelining_models t1 "
        f"left join ml_pipelining t2 on "
        f"t1.origin_pipelining_id=t2.id "
        f"where t1.create_user='{user_id}' "
        f"and t1.is_deleted={Deleted.No.value} "
        f"{condition_sql} "
        f"order by t1.create_time desc"
    )
    return db_helper1.fetchpage(list_sql, current, size)
