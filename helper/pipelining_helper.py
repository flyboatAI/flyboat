import asyncio
import json
import sys

from config.dag_config import (
    COPY_ELEMENT_FUNC_NAME,
    COPY_MODULE,
    DISABLE_ELEMENT_SQL_LIST_FUNC_NAME,
    DISABLE_MODULE,
    EMPTY_MODULE,
    MODEL_SYNC_OUTPUT,
    SYNC_INPUT,
    SYNC_OUTPUT,
    TABLE_NAME,
    element_info_dict,
)
from core.engine_execute_pool import ExecutePool
from core.pipelining_engine import (
    machine_learning_execute_engine,
    machine_learning_execute_engine_with_websocket,
)
from enum_type.deleted import Deleted
from enum_type.element_config_type import ElementConfigType
from enum_type.enabled import Enabled
from enum_type.input_type import ValueType
from enum_type.pipelining_operation_type import PipeliningOperationType
from enum_type.pipelining_process_status import PipeliningProcessStatus
from enum_type.result_code import ResultCode
from error.data_process_error import DataProcessError
from error.execute_error import ExecuteError
from helper.dag_helper import get_dag
from helper.generate_helper import generate_uuid, uuid_and_now
from helper.model_version_helper import version_handle
from helper.oss_helper.oss_helper import oss_helper1
from helper.result_helper import ExecuteResult, execute_error, execute_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.time_helper import current_time
from publish.pipelining_publish import insert_publish, update_process


def multiprocess_pipelining_exec(
    sync_input_data,
    version_id,
    user_id,
    publish_id,
    connect_id,
    process_id,
    need_websocket,
    to_element_id=None,
    serial_number=None,
):
    # 进程池执行流水线
    pool = ExecutePool.get_pool()
    if not pool:
        raise DataProcessError("流水线线程池为空, 请重启应用") from None
    manager_dict = ExecutePool.get_manager_dict()
    return pool.apply(
        pipelining_exec,
        args=(
            sync_input_data,
            version_id,
            user_id,
            publish_id,
            connect_id,
            process_id,
            need_websocket,
            manager_dict,
            to_element_id,
            serial_number,
        ),
    )


def pipelining_exec(
    sync_input_data,
    version_id,
    user_id,
    publish_id=None,
    connect_id=None,
    process_id=None,
    need_websocket=False,
    shared_mem=None,
    to_element_id=None,
    serial_number=None,
):
    """
    实际调度引擎执行方法
    :param shared_mem: 共享内存
    :param sync_input_data: 同步输入数据
    :param version_id: 版本标识
    :param user_id: 用户标识
    :param publish_id: 发布标识
    :param connect_id: 连接标识
    :param process_id: 执行过程标识
    :param need_websocket: 是否需要连接 websocket
    :param to_element_id: 开始执行到哪个算子
    :param serial_number: 授权序列号
    :return: response_result
    """
    if not process_id:
        process_id = generate_uuid()
    dag_arr = get_dag(version_id)

    if not dag_arr:
        raise ExecuteError(sys._getframe().f_code.co_name, "流水线配置解析失败") from None
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    if not need_websocket:
        execute_result: ExecuteResult = loop.run_until_complete(
            machine_learning_execute_engine(
                sync_input_data,
                version_id,
                user_id,
                process_id,
                dag_arr,
                connect_id,
                to_element_id=to_element_id,
                serial_number=serial_number,
            )
        )
    else:
        execute_result: ExecuteResult = loop.run_until_complete(
            machine_learning_execute_engine_with_websocket(
                sync_input_data,
                version_id,
                user_id,
                process_id,
                dag_arr,
                connect_id,
                shared_mem,
                to_element_id=to_element_id,
                serial_number=serial_number,
            )
        )
    loop.close()
    update_process(process_id, version_id)
    if publish_id:
        insert_publish(
            publish_id,
            user_id,
            process_id,
            PipeliningProcessStatus.Error.value if execute_result.code else PipeliningProcessStatus.Success.value,
        )
    if execute_result.code == ResultCode.Error.value:
        return execute_error(execute_result.message)

    return execute_success(
        data=execute_result.data.get("data") if execute_result.data is not None else None,
        message=execute_result.message,
    )


def publish_pipelining(version_id, tag_id_list, user_id, description):
    publish_id_sql = f"select id, is_deleted from ml_pipelining_publish " f"where version_id='{version_id}'"
    publish_id_dict = db_helper1.fetchone(publish_id_sql)

    if publish_id_dict and "id" in publish_id_dict and publish_id_dict["id"] and not publish_id_dict["is_deleted"]:
        raise ExecuteError(sys._getframe().f_code.co_name, "该流水线已发布") from None
    select_name_sql = (
        f"select t1.pipelining_name from ml_pipelining t1 "
        f"left join ml_pipelining_version t2 "
        f"on t1.id=t2.pipelining_id "
        f"where t2.id='{version_id}'"
    )
    name_dict = db_helper1.fetchone(select_name_sql)
    if not name_dict or "pipelining_name" not in name_dict or not name_dict["pipelining_name"]:
        raise ExecuteError(sys._getframe().f_code.co_name, "该流水线名称不存在") from None
    pipelining_name = name_dict["pipelining_name"]
    publish_id = None
    if publish_id_dict:
        publish_id = publish_id_dict.get("id")
    uuid_, now = uuid_and_now()
    if not publish_id:
        publish_id = uuid_
    merge_sql = (
        f"merge into ml_pipelining_publish t1 "
        f"using (select "
        f"'{version_id}' version_id, "
        f"'{user_id}' user_id from dual) t2 on (t1.version_id = t2.version_id) "
        f"when not matched then "
        f"insert (id, is_deleted, create_user, create_time, update_user, update_time, "
        f"version_id, publish_name, description"
        f") values"
        f"('{uuid_}', {Deleted.No.value}, "
        f"t2.user_id, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.user_id, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, "
        f"'{pipelining_name}', "
        f"'{description}') "
        f"when matched then "
        f"update set t1.is_deleted={Deleted.No.value}, "
        f"t1.publish_name='{pipelining_name}',"
        f"t1.description='{description}'"
    )

    delete_tag_sql = f"delete from ml_publish_tag " f"where publish_id='{publish_id}'"

    now = current_time()
    begin_configuration_sql = (
        "insert into ml_publish_tag(id, is_deleted, create_user, " "create_time, tag_id, " "publish_id) "
    )
    configuration_sql_arr = []
    for tag_id in tag_id_list:
        uuid_ = generate_uuid()
        configuration_sql = (
            f"select '{uuid_}', {Deleted.No.value},'{user_id}', "
            f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
            f"'{tag_id}', '{publish_id}' from dual"
        )
        configuration_sql_arr.append(configuration_sql)
    union_sql = " union ".join(configuration_sql_arr)
    insert_tag_sql = begin_configuration_sql + " ( " + union_sql + " ) "

    operation_sql = (
        f"insert into ml_pipelining_audit values ("
        f"CREATEGUID(), "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{publish_id}', '{PipeliningOperationType.Publish.value}', null)"
    )

    insert_result = db_helper1.execute_arr([merge_sql, delete_tag_sql, insert_tag_sql, operation_sql])
    if insert_result != ResultCode.Success.value:
        return execute_error("发布失败")
    return execute_success(data={"publish_id": publish_id})


def cancel_publish_pipelining(publish_id, user_id):
    select_sql = (
        f"select count(1) as count from ml_pipelining_publish "
        f"where id='{publish_id}' "
        f"and is_deleted={Deleted.No.value}"
    )
    count_dict = db_helper1.fetchone(select_sql)
    if not count_dict or count_dict["count"] == 0:
        raise ExecuteError(sys._getframe().f_code.co_name, "该流水线未发布") from None
    update_sql = f"update ml_pipelining_publish " f"set is_deleted={Deleted.Yes.value} " f"where id='{publish_id}'"
    now = current_time()
    operation_sql = (
        f"insert into ml_pipelining_audit values ("
        f"CREATEGUID(), "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{publish_id}', '{PipeliningOperationType.CancelPublish.value}', null)"
    )

    return db_helper1.execute_arr([update_sql, operation_sql])


def get_pipelining_list_by_name(user_id, experiment_id, pipelining_name):
    if not user_id or not pipelining_name:
        return []
    pipelining_list_sql = (
        f"select id, pipelining_name, "
        f"experiment_id, description "
        f"from ml_pipelining "
        f"where create_user='{user_id}' "
        f"and pipelining_name like '%{pipelining_name}%' "
        f"and is_deleted={Deleted.No.value} "
        f"and experiment_id='{experiment_id}'"
        f"order by update_time desc"
    )
    return db_helper1.fetchall(pipelining_list_sql)


def get_all_pipelining_list(user_id, experiment_id):
    if not user_id:
        return []
    pipelining_list_sql = (
        f"select id, pipelining_name, "
        f"experiment_id, description "
        f"from ml_pipelining "
        f"where create_user='{user_id}' "
        f"and is_deleted={Deleted.No.value} "
        f"and experiment_id='{experiment_id}'"
        f"order by update_time desc"
    )
    return db_helper1.fetchall(pipelining_list_sql)


def get_pipelining_and_version_tree(current, size, user_id, experiment_id, pipelining_name):
    if not user_id:
        return []
    # 判断是否根据名称查询
    like_name_sql = f"and pipelining_name like '%{pipelining_name}%' " if pipelining_name else ""
    pipelining_list_sql = (
        f"select id, pipelining_name as name, "
        f"experiment_id, description "
        f"from ml_pipelining "
        f"where create_user='{user_id}' "
        f"{like_name_sql}"
        f"and is_deleted={Deleted.No.value} "
        f"and experiment_id='{experiment_id}'"
        f"order by update_time desc"
    )
    page = db_helper1.fetchpage(pipelining_list_sql, current, size)
    # 提取流水线ID集合
    pipelining_ids = list(map(lambda x: x.get("id"), page.data))
    # 拼接条件
    in_pipelining_ids_sql = "','".join(pipelining_ids)
    # 查询流水线版本
    pipelining_version_list_sql = (
        f"select t1.id, t1.version_name as name, t1.create_time, t1.create_user,  "
        f"t1.logic_flow, t3.publish_id, t1.process_id, t1.pipelining_id as pid, "
        f"t4.name as create_user_name, "
        f"(case when t3.publish_id is null "
        f"then 0 else 1 end) as publish_status "
        f"from ml_pipelining_version t1 "
        f"left join (select * from ("
        f"select id as publish_id, version_id, row_number() "
        f"over (partition by version_id order by is_deleted) "
        f"as row_num from ml_pipelining_publish "
        f"where is_deleted={Deleted.No.value} "
        f")  t2 where t2.row_num=1)  t3 "
        f"on t1.id=t3.version_id "
        f"left join blade_user t4 on t1.create_user=t4.id "
        f"where t1.pipelining_id in ('{in_pipelining_ids_sql}') "
        f"and t1.is_deleted={Deleted.No.value} "
        f"order by t1.create_time desc"
    )
    version_data = db_helper1.fetchall(pipelining_version_list_sql)
    # 拼接子集版本数据
    for obj in page.data:
        obj["children"] = list(filter(lambda x: x.get("pid") == obj.get("id"), version_data))
    return page


def delete_pipelining_by_id(user_id, pipelining_id):
    publish_count_sql = (
        f"select count(1) as count from ml_pipelining_publish "
        f"where version_id in "
        f"(select id from ml_pipelining_version "
        f"where create_user='{user_id}' "
        f"and is_deleted={Deleted.No.value} "
        f"and pipelining_id='{pipelining_id}') "
        f"and is_deleted={Deleted.No.value}"
    )
    count_dict = db_helper1.fetchone(publish_count_sql)
    if count_dict and "count" in count_dict and count_dict["count"] > 0:
        raise ExecuteError(sys._getframe().f_code.co_name, "该流水线存在已发布的版本, 无法删除") from None
    delete_sql = (
        f"update ml_pipelining set "
        f"is_deleted={Deleted.Yes.value} "
        f"where create_user='{user_id}' "
        f"and id='{pipelining_id}'"
    )
    return db_helper1.execute(delete_sql)


def move_pipelining(experiment_id, pipelining_id):
    now = current_time()
    update_sql = (
        f"update ml_pipelining set "
        f"experiment_id='{experiment_id}', "
        f"update_time=to_date('{now}', 'yyyy-mm-dd hh24:mi:ss') "
        f"where is_deleted={Deleted.No.value} "
        f"and id='{pipelining_id}'"
    )
    return db_helper1.execute(update_sql)


def edit_pipelining(pipelining_id, pipelining_name, description):
    now = current_time()
    update_sql = (
        f"update ml_pipelining set "
        f"pipelining_name='{pipelining_name}', "
        f"description='{description}', "
        f"update_time=to_date('{now}', 'yyyy-mm-dd hh24:mi:ss') "
        f"where is_deleted={Deleted.No.value} "
        f"and id='{pipelining_id}'"
    )
    return db_helper1.execute(update_sql)


def edit_version_name(pipelining_id, version_id, version_name):
    update_sql = (
        f"update ml_pipelining_version set "
        f"version_name='{version_name}' "
        f"where id='{version_id}' and pipelining_id='{pipelining_id}'"
    )
    return db_helper1.execute(update_sql)


def add_pipelining(experiment_id, pipelining_name, description, user_id):
    pipelining_id = generate_uuid()
    version_id = generate_uuid()
    now = current_time()
    if description is None:
        description = ""
    insert_pipelining_sql = (
        f"insert into ml_pipelining values('{pipelining_id}', {Deleted.No.value}, "
        f"'{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{pipelining_name}', '{experiment_id}', '{description}')"
    )
    version_name = version_handle(0)
    logic_flow_path = oss_helper1.create_json_file(json.dumps({"nodes": [], "edges": []}))
    dag_config_path = oss_helper1.create_json_file(json.dumps({}))
    insert_version_sql = (
        f"insert into ml_pipelining_version values("
        f"'{version_id}', {Deleted.No.value}, "
        f"'{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"0, '{version_name}',"
        f"'{pipelining_id}', null, '{logic_flow_path}', '{dag_config_path}')"
    )
    return db_helper1.execute_arr([insert_pipelining_sql, insert_version_sql])


def get_pipelining_version_list(pipelining_id):
    if not pipelining_id:
        return []
    pipelining_version_list_sql = (
        f"select t1.id, t1.version_name, t1.create_time, "
        f"t1.logic_flow, t3.publish_id, t1.process_id, "
        f"(case when t3.publish_id is null "
        f"then 0 else 1 end) as publish_status "
        f"from ml_pipelining_version t1 "
        f"left join (select * from ("
        f"select id as publish_id, version_id, row_number() "
        f"over (partition by version_id order by is_deleted) "
        f"as row_num from ml_pipelining_publish "
        f"where is_deleted={Deleted.No.value} "
        f")  t2 where t2.row_num=1)  t3 "
        f"on t1.id=t3.version_id "
        f"where t1.pipelining_id='{pipelining_id}' "
        f"and t1.is_deleted={Deleted.No.value} "
        f"order by t1.create_time desc"
    )
    return db_helper1.fetchall(pipelining_version_list_sql)


def get_pipelining_logic_flow(version_id):
    sql = f"select * from " f"ml_pipelining_version " f"where id='{version_id}'"
    return db_helper1.fetchone(sql)


def config_pipelining_logic_flow(version_id, logic_flow):
    # noinspection SqlWithoutWhere
    update_sql = f"update ml_pipelining_version " f"set logic_flow=:1 where id='{version_id}'"
    relative_path = oss_helper1.create_json_file(logic_flow)
    return db_helper1.execute_arr([update_sql], {0: [relative_path]})


def config_pipelining_dag(version_id, dag):
    # noinspection SqlWithoutWhere
    update_sql = f"update ml_pipelining_version " f"set dag_config=:1 where id='{version_id}'"
    relative_path = oss_helper1.create_json_file(dag)
    return db_helper1.execute_arr([update_sql], {0: [relative_path]})


def disable_element_list(version_id, type_element_id_dict):
    sql_list = []
    for element_type, element_id_list in type_element_id_dict.items():
        element = element_info_dict.get(element_type, None)
        if not element:
            raise ExecuteError(sys._getframe().f_code.co_name, "更新算子失效失败, 未找到该算子") from None
        table_name = element[TABLE_NAME]
        if table_name and element_id_list:
            id_list_str = "','".join(element_id_list)
            # noinspection SqlResolve
            update_sql = (
                f"update {table_name} "
                f"set is_enabled={Enabled.No.value} "
                f"where id in ('{id_list_str}') "
                f"and version_id='{version_id}'"
            )
            sql_list.append(update_sql)
        module = element.get(DISABLE_MODULE, None)
        if not module:
            continue
        # noinspection PyTypeChecker
        module = __import__(module, fromlist=True)
        if not hasattr(module, DISABLE_ELEMENT_SQL_LIST_FUNC_NAME):
            continue
        disable_element_sql_list_func = getattr(module, DISABLE_ELEMENT_SQL_LIST_FUNC_NAME)
        for element_id in element_id_list:
            sql_list.extend(disable_element_sql_list_func(version_id, element_id))
    return db_helper1.execute_arr(sql_list)


def delete_pipelining_version(pipelining_id, version_id):
    publish_id_sql = f"select id, is_deleted from ml_pipelining_publish " f"where version_id='{version_id}'"
    publish_id_dict = db_helper1.fetchone(publish_id_sql)

    if publish_id_dict and "id" in publish_id_dict and publish_id_dict["id"] and not publish_id_dict["is_deleted"]:
        raise ExecuteError(sys._getframe().f_code.co_name, "该流水线已发布, 无法删除该版本") from None

    pipelining_version_count_sql = (
        f"select count(1) as count from ml_pipelining_version "
        f"where pipelining_id='{pipelining_id}' "
        f"and is_deleted={Deleted.No.value}"
    )
    pipelining_version_count_dict = db_helper1.fetchone(pipelining_version_count_sql)
    pipelining_version_count = pipelining_version_count_dict["count"]
    if pipelining_version_count <= 1:
        raise ExecuteError(sys._getframe().f_code.co_name, "该流水线只有唯一版本, 无法删除") from None
    delete_version_sql = f"update ml_pipelining_version set is_deleted={Deleted.Yes.value} " f"where id='{version_id}'"
    return db_helper1.execute(delete_version_sql)


def copy_pipelining_version(
    pipelining_id, version_id, default_delete=False, user_id=None, space_id=None, new_pipelining_id=None
):
    if not new_pipelining_id:
        max_version_sql = (
            f"select max(version) as max_version from ml_pipelining_version "
            f"where pipelining_id='{pipelining_id}' "
            f"and is_deleted={Deleted.No.value}"
        )

        max_version_dict = db_helper1.fetchone(max_version_sql)
        max_version = 0
        if max_version_dict and max_version_dict["max_version"]:
            max_version = max_version_dict["max_version"]
    else:
        max_version = -1
    version_name = version_handle(max_version + 1)
    new_version_id, now = uuid_and_now()

    # 如果 version_id 为 None, 即用户想新建空画布
    if not version_id:
        insert_empty_version_sql = (
            f"insert into ml_pipelining_version values("
            f"'{new_version_id}', {Deleted.No.value}, "
            f"'{user_id}', "
            f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
            f"{max_version + 1}, '{version_name}',"
            f"'{pipelining_id}', null, null, null)"
        )
        copy_result = db_helper1.execute(insert_empty_version_sql)
        if copy_result != ResultCode.Success.value:
            raise ExecuteError(sys._getframe().f_code.co_name, "复制版本失败") from None
        return execute_success(data={"new_version_id": new_version_id})

    config_sql = f"select logic_flow, dag_config from ml_pipelining_version " f"where id='{version_id}'"
    config_dict = db_helper1.fetchone(config_sql)

    logic_flow = config_dict["logic_flow"]
    dag_config = config_dict["dag_config"]

    new_logic_flow = oss_helper1.copy_json_file(logic_flow)
    new_dag_config = oss_helper1.copy_json_file(dag_config)
    is_deleted = Deleted.No.value if not default_delete else Deleted.Yes.value
    insert_pipelining_sql = None
    if new_pipelining_id:
        insert_pipelining_sql = (
            f"insert into ml_pipelining select '{new_pipelining_id}', is_deleted, "
            f"'{user_id}', create_time, '{user_id}', update_time, pipelining_name, '{space_id}', description "
            f"from ml_pipelining where id='{pipelining_id}'"
        )
    copy_pipelining_id = new_pipelining_id if new_pipelining_id else pipelining_id
    insert_pipelining_version_sql = (
        f"insert into ml_pipelining_version select "
        f"'{new_version_id}', {is_deleted}, '{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"{max_version + 1}, '{version_name}', "
        f"'{copy_pipelining_id}', null, '{new_logic_flow}', '{new_dag_config}' "
        f"from ml_pipelining_version "
        f"where id='{version_id}'"
    )
    # 复制每个算子配置
    for modules in element_info_dict.values():
        copy_module = modules[COPY_MODULE]
        if copy_module is None:
            raise ExecuteError(sys._getframe().f_code.co_name, "拷贝模块不存在, 请检查配置") from None
        if copy_module == EMPTY_MODULE:
            continue
        # noinspection PyTypeChecker
        module = __import__(copy_module, fromlist=True)
        if not hasattr(module, COPY_ELEMENT_FUNC_NAME):
            raise ExecuteError(sys._getframe().f_code.co_name, "拷贝模块函数不存在, 请检查配置") from None
        copy_element = getattr(module, COPY_ELEMENT_FUNC_NAME)
        copy_result = copy_element(new_version_id, version_id)
        if copy_result != ResultCode.Success.value:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"复制模块 {copy_module} 时出现异常导致复制失败",
            ) from None

    # ml_element_output_record 复制
    insert_element_output_record_sql = (
        f"insert into ml_element_output_record select "
        f"createguid(), '{user_id}', "
        f"create_time, "
        f"'{new_version_id}', "
        f"element_id, fields_arr, role_arr "
        f"from ml_element_output_record "
        f"where version_id='{version_id}'"
    )

    copy_result = db_helper1.execute_arr(
        [insert_pipelining_version_sql, insert_element_output_record_sql]
        if not insert_pipelining_sql
        else [insert_pipelining_sql, insert_pipelining_version_sql, insert_element_output_record_sql]
    )
    if copy_result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "复制版本失败") from None
    return execute_success(data={"new_version_id": new_version_id})


def get_sync_element_list(version_id, element_type=ElementConfigType.Input.value):
    """
    获取某版本流水线全部同步输入算子列表
    :param version_id: 版本标识
    :param element_type: 元素类型
    :return: 某版本同步输入算子列表数据
    """
    sync_input_element = element_info_dict.get(SYNC_INPUT, None)
    sync_input_table_name = sync_input_element.get(TABLE_NAME)

    sync_output_element = element_info_dict.get(SYNC_OUTPUT, None)
    sync_output_table_name = sync_output_element.get(TABLE_NAME)

    model_sync_output_element = element_info_dict.get(MODEL_SYNC_OUTPUT, None)
    model_sync_output_table_name = model_sync_output_element.get(TABLE_NAME)

    table_name_dict = {
        ElementConfigType.Input.value: sync_input_table_name,
        ElementConfigType.Output.value: sync_output_table_name,
        ElementConfigType.ModelOutput.value: model_sync_output_table_name,
    }
    table_name = table_name_dict.get(element_type)
    # noinspection SqlResolve
    sync_element_sql = (
        f"select t1.id as element_id, t2.id, t2.json_key, t2.nick_name, "
        f"t2.description, t2.sort, t2.value_type "
        f"from {table_name} t1 "
        f"left join ml_sync_element_config t2 on "
        f"t1.id=t2.element_id "
        f"and t1.version_id=t2.version_id "
        f"where t1.version_id='{version_id}' and "
        f"t2.is_deleted={Deleted.No.value} and "
        f"t2.element_type='{element_type}' "
        f"order by t2.sort, t1.create_time"
    )
    return db_helper1.fetchall(sync_element_sql, [])


def config_order(element_list, version_id):
    update_order_sql_arr = [
        f"update ml_sync_element_config set sort={x.get('sort')} "
        f"where id='{x.get('id')}' "
        f"and version_id='{version_id}'"
        for x in element_list
    ]
    return db_helper1.execute_arr(update_order_sql_arr)


def get_input_params(version_id):
    sync_input_element_list = get_sync_element_list(version_id)
    if not sync_input_element_list:
        return execute_success(data=[])
    for item in sync_input_element_list:
        if item["value_type"] == ValueType.Table.value:
            config_id = item["id"]
            sync_input_column_sql = (
                f"select column_name as nick_name, "
                f"column_code as name, "
                f"data_type, sort, remark "
                f"from ml_sync_element_column "
                f"where element_config_id='{config_id}' "
                f"and is_deleted={Deleted.No.value} order by sort"
            )
            fields = db_helper1.fetchall(sync_input_column_sql)
            item["fields"] = fields
    return execute_success(data=sync_input_element_list)


def get_output_params(version_id):
    sync_output_element_list = get_sync_element_list(version_id, ElementConfigType.Output.value)
    if not sync_output_element_list:
        return execute_success(data=[])
    for item in sync_output_element_list:
        if item["value_type"] == ValueType.Table.value:
            config_id = item["id"]
            sync_output_column_sql = (
                f"select column_name as nick_name, "
                f"column_code as name, "
                f"data_type, sort, remark "
                f"from ml_sync_element_column "
                f"where element_config_id='{config_id}' "
                f"and is_deleted={Deleted.No.value} order by sort"
            )
            fields = db_helper1.fetchall(sync_output_column_sql)
            item["fields"] = fields
    return execute_success(data=sync_output_element_list)


def get_experiment_space_list(user_id):
    space_sql = (
        f"select * "
        f"from ml_experiment_space "
        f"where create_user='{user_id}' "
        f"and is_deleted={Deleted.No.value} "
        f"order by create_time desc"
    )
    return db_helper1.fetchall(space_sql)


def save_experiment_space(user_id, space_name, space_description, placeholder):
    space_sql = (
        f"insert into ml_experiment_space "
        f"values(CREATEGUID(), {Deleted.No.value},"
        f"'{user_id}', to_date('{current_time()}', 'yyyy-mm-dd hh24:mi:ss'),"
        f"'{space_name}', '{space_description}', '{placeholder}')"
    )
    return db_helper1.execute(space_sql)


def save_experiment_space_with_id(user_id, space_id, space_name, space_description):
    space_sql = (
        f"update ml_experiment_space "
        f"set space_name='{space_name}', space_description='{space_description}' "
        f"where id='{space_id}' and create_user='{user_id}'"
    )
    return db_helper1.execute(space_sql)


def delete_experiment_space_with_id(user_id, space_id):
    space_sql = (
        f"update ml_experiment_space "
        f"set is_deleted={Deleted.Yes.value} "
        f"where id='{space_id}' and create_user='{user_id}'"
    )
    return db_helper1.execute(space_sql)


def delete_pipelining_version_multi(version_ids):
    version_ids_sql = "','".join(version_ids)
    # 查询版本是否已发布
    publish_id_sql = f"select id, is_deleted from ml_pipelining_publish " f"where version_id in ('{version_ids_sql}')"
    publish_id_dict = db_helper1.fetchone(publish_id_sql)
    if publish_id_dict and "id" in publish_id_dict and publish_id_dict["id"] and not publish_id_dict["is_deleted"]:
        raise ExecuteError(sys._getframe().f_code.co_name, "包含已发布流水线, 无法删除该版本") from None
    # 根据版本ID查询数据，根据流水线ID分组统计数量
    pipelining_version_sql = (
        f"select pipelining_id,count(1) as count from ml_pipelining_version "
        f"where id in ('{version_ids_sql}') "
        f"and is_deleted={Deleted.No.value} "
        f"group by pipelining_id"
    )
    pipelining_version_dict = db_helper1.fetchall(pipelining_version_sql)
    # 提取流水线ID
    pipelining_ids = list(map(lambda x: x.get("pipelining_id"), pipelining_version_dict))
    # 拼接条件
    in_pipelining_ids_sql = "','".join(pipelining_ids)
    # 查询流水线ID下有多少版本
    pipelining_version_count_sql = (
        f"select pipelining_id,count(1) as count from ml_pipelining_version "
        f"where pipelining_id in ('{in_pipelining_ids_sql}') "
        f"and is_deleted={Deleted.No.value} "
        f"group by pipelining_id"
    )
    pipelining_version_count_dict = db_helper1.fetchall(pipelining_version_count_sql)
    # 转map 好查询
    mp = dict(
        map(
            lambda x: (x.get("pipelining_id"), x.get("count")),
            pipelining_version_count_dict,
        )
    )
    # 循环
    for info in pipelining_version_dict:
        # 判断要删除的版本是否时流水线下所有版本，流水线下必须留一个版本
        if info.get("count") >= mp[info.get("pipelining_id")]:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                "当前操作会清空单个流水线下所有版本，不可执行此操作",
            ) from None

    delete_version_sql = (
        f"update ml_pipelining_version set is_deleted={Deleted.Yes.value} " f"where id in ('{version_ids_sql}') "
    )
    return db_helper1.execute(delete_version_sql)
