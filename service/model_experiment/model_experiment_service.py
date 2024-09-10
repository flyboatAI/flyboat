import json
import sys

import aiohttp

from config import setting
from enum_type.deleted import Deleted
from enum_type.response_code import ResponseCode
from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from helper.generate_helper import uuid_and_now
from helper.oss_helper.oss_helper import oss_helper1
from helper.response_result_helper import response_error_result, response_result
from helper.sql_helper.init_sql_helper import db_helper1
from parameter_entity.model_experiment.execute_model_experiment import ExecuteModelExperiment
from parameter_entity.model_experiment.model_experiment import ModelExperiment
from parameter_entity.model_experiment.model_experiment_history import ModelExperimentHistory
from parameter_entity.model_experiment.model_experiment_parameters import (
    ModelExperimentParameters,
)
from publish.pipelining_publish import get_version
from service.publish.publish_service import get_sync_output_parameter_by_version


def create_new_model_experiment(user_id: str, model_experiment: ModelExperiment):
    experiment_name = model_experiment.experiment_name
    if not experiment_name:
        return ResultCode.Error.value
    uuid_, now = uuid_and_now()
    return db_helper1.execute(
        f"insert into ml_model_experiment values('{uuid_}', '{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), '{experiment_name}', {Deleted.No.value})"
    )


def edit_exist_model_experiment(model_experiment: ModelExperiment):
    experiment_name = model_experiment.experiment_name
    experiment_id = model_experiment.experiment_id
    if not experiment_name or not experiment_id:
        return ResultCode.Error.value
    return db_helper1.execute(
        f"update ml_model_experiment set experiment_name='{experiment_name}'" f"where id='{experiment_id}'"
    )


def delete_exist_model_experiment(model_experiment: ModelExperiment):
    experiment_id = model_experiment.experiment_id
    if not experiment_id:
        return ResultCode.Error.value
    return db_helper1.execute(
        f"update ml_model_experiment set is_deleted={Deleted.Yes.value} " f"where id='{experiment_id}'"
    )


def get_experiment_list(user_id: str, model_experiment: ModelExperiment):
    experiment_name = model_experiment.experiment_name
    filter_sql = f" and t1.experiment_name like '%{experiment_name}%' " if experiment_name else ""
    publish_name = model_experiment.publish_name
    filter_sql += f" and t3.publish_name like '%{publish_name}%' " if publish_name else ""

    sql = (
        f"select t1.*, t3.publish_name, t4.version_name, t6.name as user_name, "
        f"(case when t7.id is null "
        f"then 0 else 1 end) as exec_status, "
        f"(case when t3.is_deleted={Deleted.No.value} "
        f"then 1 else 0 end) as publish_status "
        f"from ml_model_experiment t1 "
        f"left join ("
        f"select id, experiment_id, publish_id, row_number() over "
        f"( partition by experiment_id order by create_time desc) as rn "
        f"from ml_model_experiment_parameters "
        f") t2 "
        f"on t1.id=t2.experiment_id "
        f"left join ml_pipelining_publish t3 "
        f"on t3.id=t2.publish_id "
        f"left join ml_pipelining_version t4 "
        f"on t3.version_id=t4.id "
        f"left join ml_pipelining t5 "
        f"on t5.id=t4.pipelining_id "
        f"left join blade_user t6 "
        f"on t6.id=t1.create_user "
        f"left join ml_model_experiment_execute t7 "
        f"on t7.experiment_id=t1.id "
        f"and t7.parameter_id=t2.id "
        f"where t1.create_user='{user_id}' "
        f"and t1.is_deleted={Deleted.No.value} "
        f"and (t2.rn = 1 or t2.rn is null) "
        f"{filter_sql} "
        f"order by t1.create_time desc"
    )
    return db_helper1.fetchpage(sql, model_experiment.current, model_experiment.size)


def get_publish_tree(user_id):
    sql = (
        f"select t1.*, t2.version_name, t3.tag_id "
        f"from ml_pipelining_publish t1 "
        f"left join ml_pipelining_version t2 "
        f"on t1.version_id=t2.id "
        f"left join ml_publish_tag t3 "
        f"on t3.publish_id=t1.id "
        f"where t1.create_user='{user_id}' "
        f"and t1.is_deleted={Deleted.No.value}"
    )
    publish_service_list = db_helper1.fetchall(sql)
    data = []
    if publish_service_list:
        sql = f"select * from ml_tag " f"where create_user='{user_id}' " f"and is_deleted={Deleted.No.value}"
        tags = db_helper1.fetchall(sql)
        for tag in tags:
            tag_id = tag.get("id", "")
            tag_name = tag.get("tag_name", "")
            children = [
                {
                    "id": s.get("id", ""),
                    "name": f"{s.get('publish_name', '')}-{s.get('version_name', '')}",
                }
                for s in publish_service_list
                if s.get("tag_id") == tag_id
            ]
            if children:
                data.append({"id": tag_id, "name": tag_name, "children": children})

    return data


def get_model_experiment_parameters_and_execute_result(experiment_id: str):
    sql = (
        f"select * from ml_model_experiment_parameters "
        f"where experiment_id='{experiment_id}' "
        f"order by create_time desc"
    )
    parameter_dict = db_helper1.fetchone(sql)
    if parameter_dict:
        parameter_id = parameter_dict.get("id")
        sql = (
            f"select t1.*, t2.publish_id, t3.publish_name, "
            f"t4.version_name, t6.name as user_name, "
            f"(case when t7.id is null "
            f"then 0 else 1 end) as exec_status, "
            f"(case when t3.is_deleted={Deleted.No.value} "
            f"then 1 else 0 end) as publish_status "
            f"from ml_model_experiment t1 "
            f"left join ml_model_experiment_parameters t2 "
            f"on t1.id=t2.experiment_id "
            f"left join ml_pipelining_publish t3 "
            f"on t3.id=t2.publish_id "
            f"left join ml_pipelining_version t4 "
            f"on t3.version_id=t4.id "
            f"left join ml_pipelining t5 "
            f"on t5.id=t4.pipelining_id "
            f"left join blade_user t6 "
            f"on t6.id=t1.create_user "
            f"left join ml_model_experiment_execute t7 "
            f"on t7.experiment_id=t1.id "
            f"where t1.id='{experiment_id}' "
            f"and t2.id='{parameter_id}'"
        )
        experiment_dict = db_helper1.fetchone(sql)
    else:
        sql = (
            f"select t1.*, null as publish_id, null as publish_name, "
            f"null as version_name, t6.name as user_name, "
            f"(case when t7.id is null "
            f"then 0 else 1 end) as exec_status, "
            f"null as publish_status "
            f"from ml_model_experiment t1 "
            f"left join blade_user t6 "
            f"on t6.id=t1.create_user "
            f"left join ml_model_experiment_execute t7 "
            f"on t7.experiment_id=t1.id "
            f"where t1.id='{experiment_id}'"
        )
        experiment_dict = db_helper1.fetchone(sql)
    if not experiment_dict:
        return experiment_dict

    experiment_dict["parameters"] = None
    experiment_dict["execute"] = None
    experiment_dict["last_execute_time"] = None
    if parameter_dict and parameter_dict["parameter_path"]:
        parameter_path = parameter_dict["parameter_path"]
        json_str = oss_helper1.get_json_file_data(parameter_path)
        parameters = json.loads(json_str)
        experiment_dict["parameters"] = parameters
        parameter_id = parameter_dict["id"]
        sql = f"select * from ml_model_experiment_execute where parameter_id='{parameter_id}'"
        execute_dict = db_helper1.fetchone(sql)
        if execute_dict and execute_dict["execute_path"]:
            execute_path = execute_dict["execute_path"]
            json_str = oss_helper1.get_json_file_data(execute_path)
            execute = json.loads(json_str)
            experiment_dict["execute"] = execute
            experiment_dict["last_execute_time"] = execute_dict["create_time"]
    return experiment_dict


def edit_model_experiment_parameters(user_id: str, model_experiment_parameters: ModelExperimentParameters):
    params = model_experiment_parameters.params
    json_str = json.dumps(params)
    path = oss_helper1.create_json_file(json_str)
    if not path:
        return ResultCode.Error.value
    uuid_, now = uuid_and_now()
    publish_id = model_experiment_parameters.publish_id
    experiment_id = model_experiment_parameters.experiment_id
    sql = (
        f"insert into ml_model_experiment_parameters "
        f"values"
        f"('{uuid_}', '{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{path}', '{publish_id}', '{experiment_id}')"
    )
    return db_helper1.execute(sql)


async def execute_model(user_id: str, execute_model_experiment: ExecuteModelExperiment):
    # 调用执行 api
    experiment_id = execute_model_experiment.experiment_id
    publish_id = execute_model_experiment.publish_id
    params = execute_model_experiment.params
    origin_params = execute_model_experiment.origin_params
    async with aiohttp.ClientSession() as session:
        body = params
        # noinspection PyBroadException
        try:
            async with session.post(url=setting.PUBLISH_HOST + publish_id, data=json.dumps(body)) as resp:
                execute_result = await resp.json()
                code = execute_result.get("code")
                if code != ResponseCode.Success.value:
                    return execute_result
                version_id = get_version(publish_id)
                fields = get_sync_output_parameter_by_version(version_id)

                uuid_, now = uuid_and_now()
                # 插入 ml_model_experiment_parameters 表
                origin_params_json_str = json.dumps(origin_params)
                origin_params_path = oss_helper1.create_json_file(origin_params_json_str)
                if not origin_params_path:
                    return response_error_result(message="执行失败")
                sql1 = (
                    f"insert into ml_model_experiment_parameters "
                    f"values"
                    f"('{uuid_}', '{user_id}', "
                    f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
                    f"'{origin_params_path}', '{publish_id}', '{experiment_id}')"
                )
                # 插入 ml_model_experiment_execute 表
                execute_json = execute_result.get("data")
                r = {"data": execute_json, "fields": fields}
                execute_json_str = json.dumps(r)
                execute_path = oss_helper1.create_json_file(execute_json_str)
                if not execute_path:
                    return response_error_result(message="执行失败")
                sql2 = (
                    f"insert into ml_model_experiment_execute "
                    f"values"
                    f"(createguid(), '{user_id}', "
                    f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
                    f"'{uuid_}', '{experiment_id}', '{execute_path}')"
                )
                insert_result = db_helper1.execute_arr([sql1, sql2])
                if insert_result != ResultCode.Success.value:
                    return response_error_result(message="执行失败")
                return response_result(data=r)
        except Exception as e:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{e}",
            ) from None


def execute_history(history: ModelExperimentHistory):
    experiment_id = history.experiment_id
    sql = (
        f"select t1.*, t4.publish_name, t5.version_name "
        f"from ml_model_experiment_execute t1 "
        f"left join ml_model_experiment_parameters t2 "
        f"on t1.parameter_id=t2.id "
        f"left join ml_model_experiment t3 "
        f"on t3.id=t1.experiment_id "
        f"left join ml_pipelining_publish t4 "
        f"on t2.publish_id=t4.id "
        f"left join ml_pipelining_version t5 "
        f"on t4.version_id=t5.id "
        f"where t1.experiment_id='{experiment_id}' "
        f"order by t1.create_time desc"
    )
    return db_helper1.fetchall(sql)


def execute_history_detail(history: ModelExperimentHistory):
    experiment_id = history.experiment_id
    execute_id = history.execute_id
    sql = f"select * from ml_model_experiment_execute " f"where id='{execute_id}'"
    execute_dict = db_helper1.fetchone(sql)
    if not execute_dict:
        return None
    parameter_id = execute_dict.get("parameter_id")
    sql = (
        f"select t1.*, t2.parameter_path, t2.publish_id, t3.publish_name, "
        f"t4.version_name, t6.name as user_name, "
        f"(case when t7.id is null "
        f"then 0 else 1 end) as exec_status, "
        f"(case when t3.is_deleted={Deleted.No.value} "
        f"then 1 else 0 end) as publish_status "
        f"from ml_model_experiment t1 "
        f"left join ml_model_experiment_parameters t2 "
        f"on t1.id=t2.experiment_id and t2.id='{parameter_id}' "
        f"left join ml_pipelining_publish t3 "
        f"on t3.id=t2.publish_id "
        f"left join ml_pipelining_version t4 "
        f"on t3.version_id=t4.id "
        f"left join ml_pipelining t5 "
        f"on t5.id=t4.pipelining_id "
        f"left join blade_user t6 "
        f"on t6.id=t1.create_user "
        f"left join ml_model_experiment_execute t7 "
        f"on t7.experiment_id=t1.id "
        f"and t7.id='{execute_id}' "
        f"and t2.id=t7.parameter_id "
        f"left join ml_publish_tag t8 "
        f"on t8.publish_id=t3.id "
        f"and t8.is_deleted={Deleted.No.value} "
        f"where t1.id='{experiment_id}'"
    )
    experiment_dict = db_helper1.fetchone(sql)
    if not experiment_dict:
        return experiment_dict

    experiment_dict["parameters"] = None
    experiment_dict["execute"] = None
    experiment_dict["last_execute_time"] = None
    if experiment_dict["parameter_path"]:
        parameter_path = experiment_dict["parameter_path"]
        json_str = oss_helper1.get_json_file_data(parameter_path)
        parameters = json.loads(json_str)
        experiment_dict["parameters"] = parameters
        sql = f"select * from ml_model_experiment_execute where id='{execute_id}'"
        execute_dict = db_helper1.fetchone(sql)
        if execute_dict and execute_dict["execute_path"]:
            execute_path = execute_dict["execute_path"]
            json_str = oss_helper1.get_json_file_data(execute_path)
            execute = json.loads(json_str)
            experiment_dict["execute"] = execute
            experiment_dict["last_execute_time"] = execute_dict["create_time"]
    return experiment_dict
