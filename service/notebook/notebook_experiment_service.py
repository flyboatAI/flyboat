import json
from pathlib import Path

from config import setting
from enum_type.deleted import Deleted
from enum_type.result_code import ResultCode
from error.general_error import GeneralError
from helper.folder_helper import notebook_folder
from helper.generate_helper import generate_uuid, uuid_and_now
from helper.oss_helper.oss_helper import oss_helper1
from helper.sql_helper.init_sql_helper import db_helper1
from parameter_entity.notebook.notebook_experiment import NotebookExperiment


def create_new_notebook_experiment(user_id: str, notebook_experiment: NotebookExperiment):
    experiment_name = notebook_experiment.experiment_name

    if not experiment_name:
        return ResultCode.Error.value
    uuid_, now = uuid_and_now()
    # relative_folder = notebook_folder()
    # key = relative_folder + str(uuid_) + ".ipynb"
    # oss_helper1.create_notebook(key, "")
    return db_helper1.execute(
        f"insert into ml_notebook_experiment values('{uuid_}', '{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), null, '{experiment_name}', {Deleted.No.value})"
    )


def edit_exist_notebook_experiment(notebook_experiment: NotebookExperiment):
    experiment_name = notebook_experiment.experiment_name
    experiment_id = notebook_experiment.experiment_id
    if not experiment_name or not experiment_id:
        return ResultCode.Error.value
    return db_helper1.execute(
        f"update ml_notebook_experiment set experiment_name='{experiment_name}'" f"where id='{experiment_id}'"
    )


def delete_exist_notebook_experiment(notebook_experiment: NotebookExperiment):
    experiment_id = notebook_experiment.experiment_id
    if not experiment_id:
        return ResultCode.Error.value
    return db_helper1.execute(
        f"update ml_notebook_experiment set is_deleted={Deleted.Yes.value} " f"where id='{experiment_id}'"
    )


def get_notebook_experiment_list(user_id: str, notebook_experiment: NotebookExperiment):
    separator = "/"  # os.sep
    experiment_name = notebook_experiment.experiment_name
    filter_sql = f" and t1.experiment_name like '%{experiment_name}%' " if experiment_name else ""
    sql = (
        f"select t1.*, "
        f"substr(t1.notebook_path, instr(t1.notebook_path, '{separator}', -1) + 1) as notebook_name, "
        f"t6.name as user_name "
        f"from ml_notebook_experiment t1 "
        f"left join blade_user t6 "
        f"on t6.id=t1.create_user "
        f"where t1.create_user='{user_id}' "
        f"and t1.is_deleted={Deleted.No.value} "
        f"{filter_sql} "
        f"order by t1.create_time desc"
    )
    return db_helper1.fetchpage(sql, notebook_experiment.current, notebook_experiment.size)


def create_empty_notebook(experiment_id: str):
    uuid_ = generate_uuid()
    notebook_name = uuid_ + ".ipynb"
    local_file = setting.NOTEBOOK_LOCAL_FOLDER + notebook_name
    # 本地新建空文件
    fp = Path(local_file)
    content = {
        "cells": [
            {
                "cell_type": "code",
                "execution_count": None,
                "id": f"{uuid_}",
                "metadata": {},
                "outputs": [],
                "source": [],
            }
        ],
        "metadata": {"kernelspec": {"display_name": "", "name": ""}, "language_info": {"name": ""}},
        "nbformat": 4,
        "nbformat_minor": 5,
    }

    with fp.open("w") as file:
        file.write(json.dumps(content))
    relative_folder = notebook_folder()
    key = relative_folder + notebook_name
    oss_helper1.create_notebook(key, json.dumps(content))
    sql = f"update ml_notebook_experiment set notebook_path='{key}' where id='{experiment_id}'"
    result = db_helper1.execute(sql)
    if result != ResultCode.Success.value:
        raise GeneralError("加载 notebook 数据失败") from None
    return notebook_name


# 加载到本地
def load_notebook_to_local(notebook_name: str):
    local_file = setting.NOTEBOOK_LOCAL_FOLDER + notebook_name
    relative_folder = notebook_folder()
    key = relative_folder + notebook_name
    oss_helper1.load_notebook(key, local_file)


# 覆盖
def put_notebook_to_s3(notebook_name: str):
    local_file = setting.NOTEBOOK_LOCAL_FOLDER + notebook_name
    # 读取本地文件内容
    with open(local_file, "r", encoding="utf-8") as file:
        content = file.read()
    relative_folder = notebook_folder()
    key = relative_folder + notebook_name
    oss_helper1.create_notebook(key, content)
