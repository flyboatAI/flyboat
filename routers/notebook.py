import sys
from typing import Annotated

from fastapi import APIRouter, Header
from fastapi.responses import JSONResponse

from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from helper.http_parameter_helper import get_user_id_from_blade_auth
from helper.response_result_helper import make_json, response_result
from helper.warning_helper import UNUSED
from helper.wrapper_helper import valid_exist_user_id_from_blade_auth
from parameter_entity.notebook.notebook_experiment import NotebookExperiment
from parameter_entity.notebook.notebook_experiment_detail import NotebookExperimentDetail
from service.notebook.notebook_experiment_service import (
    create_empty_notebook,
    create_new_notebook_experiment,
    delete_exist_notebook_experiment,
    edit_exist_notebook_experiment,
    get_notebook_experiment_list,
    load_notebook_to_local,
    put_notebook_to_s3,
)

# Jupyter NoteBook 管理 Controller
router = APIRouter()


@router.post("/create_experiment")
@valid_exist_user_id_from_blade_auth
async def create_experiment(blade_auth: Annotated[str | None, Header()], notebook_experiment: NotebookExperiment):
    """
    创建 notebook 实验
    :return: response_result
    """
    user_id = get_user_id_from_blade_auth(blade_auth)
    result = create_new_notebook_experiment(user_id, notebook_experiment)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "保存实验失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/edit_experiment")
@valid_exist_user_id_from_blade_auth
async def edit_experiment(blade_auth: Annotated[str | None, Header()], notebook_experiment: NotebookExperiment):
    """
    编辑 notebook 实验基础信息
    :return: response_result
    """
    UNUSED(blade_auth)
    result = edit_exist_notebook_experiment(notebook_experiment)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "编辑实验失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/delete_experiment")
@valid_exist_user_id_from_blade_auth
async def delete_experiment(blade_auth: Annotated[str | None, Header()], notebook_experiment: NotebookExperiment):
    """
    删除 notebook 实验基础信息
    :return: response_result
    """
    UNUSED(blade_auth)
    result = delete_exist_notebook_experiment(notebook_experiment)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "删除实验失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/experiment_list")
@valid_exist_user_id_from_blade_auth
async def experiment_list(blade_auth: Annotated[str | None, Header()], notebook_experiment: NotebookExperiment):
    """
    notebook 实验列表
    :return: response_result
    """
    user_id = get_user_id_from_blade_auth(blade_auth)
    return JSONResponse(
        make_json(response_result(data=make_json(get_notebook_experiment_list(user_id, notebook_experiment))))
    )


@router.post("/load_notebook")
@valid_exist_user_id_from_blade_auth
async def load_notebook(
    blade_auth: Annotated[str | None, Header()],
    notebook_detail: NotebookExperimentDetail,
):
    UNUSED(blade_auth)
    notebook_name = notebook_detail.notebook_name
    notebook_id = notebook_detail.notebook_id
    if not notebook_name:
        notebook_name = create_empty_notebook(notebook_id)
    else:
        load_notebook_to_local(notebook_name)
    return JSONResponse(make_json(response_result(data=notebook_name)))


@router.post("/save_notebook")
async def save_notebook(
    notebook_detail: NotebookExperimentDetail,
):
    notebook_name = notebook_detail.notebook_name
    put_notebook_to_s3(notebook_name)
    return JSONResponse(make_json(response_result()))
