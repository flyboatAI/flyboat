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
from parameter_entity.model_experiment.execute_model_experiment import ExecuteModelExperiment
from parameter_entity.model_experiment.model_experiment import ModelExperiment
from parameter_entity.model_experiment.model_experiment_history import ModelExperimentHistory
from parameter_entity.model_experiment.model_experiment_parameters import (
    ModelExperimentParameters,
)
from service.model_experiment.model_experiment_service import (
    create_new_model_experiment,
    delete_exist_model_experiment,
    edit_exist_model_experiment,
    edit_model_experiment_parameters,
    execute_history,
    execute_history_detail,
    execute_model,
    get_experiment_list,
    get_model_experiment_parameters_and_execute_result,
    get_publish_tree,
)

# 实验管理 Controller
router = APIRouter()


@router.post("/create_experiment")
@valid_exist_user_id_from_blade_auth
async def create_experiment(blade_auth: Annotated[str | None, Header()], model_experiment: ModelExperiment):
    """
    创建模型实验
    :return: response_result
    """
    user_id = get_user_id_from_blade_auth(blade_auth)
    result = create_new_model_experiment(user_id, model_experiment)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "保存实验失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/edit_experiment")
@valid_exist_user_id_from_blade_auth
async def edit_experiment(blade_auth: Annotated[str | None, Header()], model_experiment: ModelExperiment):
    """
    编辑模型实验基础信息
    :return: response_result
    """
    UNUSED(blade_auth)
    result = edit_exist_model_experiment(model_experiment)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "编辑实验失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/delete_experiment")
@valid_exist_user_id_from_blade_auth
async def delete_experiment(blade_auth: Annotated[str | None, Header()], model_experiment: ModelExperiment):
    """
    删除模型实验基础信息
    :return: response_result
    """
    UNUSED(blade_auth)
    result = delete_exist_model_experiment(model_experiment)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "删除实验失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/experiment_list")
@valid_exist_user_id_from_blade_auth
async def experiment_list(blade_auth: Annotated[str | None, Header()], model_experiment: ModelExperiment):
    """
    模型实验列表
    :return: response_result
    """
    user_id = get_user_id_from_blade_auth(blade_auth)
    return JSONResponse(make_json(response_result(data=make_json(get_experiment_list(user_id, model_experiment)))))


@router.post("/publish_tree")
@valid_exist_user_id_from_blade_auth
async def publish_tree(blade_auth: Annotated[str | None, Header()]):
    """
    模型实验列表
    :return: response_result
    """
    user_id = get_user_id_from_blade_auth(blade_auth)
    return JSONResponse(make_json(response_result(data=get_publish_tree(user_id))))


@router.post("/get_experiment_detail")
@valid_exist_user_id_from_blade_auth
async def get_experiment_detail(
    blade_auth: Annotated[str | None, Header()],
    model_experiment_parameters: ModelExperimentParameters,
):
    UNUSED(blade_auth)
    experiment_id = model_experiment_parameters.experiment_id
    return JSONResponse(
        make_json(response_result(data=get_model_experiment_parameters_and_execute_result(experiment_id)))
    )


@router.post("/edit_model_parameters")
@valid_exist_user_id_from_blade_auth
async def edit_model_and_model_parameters(
    blade_auth: Annotated[str | None, Header()],
    model_experiment_parameters: ModelExperimentParameters,
):
    user_id = get_user_id_from_blade_auth(blade_auth)
    result = edit_model_experiment_parameters(user_id, model_experiment_parameters)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "编辑模型参数失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/run_experiment")
@valid_exist_user_id_from_blade_auth
async def run_experiment(blade_auth: Annotated[str | None, Header()], execute_model_experiment: ExecuteModelExperiment):
    user_id = get_user_id_from_blade_auth(blade_auth)
    return await execute_model(user_id, execute_model_experiment)


@router.post("/experiment_history")
@valid_exist_user_id_from_blade_auth
async def experiment_history(blade_auth: Annotated[str | None, Header()], history: ModelExperimentHistory):
    UNUSED(blade_auth)
    return JSONResponse(make_json(response_result(data=execute_history(history))))


@router.post("/experiment_history_detail")
@valid_exist_user_id_from_blade_auth
async def experiment_history_detail(blade_auth: Annotated[str | None, Header()], history: ModelExperimentHistory):
    UNUSED(blade_auth)
    return JSONResponse(make_json(response_result(data=execute_history_detail(history))))
