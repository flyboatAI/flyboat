import json
import multiprocessing
import time
import traceback
from contextlib import asynccontextmanager

import aiohttp
import uvicorn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from loguru import logger
from psutil import NoSuchProcess

from config import setting
from core.engine_execute_pool import ExecutePool
from error.convert_error import ConvertError
from error.data_process_error import DataProcessError
from error.delete_error import DeleteError
from error.element_configuration_config_error import ElementConfigurationConfigError
from error.element_configuration_query_error import ElementConfigurationQueryError
from error.empty_parameter_value_error import EmptyParameterValueError
from error.execute_error import ExecuteError
from error.general_error import GeneralError
from error.init_error import InitError
from error.insight_error import InsightError
from error.license_error import LicenseError
from error.no_such_user_error import NoSuchUserError
from error.query_error import QueryError
from error.store_error import StoreError
from error.valid_token_error import ValidTokenError
from helper.response_result_helper import make_json, response_error_result
from helper.warning_helper import UNUSED
from routers import (
    configuration,
    data_model,
    data_source,
    deduction,
    element,
    init,
    license_manager,
    login,
    model_experiment,
    notebook,
    parameterization,
    pipelining,
    publish,
    sample_data,
    user,
)


@asynccontextmanager
async def lifespan(application: FastAPI):
    UNUSED(application)
    ExecutePool.set_pool(multiprocessing.Pool(processes=4))
    ExecutePool.set_manager_dict(multiprocessing.Manager().dict())
    logger.add(
        "log/dora_{time:YYYY-MM-DD}.log",
        format="<green>{time}</green> <level>{message}</level>",
        enqueue=True,
        backtrace=True,
        diagnose=True,
        rotation="5 MB",
        retention=9,
    )
    yield


app = FastAPI(lifespan=lifespan)

# 注册蓝图, 并指定其对应的前缀
app.include_router(configuration.router, prefix="/configuration", tags=["configuration"])
app.include_router(element.router, prefix="/element", tags=["element"])
app.include_router(init.router, prefix="/init", tags=["init"])
app.include_router(pipelining.router, prefix="/pipelining", tags=["pipelining"])
app.include_router(parameterization.router, prefix="/parameterization", tags=["parameterization"])
app.include_router(deduction.router, prefix="/deduction", tags=["deduction"])
app.include_router(data_source.router, prefix="/data_source", tags=["data_source"])
app.include_router(data_model.router, prefix="/data_model", tags=["data_model"])
app.include_router(license_manager.router, prefix="/license", tags=["license"])
app.include_router(publish.router, prefix="/publish", tags=["publish"])
app.include_router(user.router, prefix="/user", tags=["user"])
app.include_router(model_experiment.router, prefix="/model_experiment", tags=["model_experiment"])
app.include_router(sample_data.router, prefix="/sample_data", tags=["sample_data"])
app.include_router(login.router, prefix="/login", tags=["login"])
app.include_router(notebook.router, prefix="/notebook", tags=["notebook"])


@app.middleware("http")
async def log_request(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = (time.time() - start_time) * 1000  # ms
    if process_time > 200:
        # 记录处理时间大于 200 ms 的慢操作
        logger.info(f"Request: {request.method} " f"{request.url} " f"{response.status_code} " f"{process_time}ms")
    return response


async def logger_exception(msg):
    logger.error(f"traceback: {traceback.format_exc()}, " f"{msg}")
    async with aiohttp.ClientSession() as session:
        body = {
            "msgtype": "markdown",
            "markdown": {
                "title": "您的应用监控到异常",
                "text": f"###traceback:### {traceback.format_exc()}\n " f"{msg}",
            },
        }
        headers = {"Content-Type": "application/json; charset=utf-8"}
        # noinspection PyBroadException
        try:
            async with session.post(
                url=setting.ROBOT_URL + setting.ROBOT_ACCESS_TOKEN,
                data=json.dumps(body),
                headers=headers,
            ) as resp:
                await resp.text()
        except Exception as e:
            logger.error(f"{e}")
            pass


@app.exception_handler(GeneralError)
async def general_error_handler(request: Request, exc: GeneralError):
    UNUSED(request, exc)
    await logger_exception(exc.error_message)
    return JSONResponse(make_json(response_error_result(message=exc.error_message)))


@app.exception_handler(ConnectionRefusedError)
async def connection_refused_handler(request: Request, exc: ConnectionRefusedError):
    UNUSED(request, exc)
    msg = "websocket 连接被拒绝，请检查 websocket 服务"
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=msg)))


@app.exception_handler(DataProcessError)
async def process_error_handler(request: Request, exc: DataProcessError):
    UNUSED(request)
    msg = f"{exc.error_message}"
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=f"数据处理发生错误, {exc.error_message}")))


@app.exception_handler(ExecuteError)
async def execute_error_handler(request: Request, exc: ExecuteError):
    UNUSED(request)
    msg = f"function name: {exc.func_name}, 执行失败, {exc.error_message}"
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=f"{exc.error_message}")))


@app.exception_handler(QueryError)
async def query_error_handler(request: Request, exc: QueryError):
    UNUSED(request)
    msg = f"function name: {exc.func_name}, user: {exc.user_id} 查询失败, {exc.error_message}"
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=f"查询失败, {exc.error_message}")))


@app.exception_handler(StoreError)
async def store_error_handler(request: Request, exc: StoreError):
    UNUSED(request)
    msg = f"function name: {exc.func_name}, user: {exc.user_id} 存储失败, {exc.error_message}"
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=f"存储失败, {exc.error_message}")))


@app.exception_handler(EmptyParameterValueError)
async def empty_parameter_value_error_handler(request: Request, exc: EmptyParameterValueError):
    UNUSED(request)
    msg = exc.error_message
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=msg)))


@app.exception_handler(NoSuchUserError)
async def no_such_user_error_handler(request: Request, exc: NoSuchUserError):
    UNUSED(request)
    msg = exc.error_message
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=msg)))


@app.exception_handler(NoSuchProcess)
async def no_such_process_error_handler(request: Request, exc: NoSuchProcess):
    UNUSED(request)
    msg = f"未找到 pid={exc.pid} 关联的进程"
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=msg)))


@app.exception_handler(KeyError)
async def no_key_error_handler(request: Request, exc: KeyError):
    UNUSED(request)
    msg = f"{exc.args[0]} 参数未传递, 请检查"
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=msg)))


@app.exception_handler(ElementConfigurationQueryError)
async def element_config_query_error_handler(request: Request, exc: ElementConfigurationQueryError):
    UNUSED(request)
    msg = f"算子配置查询失败, {exc.error_message}"
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=msg)))


@app.exception_handler(InitError)
async def element_init_error_handler(request: Request, exc: InitError):
    UNUSED(request)
    msg = f"算子初始化失败, {exc.error_message}"
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=msg)))


@app.exception_handler(ElementConfigurationConfigError)
async def element_config_error_handler(request: Request, exc: ElementConfigurationConfigError):
    UNUSED(request)
    msg = f"算子配置失败, {exc.error_message}"
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=msg)))


@app.exception_handler(DeleteError)
async def element_delete_error_handler(request: Request, exc: DeleteError):
    UNUSED(request)
    msg = f"算子删除失败, {exc.error_message}"
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=msg)))


@app.exception_handler(ConvertError)
async def convert_error_handler(request: Request, exc: ConvertError):
    UNUSED(request)
    msg = f"转换失败, {exc.error_message}"
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=msg)))


@app.exception_handler(InsightError)
async def insight_error_handler(request: Request, exc: InsightError):
    UNUSED(request)
    msg = f"洞察数据获取失败, {exc.error_message}"
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=msg)))


@app.exception_handler(ValidTokenError)
async def valid_api_token_error_handler(request: Request, exc: ValidTokenError):
    UNUSED(request)
    msg = f"接口权限校验失败, {exc.error_message}"
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=msg)))


@app.exception_handler(LicenseError)
async def valid_license_error_handler(request: Request, exc: LicenseError):
    UNUSED(request)
    msg = exc.error_message
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=msg)))


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    UNUSED(request)
    origin_err = JSONResponse(make_json(response_error_result(message=exc.errors())))
    await logger_exception(origin_err)
    if not exc.args or not exc.args[0]:
        return origin_err
    errs = exc.args[0]
    if not isinstance(errs, list):
        return origin_err
    missing_parameters = []
    bad_type_parameters = []
    for err in errs:
        err_type: str = err.get("type")
        loc: tuple = err.get("loc")
        input_parameter = err.get("input")
        if not input_parameter:
            if err_type == "missing" and len(loc) > 1 and loc[0] == "header" and loc[1] == "blade-auth":
                # 用户校验
                msg = "未传递用户标识, 鉴权失败"
                await logger_exception(msg)
                return JSONResponse(make_json(response_error_result(message=msg)))
            msg = "必传参数未传递, 请检查"
            await logger_exception(msg)
            return JSONResponse(make_json(response_error_result(message=msg)))
        if err_type == "missing" and len(loc) > 1:
            # 用户校验
            missing_parameters.append(loc[1])
        if err_type.endswith("_type") and len(loc) > 1:
            bad_type_parameters.append(loc[1])

    msg1 = f"{'、'.join(missing_parameters)} 参数未传递, " if missing_parameters else ""
    msg2 = f"{'、'.join(bad_type_parameters)} 参数类型错误, " if bad_type_parameters else ""

    msg = f"{msg1}{msg2}"
    if not msg:
        msg = "参数传递错误, 请检查"
    else:
        msg = f"{msg1}{msg2}请检查"
    await logger_exception(msg)
    return JSONResponse(make_json(response_error_result(message=msg)))


@app.get("/")
def hello_world(request: Request):
    UNUSED(request)
    return f"Hello Baby! {setting.ZMS_MAGIC}"


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=9090, reload=False)
