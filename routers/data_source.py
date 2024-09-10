from typing import Annotated

from fastapi import APIRouter, Header
from fastapi.responses import JSONResponse

from enum_type.result_code import ResultCode
from error.empty_parameter_value_error import EmptyParameterValueError
from error.general_error import GeneralError
from helper.http_parameter_helper import get_user_id_from_blade_auth
from helper.response_result_helper import make_json, response_result
from helper.warning_helper import UNUSED
from helper.wrapper_helper import valid_exist_user_id_from_blade_auth
from parameter_entity.data_source.data_source import DataSource
from parameter_entity.data_source.data_table import DataTable
from parameter_entity.data_source.test_data_source_link import TestDataSourceLink
from service.data_source.data_source_service import (
    get_tables_by_page,
    save_data_source,
    test_check_link,
    update_data_source,
)

# 初始化数据源 Controller
router = APIRouter()


@router.post("/get_tables_by_page")
@valid_exist_user_id_from_blade_auth
async def get_data_tables(blade_auth: Annotated[str | None, Header()], data_table: DataTable):
    UNUSED(blade_auth)
    page = get_tables_by_page(data_table)
    data = make_json(page)
    return JSONResponse(make_json(response_result(data=data)))


@router.post("/check_link")
@valid_exist_user_id_from_blade_auth
async def check_link(blade_auth: Annotated[str | None, Header()], data_source: TestDataSourceLink):
    """
    测试数据库连接情况
    :return: response_result
    """
    UNUSED(blade_auth)
    db_url = data_source.db_url
    if not db_url:
        raise EmptyParameterValueError(["db_url"]) from None
    is_link = test_check_link(data_source)
    return JSONResponse(make_json(response_result(data={"is_link": is_link})))


@router.post("/save_or_update")
@valid_exist_user_id_from_blade_auth
async def save_or_update(blade_auth: Annotated[str | None, Header()], data_source: DataSource):
    """
    保存数据源
    :return: response_result
    """
    db_url = data_source.db_url
    datasource_name = data_source.datasource_name
    db_type = data_source.db_type
    if not db_url or not datasource_name or not db_type:
        raise EmptyParameterValueError(["db_url", "datasource_name", "db_type"]) from None
    user_id = get_user_id_from_blade_auth(blade_auth)
    is_link = test_check_link(data_source)
    if not is_link:
        raise GeneralError("数据库链接测试未通过, 请检查") from None
    if data_source.datasource_id:
        result = update_data_source(data_source, user_id)
    else:
        result = save_data_source(data_source, user_id)
    if result != ResultCode.Success.value:
        raise GeneralError("数据源配置保存失败") from None
    return JSONResponse(make_json(response_result()))
