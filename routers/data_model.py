import sys
from typing import Annotated

from fastapi import APIRouter, Header
from fastapi.responses import JSONResponse

from config import setting
from enum_type.result_code import ResultCode
from error.empty_parameter_value_error import EmptyParameterValueError
from error.execute_error import ExecuteError
from helper.generate_helper import generate_uuid
from helper.http_parameter_helper import get_user_id_from_blade_auth
from helper.response_result_helper import make_json, response_result
from helper.sql_helper.init_sql_helper import db_helper2
from helper.warning_helper import UNUSED
from helper.wrapper_helper import valid_exist_user_id_from_blade_auth
from parameter_entity.data_model.append_data_model import AppendDataModel
from parameter_entity.data_model.data_tag import DataTag
from parameter_entity.data_model.file_data_model import FileDataModel
from parameter_entity.data_model.formula_data_model import FormulaDataModel
from parameter_entity.data_model.general_data_model import GeneralDataModel
from parameter_entity.data_model.manual_data_model import ManualDataModel
from parameter_entity.data_model.relation_data_model import RelationDataModel
from parameter_entity.data_model.update_data_model import UpdateDataModel
from service.data_model.data_model_service import (
    append_data_model_sample_data,
    batch_insert_to_dynamic_table_by_datasource_id,
    create_dynamic_table_and_insert_data,
    delete_data_model_sample_data,
    fetch_fields_standard_from_datasource_id,
    get_sample_data_fields_from_dt_table_by_id,
    get_sample_data_from_dt_table_by_id,
    insert_dynamic_table_id,
    insert_field_to_dynamic_table_for_data_tag,
    insert_file_data_model,
    insert_formula_data_model,
    insert_manual_data_model,
    insert_table_data_model,
    update_data_model_sample_data,
)

# 初始化数据源 Controller
router = APIRouter()


@router.post("/data_model_samples")
@valid_exist_user_id_from_blade_auth
def data_model_samples(blade_auth: Annotated[str | None, Header()], data_model: GeneralDataModel):
    """
    测试数据库连接情况
    :return: response_result
    """
    UNUSED(blade_auth)
    table_id = data_model.table_id
    if not table_id:
        raise EmptyParameterValueError(["table_id"]) from None
    page = get_sample_data_from_dt_table_by_id(table_id, data_model.search_key, data_model.current, data_model.size)
    return JSONResponse(make_json(response_result(data=make_json(page))))


@router.post("/data_model_fields")
@valid_exist_user_id_from_blade_auth
def data_model_fields(blade_auth: Annotated[str | None, Header()], data_model: GeneralDataModel):
    """
    测试数据库连接情况
    :return: response_result
    """
    UNUSED(blade_auth)
    table_id = data_model.table_id
    if not table_id:
        raise EmptyParameterValueError(["table_id"]) from None
    fields = get_sample_data_fields_from_dt_table_by_id(table_id)
    return JSONResponse(make_json(response_result(data=fields)))


@router.post("/save_relation_data_model")
@valid_exist_user_id_from_blade_auth
async def save_relation_data_model(blade_auth: Annotated[str | None, Header()], data_model: RelationDataModel):
    user_id = get_user_id_from_blade_auth(blade_auth)
    datasource_id = data_model.datasource_id
    table_name = data_model.table_name
    data_model_name = data_model.data_model_name
    data_model_description = data_model.data_model_description
    standard_fields = fetch_fields_standard_from_datasource_id(datasource_id, table_name)
    dynamic_table_name = f"dt_{generate_uuid()}"
    dynamic_table_name = dynamic_table_name.upper()
    # 动态建表
    result = db_helper2.create_dynamic_table(dynamic_table_name, standard_fields)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "动态创建数据表失败") from None
    table_id = insert_dynamic_table_id(user_id, dynamic_table_name)
    if not table_id:
        raise ExecuteError(sys._getframe().f_code.co_name, "动态创建数据表失败") from None

    # 批量插入数据
    result = batch_insert_to_dynamic_table_by_datasource_id(
        dynamic_table_name, datasource_id, table_name, standard_fields
    )
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "动态插入数据表失败") from None
    result = insert_table_data_model(
        user_id,
        table_id,
        dynamic_table_name,
        data_model_name,
        datasource_id,
        data_model_description,
    )
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "动态创建数据表失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/save_file_data_model")
@valid_exist_user_id_from_blade_auth
async def save_file_data_model(blade_auth: Annotated[str | None, Header()], data_model: FileDataModel):
    user_id = get_user_id_from_blade_auth(blade_auth)
    data_model_name = data_model.data_model_name
    data_model_description = data_model.data_model_description
    table_id = create_dynamic_table_and_insert_data(user_id, data_model)

    file_type = data_model.file_type
    result = insert_file_data_model(user_id, table_id, data_model_name, file_type, data_model_description)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "动态创建数据表失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/save_manual_data_model")
@valid_exist_user_id_from_blade_auth
async def save_manual_data_model(blade_auth: Annotated[str | None, Header()], data_model: ManualDataModel):
    user_id = get_user_id_from_blade_auth(blade_auth)
    data_model_name = data_model.data_model_name
    data_model_description = data_model.data_model_description
    table_id = create_dynamic_table_and_insert_data(user_id, data_model)
    result = insert_manual_data_model(user_id, table_id, data_model_name, data_model_description)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "动态创建数据表失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/save_formula_data_model")
@valid_exist_user_id_from_blade_auth
async def save_formula_data_model(blade_auth: Annotated[str | None, Header()], data_model: FormulaDataModel):
    user_id = get_user_id_from_blade_auth(blade_auth)
    data_model_name = data_model.data_model_name
    data_model_description = data_model.data_model_description
    table_id = create_dynamic_table_and_insert_data(user_id, data_model)

    generate_count = data_model.generate_count
    formula = data_model.formula
    result = insert_formula_data_model(
        user_id,
        table_id,
        data_model_name,
        formula,
        generate_count,
        data_model_description,
    )
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "动态创建数据表失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/update_data_model")
@valid_exist_user_id_from_blade_auth
async def update_data_model(blade_auth: Annotated[str | None, Header()], update_parameter: UpdateDataModel):
    UNUSED(blade_auth)
    data = update_parameter.data
    table_id = update_parameter.table_id
    if not table_id:
        raise ExecuteError(sys._getframe().f_code.co_name, "更新数据失败, 未传递 table_id 字段") from None
    rowid = data.get(f"row_id_{setting.ZMS_MAGIC}")
    if not rowid:
        raise ExecuteError(
            sys._getframe().f_code.co_name,
            f"更新数据失败, 未传递 row_id_{setting.ZMS_MAGIC} 字段",
        ) from None
    del data[f"row_id_{setting.ZMS_MAGIC}"]
    result = update_data_model_sample_data(table_id, rowid, data)

    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "更新数据失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/delete_data_model")
@valid_exist_user_id_from_blade_auth
async def delete_data_model(blade_auth: Annotated[str | None, Header()], update_parameter: UpdateDataModel):
    UNUSED(blade_auth)
    rowid_list = update_parameter.rowid_list
    table_id = update_parameter.table_id
    if not table_id:
        raise ExecuteError(sys._getframe().f_code.co_name, "删除数据失败, 未传递 table_id 字段") from None
    if not rowid_list:
        raise ExecuteError(sys._getframe().f_code.co_name, "删除数据失败, 未传递 rowid_list 字段") from None

    result = delete_data_model_sample_data(table_id, rowid_list)

    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "删除数据失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/batch_append_data_model")
@valid_exist_user_id_from_blade_auth
async def batch_append_data_model(blade_auth: Annotated[str | None, Header()], append_parameter: AppendDataModel):
    UNUSED(blade_auth)
    data = append_parameter.data
    fields = append_parameter.fields
    table_id = append_parameter.table_id
    if not table_id:
        raise ExecuteError(sys._getframe().f_code.co_name, "追加数据失败, 未传递 table_id 字段") from None
    if not data or not fields:
        raise ExecuteError(sys._getframe().f_code.co_name, "追加数据失败, 未传递 data 或 fields 字段") from None

    result = append_data_model_sample_data(table_id, data, fields)

    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "追加数据失败") from None
    return JSONResponse(make_json(response_result()))


@router.post("/add_data_tag_field")
@valid_exist_user_id_from_blade_auth
def add_data_tag_field(blade_auth: Annotated[str | None, Header()], data_tag: DataTag):
    UNUSED(blade_auth)
    table_id = data_tag.table_id
    field = data_tag.field
    result = insert_field_to_dynamic_table_for_data_tag(table_id, field)
    if result != ResultCode.Success.value:
        raise ExecuteError(sys._getframe().f_code.co_name, "添加数据标签列失败") from None
    return JSONResponse(make_json(response_result()))
