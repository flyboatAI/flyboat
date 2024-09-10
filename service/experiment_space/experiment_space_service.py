from enum_type.deleted import Deleted
from enum_type.result_code import ResultCode
from helper.generate_helper import uuid_and_now
from helper.sql_helper.init_sql_helper import db_helper1


def create_experiment_space(
    user_id: str, space_name: str, space_description: str | None = None, placeholder: str = "1"
):
    experiment_name = space_name
    if not experiment_name:
        return ResultCode.Error.value
    uuid_, now = uuid_and_now()
    space_description_sql = f"'{space_description}'" if space_description is not None else "null"
    result = db_helper1.execute(
        f"insert into ml_experiment_space values('{uuid_}', {Deleted.No.value}, '{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), '{experiment_name}', "
        f"{space_description_sql}, '{placeholder}')"
    )
    if result == ResultCode.Success.value:
        return uuid_
    return None
