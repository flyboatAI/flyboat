from enum_type.user_role_type import UserRoleType
from helper.sql_helper.init_sql_helper import db_helper1


def get_role(user_id):
    sql = f"select * from blade_user where id='{user_id}'"
    user_dict = db_helper1.fetchone(sql)
    if not user_dict or "administrator" not in user_dict["role_id"]:
        return None
    return UserRoleType.Administrator.value
