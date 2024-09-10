from enum_type.secret_type import SecretType
from helper.generate_helper import uuid_and_now
from helper.sql_helper.init_sql_helper import db_helper1


def register_app(user_id, app_name, app_key, app_secret):
    secret_id, now = uuid_and_now()

    insert_secret_sql = (
        f"insert into ml_secret values('{secret_id}', "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'外部密钥 {app_name}', "
        f"'{app_key}', '{app_secret}', {SecretType.App.value}, null, null)"
    )

    app_register_sql = (
        f"insert into ml_app_register values ("
        f"CREATEGUID(), "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{app_name}', '{secret_id}' , null)"
    )
    return db_helper1.execute_arr([insert_secret_sql, app_register_sql])
