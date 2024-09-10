import sys

from enum_type.deleted import Deleted
from error.execute_error import ExecuteError
from helper.generate_helper import uuid_and_now
from helper.sql_helper.init_sql_helper import TransientSqlHelper, db_helper1
from parameter_entity.data_source.data_source import DataSource
from parameter_entity.data_source.data_table import DataTable
from parameter_entity.data_source.test_data_source_link import TestDataSourceLink


def get_transient_sql_helper(datasource_id) -> TransientSqlHelper:
    sql = (
        f"select t2.db_url, t2.db_user, t2.db_password "
        f"from ml_datasource t2 "
        f"where t2.id='{datasource_id}' and "
        f"t2.is_deleted={Deleted.No.value}"
    )

    datasource_dict = db_helper1.fetchone(sql)
    if (
        not datasource_dict
        or "db_url" not in datasource_dict
        or "db_user" not in datasource_dict
        or "db_password" not in datasource_dict
    ):
        raise ExecuteError(sys._getframe().f_code.co_name, "数据源配置不完整") from None
    # 数据库连接字段
    db_url = datasource_dict["db_url"]
    db_user = datasource_dict["db_user"]
    db_password = datasource_dict["db_password"]
    if not db_url or not db_user or not db_password:
        raise ExecuteError(sys._getframe().f_code.co_name, "数据源配置不完整") from None
    return TransientSqlHelper(db_url, db_user, db_password)


def get_tables_by_page(data_table: DataTable):
    """
    根据 datasource_id 获取对应表信息
    :param data_table: 数据表参数
    :return: [
                {
                    "table_name": "表明"
                    "comments": "描述"
                }
              ]
    """
    datasource_id = data_table.datasource_id
    conn = get_transient_sql_helper(datasource_id)
    table_name = data_table.table_name
    current = data_table.current
    size = data_table.size
    return conn.fetch_tables_by_page(table_name, current, size)


def test_check_link(data_source: TestDataSourceLink):
    """
    测试输入的数据连接是否可联通
    :param data_source: 数据源
    :return:
    """
    db_url = data_source.db_url
    db_user = data_source.db_user
    db_password = data_source.db_password
    helper = TransientSqlHelper(db_url, db_user, db_password)
    return helper.test()


def save_data_source(data_source: DataSource, user_id: str):
    db_url = data_source.db_url
    db_user = data_source.db_user
    db_password = data_source.db_password
    datasource_name = data_source.datasource_name
    db_description = data_source.db_description
    db_type = data_source.db_type
    uuid_, now = uuid_and_now()
    sql = (
        f"insert into ml_datasource values('{uuid_}', {Deleted.No.value}, '{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), '{datasource_name}', "
        f"'{db_url}', '{db_user}', '{db_password}', "
        f"'{db_description}', 1, '{db_type}')"
    )
    return db_helper1.execute(sql)


def update_data_source(data_source: DataSource, user_id: str):
    db_url = data_source.db_url
    db_user = data_source.db_user
    db_password = data_source.db_password
    datasource_name = data_source.datasource_name
    db_description = data_source.db_description
    db_type = data_source.db_type
    datasource_id = data_source.datasource_id
    sql = (
        f"update ml_datasource set data_source_name='{datasource_name}', "
        f"db_url='{db_url}', db_user='{db_user}', db_password='{db_password}', "
        f"db_description='{db_description}', db_type='{db_type}' "
        f"where id='{datasource_id}' "
        f"and create_user='{user_id}'"
    )
    return db_helper1.execute(sql)
