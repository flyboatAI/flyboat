import importlib
from typing import Type

from config.setting import DataBase
from entity.database.database_field_standard import DatabaseFieldStandard
from entity.page.page import Page
from enum_type.database_type import DataBaseType
from enum_type.db_pool_type import DataBasePoolType
from error.no_such_db_type_error import NoSuchDbTypeError
from helper.response_result_helper import make_json
from helper.sql_helper.abstract_sql_helper import AbstractSqlHelper


class SqlHelper(object):
    def __init__(self, cls: Type[AbstractSqlHelper], pool_type: DataBasePoolType):
        self.cls = cls
        self.pool = cls.get_pool(pool_type)

    def fetchone(self, sql, params=None):
        with self.cls(self.pool) as helper:
            return helper.fetchone(sql, params)

    def fetchall(self, sql, params=None):
        with self.cls(self.pool) as helper:
            return helper.fetchall(sql, params)

    def fetchpage(self, sql, current, size, params=None) -> Page:
        with self.cls(self.pool) as helper:
            return helper.fetchpage(sql, current, size, params)

    def fetch_fields(self, table_name):
        with self.cls(self.pool) as helper:
            data = helper.fetch_fields(table_name)
            return [make_json(field) for field in data]

    def fetch_fields_format(self, table_name):
        with self.cls(self.pool) as helper:
            data = helper.fetch_fields_format(table_name)
            return [make_json(field) for field in data]

    def create_dynamic_table(self, table_name: str, fields: list[DatabaseFieldStandard]):
        with self.cls(self.pool) as helper:
            return helper.create_dynamic_table(table_name, fields)

    def insert_field_to_dynamic_table(self, table_name: str, field: DatabaseFieldStandard):
        with self.cls(self.pool) as helper:
            return helper.insert_field_to_dynamic_table(table_name, field)

    def batch_insert_to_dynamic_table(
        self,
        table_name: str,
        values: list[dict],
        standard_fields: list[DatabaseFieldStandard],
    ):
        with self.cls(self.pool) as helper:
            return helper.batch_insert_to_dynamic_table(table_name, values, standard_fields)

    def update_dynamic_table(self, table_name: str, value: dict, rowid: str):
        with self.cls(self.pool) as helper:
            return helper.update_dynamic_table(table_name, value, rowid)

    def execute(self, sql, params=None):
        with self.cls(self.pool) as helper:
            return helper.execute(sql, params)

    def execute_arr(self, sql_arr, params_dict=None):
        with self.cls(self.pool) as helper:
            return helper.execute_arr(sql_arr, params_dict)

    def get_sql_helper(self) -> AbstractSqlHelper:
        return self.cls(self.pool)

    @staticmethod
    def open(helper: AbstractSqlHelper):
        helper.open()

    @staticmethod
    def close(helper: AbstractSqlHelper):
        helper.close()

    @staticmethod
    def commit(helper: AbstractSqlHelper):
        helper.commit()

    @staticmethod
    def rollback(helper: AbstractSqlHelper):
        helper.rollback()

    @staticmethod
    def fetchone_without_auto_transaction(helper: AbstractSqlHelper, sql, params=None):
        return helper.fetchone_without_auto_transaction(sql, params)

    @staticmethod
    def fetchall_without_auto_transaction(helper: AbstractSqlHelper, sql, params=None):
        return helper.fetchall_without_auto_transaction(sql, params)

    @staticmethod
    def execute_without_auto_transaction(helper: AbstractSqlHelper, sql, params=None):
        return helper.execute_without_auto_transaction(sql, params)

    @staticmethod
    def execute_arr_without_auto_transaction(helper: AbstractSqlHelper, sql_arr, params_dict=None):
        return helper.execute_arr_without_auto_transaction(sql_arr, params_dict)


class TransientSqlHelper(object):
    def __init__(self, connection_string, user, password):
        self.connection_string = connection_string
        self.user = user
        self.password = password
        self.cls = DataBaseType.get_transient_class_by_connection_str(connection_string)

    def test(self):
        # noinspection PyBroadException
        try:
            with self.cls(self.connection_string, self.user, self.password):
                return True
        except Exception:
            return False

    def fetchone(self, sql, params=None):
        with self.cls(self.connection_string, self.user, self.password) as helper:
            return helper.fetchone(sql, params)

    def fetchall(self, sql, params=None):
        with self.cls(self.connection_string, self.user, self.password) as helper:
            return helper.fetchall(sql, params)

    def fetchpage(self, sql, current, size, params=None) -> Page:
        with self.cls(self.connection_string, self.user, self.password) as helper:
            return helper.fetchpage(sql, current, size, params)

    def execute(self, sql, params=None):
        with self.cls(self.connection_string, self.user, self.password) as helper:
            return helper.execute(sql, params)

    def execute_arr(self, sql_arr, params_dict=None):
        with self.cls(self.connection_string, self.user, self.password) as helper:
            return helper.execute_arr(sql_arr, params_dict)

    def fetch_tables(self):
        with self.cls(self.connection_string, self.user, self.password) as helper:
            data = helper.fetch_tables()
            return [make_json(table) for table in data]

    def fetch_tables_by_page(self, table_name, current, size):
        with self.cls(self.connection_string, self.user, self.password) as helper:
            page = helper.fetch_tables_by_page(table_name, current, size)
            page.data = [make_json(table) for table in page.data]
            return page

    def fetch_tables_format(self, table_name):
        with self.cls(self.connection_string, self.user, self.password) as helper:
            data = helper.fetch_tables_format(table_name)
            return [make_json(field) for field in data]

    def fetch_fields(self, table_name):
        with self.cls(self.connection_string, self.user, self.password) as helper:
            data = helper.fetch_fields(table_name)
            return [make_json(field) for field in data]

    def fetch_fields_format(self, table_name):
        with self.cls(self.connection_string, self.user, self.password) as helper:
            data = helper.fetch_fields_format(table_name)
            return [make_json(field) for field in data]

    def fetch_fields_standard(self, table_name) -> list[DatabaseFieldStandard]:
        with self.cls(self.connection_string, self.user, self.password) as helper:
            return helper.fetch_fields_standard(table_name)

    def insert_by_fields(self, table_name, field_arr: list, value_dict: dict):
        with self.cls(self.connection_string, self.user, self.password) as helper:
            return helper.insert_by_fields(table_name, field_arr, value_dict)


def create_db_helper():
    if DataBase in DataBaseType._value2member_map_:
        module = importlib.import_module(f"helper.sql_helper.{DataBase}_sql_helper")
        class_name = f"{DataBase.capitalize()}SqlHelper"
        sql_helper = getattr(module, class_name)
        return sql_helper
    raise NoSuchDbTypeError(error_message="不支持的数据库类型") from None


db_helper1 = SqlHelper(create_db_helper(), DataBasePoolType.Business)
db_helper2 = SqlHelper(create_db_helper(), DataBasePoolType.Dynamic)
