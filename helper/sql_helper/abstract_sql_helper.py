import abc

from config.dynamic_table_setting import TS_COLUMN_NAME
from entity.database.database_field import DatabaseField, DatabaseFieldFormat
from entity.database.database_field_standard import DatabaseFieldStandard
from entity.database.database_table import DatabaseTable
from entity.page.page import Page
from enum_type.database_field_data_type import DatabaseFieldStandardDataType


class AbstractSqlHelper(abc.ABC):
    def __init__(self, conn_pool):
        self.conn = None
        self.cursor = None
        self.pool = conn_pool

    @staticmethod
    def get_pool(pool_type):
        """
        获取连接池对象
        :param pool_type: 连接池类型
        :return: 连接池对象
        """
        pass

    @abc.abstractmethod
    def open(self):
        """
        打开数据库链接
        :return:
        """
        pass

    @abc.abstractmethod
    def close(self):
        """
        关闭数据库链接，返回到连接池
        :return:
        """
        pass

    @abc.abstractmethod
    def fetchone(self, sql, params=None):
        """
        获取一条记录
        :param sql: sql
        :param params:
        :return: 字典
        """
        pass

    @abc.abstractmethod
    def fetchall(self, sql, params=None):
        """
        获取记录数组
        :param sql: sql
        :param params:
        :return: 字典数组
        """
        pass

    @abc.abstractmethod
    def fetchpage(self, sql, current, size, params=None) -> Page:
        """
        获取记录数组(分页)
        :param sql: sql
        :param current: 当前页码
        :param size: 每页条数
        :param params:
        :return: 字典数组
        """
        pass

    @abc.abstractmethod
    def fetch_fields(self, table_name) -> list[DatabaseField]:
        """
        获取表名为 table_name 的元数据信息
        :param table_name: 表名
        :return: 源数据列表
        """
        pass

    def fetch_fields_format(self, table_name):
        """
        获取表名为 table_name 的格式化元数据信息
        :param table_name: 表名
        :return: 源数据列表
         {
          "name": 字段名
          "nick_name": 字段中文
          "data_type": 字段类型
         }
        """
        data = self.fetch_fields(table_name)
        lower_fields = [self._create_database_field_format(field) for field in data]
        lower_fields = [f for f in lower_fields if f is not None]
        return lower_fields

    def _create_database_field_format(self, field):
        name = (field.column_name or "").lower()
        nick_name = field.column_comment or field.column_name or ""
        data_type = self.data_type_format(field.data_type)
        if name.upper() == TS_COLUMN_NAME:  # 自定义的时间列不返回
            return None
        return DatabaseFieldFormat(name, data_type, nick_name)

    @staticmethod
    def _create_database_field(field):
        column_name = field.get("column_name") or ""
        data_type = field.get("data_type") or ""
        column_comment = field.get("column_comment") or field.get("column_name") or ""
        data_length = field.get("data_length")
        data_precision = field.get("data_precision")
        data_scale = field.get("data_scale")
        data_type_new = field.get("data_type_new")
        is_pk = field.get("is_pk") == 1
        is_not_null = field.get("is_not_null") == 1
        data_default = field.get("data_default")
        if column_name.upper() == TS_COLUMN_NAME:  # 自定义的时间列不返回
            return None
        return DatabaseField(
            column_name,
            data_type,
            column_comment,
            data_length,
            data_precision,
            data_scale,
            data_type_new,
            is_pk,
            is_not_null,
            data_default,
        )

    @abc.abstractmethod
    def create_dynamic_table(self, table_name: str, fields: list[DatabaseFieldStandard]):
        """
        根据 DatabaseField 列表, 动态建表
        :param table_name: 动态表名
        :param fields: DatabaseFieldStandard 列表
        :return: 建表
        """
        pass

    @abc.abstractmethod
    def insert_field_to_dynamic_table(self, table_name: str, field: DatabaseFieldStandard):
        """
        根据 DatabaseField, 向表内动态插入字段
        :param table_name: 动态表名
        :param field: DatabaseFieldStandard
        :return: 插入结果
        """
        pass

    @abc.abstractmethod
    def batch_insert_to_dynamic_table(
        self,
        table_name: str,
        values: list[dict],
        standard_fields: list[DatabaseFieldStandard],
    ):
        """
        批量插入数据至动态表
        :param table_name: 动态表名
        :param values: dict 列表
        :param standard_fields: 标准字段列表
        :return: 插入结果
        """
        pass

    @abc.abstractmethod
    def update_dynamic_table(self, table_name: str, value: dict, rowid: str):
        """
        更新数据至动态表
        :param table_name: 动态表名
        :param value: dict
        :param rowid: 行标识
        :return: 插入结果
        """
        pass

    @abc.abstractmethod
    def created_field_string_from_field_standard(self, field_standard: DatabaseFieldStandard):
        """
        根据标准字段返回各自数据库对应的字段创建字符串
        :param field_standard: DatabaseFieldStandard
        :return: 数据库对应的字段创建字符串
        """
        pass

    @abc.abstractmethod
    def data_type_format(self, data_type):
        """
        将数据库返回的数据类型转换为系统规范的数据类型
        :param data_type: 数据类型
        :return: 系统规范的数据类型
        """
        pass

    @abc.abstractmethod
    def reserved_words_list(self):
        """
        保留字列表
        :return: 数据库保留字列表
        """
        pass

    @abc.abstractmethod
    def execute(self, sql, params=None):
        """
        执行 insert、update、delete
        :param sql: sql
        :param params:
        :return: 成功/失败 ResultCode
        """
        pass

    @abc.abstractmethod
    def execute_arr(self, sql_arr, params_dict=None):
        """
        同时执行多条 insert、update、delete
        :param sql_arr: sql 数组
        :param params_dict:
        :return: 成功/失败 ResultCode
        """
        pass

    @abc.abstractmethod
    def execute_many(self, sql, params):
        """
        执行批量插入
        :param sql: sql
        :param params: 绑定变量, [[], []] 形式
        :return: 成功/失败 ResultCode
        """
        pass

    @abc.abstractmethod
    def commit(self):
        """
        提交事务
        :return:
        """
        pass

    @abc.abstractmethod
    def rollback(self):
        """
        回滚事务
        :return:
        """
        pass

    @abc.abstractmethod
    def fetchone_without_auto_transaction(self, sql, params=None):
        """
        非自动事务获取一条记录
        :param sql:
        :param params:
        :return:
        """
        pass

    @abc.abstractmethod
    def fetchall_without_auto_transaction(self, sql, params=None):
        """
        非自动事务获取多条记录
        :param sql:
        :param params:
        :return:
        """
        pass

    @abc.abstractmethod
    def execute_without_auto_transaction(self, sql, params=None):
        """
        非自动事务执行 insert、update、delete
        :param sql:
        :param params:
        :return:
        """
        pass

    @abc.abstractmethod
    def execute_arr_without_auto_transaction(self, sql_arr, params_dict=None):
        """
        非自动事务执行多条 insert、update、delete
        :param sql_arr:
        :param params_dict:
        :return:
        """
        pass


class AbstractTransientSqlHelper(abc.ABC):
    def __init__(self, connection_string, user, password):
        self.conn = None
        self.cursor = None
        self.connection_string = connection_string
        self.user = user
        self.password = password

    @abc.abstractmethod
    def open(self):
        pass

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def fetchone(self, sql, params=None):
        """
        获取一条记录
        :param sql: sql
        :param params:
        :return: 字典
        """
        pass

    @abc.abstractmethod
    def fetchall(self, sql, params=None):
        """
        获取记录数组
        :param sql: sql
        :param params:
        :return: 字典数组
        """
        pass

    @abc.abstractmethod
    def fetchpage(self, sql, current, size, params=None) -> Page:
        """
        获取记录数组(分页)
        :param sql: sql
        :param current: 当前页码
        :param size: 每页条数
        :param params:
        :return: 字典数组
        """
        pass

    @abc.abstractmethod
    def execute(self, sql, params=None):
        """
        执行 insert、update、delete
        :param sql: sql
        :param params:
        :return: 成功/失败 ResultCode
        """
        pass

    @abc.abstractmethod
    def execute_arr(self, sql_arr, params_dict=None):
        """
        执行多条 insert、update、delete
        :param sql_arr: sql 数组
        :param params_dict:
        :return: 成功/失败 ResultCode
        """
        pass

    @abc.abstractmethod
    def fetch_tables(self) -> list[DatabaseTable]:
        """
        获取表名称列表
        :return: 表名称列表
        """
        pass

    @abc.abstractmethod
    def fetch_tables_by_page(self, table_name, current, size) -> Page:
        """
        获取表名称列表分页
        :return: 表名称列表
        """
        pass

    def fetch_tables_format(self):
        """
        获取拥有表列表
        :return: 源数据列表
         {
          "table_name": 表名
          "comment": 描述
         }
        """
        data = self.fetch_tables()
        lower_tables = [self._create_database_table_format(table) for table in data]
        return lower_tables

    @abc.abstractmethod
    def fetch_fields(self, table_name) -> list[DatabaseField]:
        """
        获取表名为 table_name 的元数据信息
        :param table_name: 表名
        :return: 源数据列表
        """
        pass

    def fetch_fields_format(self, table_name):
        """
        获取表名为 table_name 的格式化元数据信息
        :param table_name: 表名
        :return: 源数据列表
         {
          "name": 字段名
          "nick_name": 字段中文
          "data_type": 字段类型
         }
        """
        data = self.fetch_fields(table_name)
        lower_fields = [self._create_database_field_format(field) for field in data]
        return lower_fields

    def fetch_fields_standard(self, table_name) -> list[DatabaseFieldStandard]:
        """
        获取表名为 table_name 的格式化元数据信息
        :param table_name: 表名
        :return: 数据标准列列表
        """
        data = self.fetch_fields(table_name)
        lower_fields = [self._create_database_field_standard(field) for field in data]
        return lower_fields

    @staticmethod
    def _create_database_table_format(table):
        name = (table.table_name or "").lower()
        comment = table.comment or table.table_name or ""
        return DatabaseTable(name, comment)

    def _create_database_field_format(self, field):
        name = (field.column_name or "").lower()
        nick_name = field.column_comment or field.column_name or ""
        dt = field.data_type
        data_type = self.data_type_format(dt)
        return DatabaseFieldFormat(name, data_type, nick_name)

    def _create_database_field_standard(self, field):
        column_name = field.column_name
        data_type = self.data_type_standard(field.data_type)
        column_comment = field.column_comment
        data_length = field.data_length
        data_precision = field.data_precision
        data_scale = field.data_scale
        is_pk = field.is_pk
        is_not_null = field.is_not_null
        return DatabaseFieldStandard(
            column_name,
            data_type,
            column_comment,
            data_length,
            data_precision,
            data_scale,
            is_pk,
            is_not_null,
        )

    @staticmethod
    def _create_database_table(field):
        name = field.get("table_name") or ""
        comments = field.get("comments") or field.get("table_name") or ""
        return DatabaseTable(name, comments)

    @staticmethod
    def _create_database_field(field):
        column_name = field.get("column_name") or ""
        data_type = field.get("data_type") or ""
        column_comment = field.get("column_comment") or field.get("column_name") or ""
        data_length = field.get("data_length")
        data_precision = field.get("data_precision")
        data_scale = field.get("data_scale")
        data_type_new = field.get("data_type_new")
        is_pk = field.get("is_pk") == 1
        is_not_null = field.get("is_not_null") == 1
        data_default = field.get("data_default")
        return DatabaseField(
            column_name,
            data_type,
            column_comment,
            data_length,
            data_precision,
            data_scale,
            data_type_new,
            is_pk,
            is_not_null,
            data_default,
        )

    @abc.abstractmethod
    def data_type_standard(self, data_type) -> DatabaseFieldStandardDataType:
        """
        将数据库返回的数据类型转换为系统规范的数据类型用于动态创建表
        :param data_type: 数据类型
        :return: 系统规范的数据类型
        """
        pass

    @abc.abstractmethod
    def data_type_format(self, data_type):
        """
        将数据库返回的数据类型转换为系统规范的数据类型
        :param data_type: 数据类型
        :return: 系统规范的数据类型
        """
        pass

    def insert_by_fields(self, table_name, field_arr: list, value_dict: dict):
        """
        获取表名为 table_name 的元数据信息
        :param table_name: 表名
        :param field_arr: 列信息对象
        :param value_dict: 值的字典
        :return: 源数据列表
        """
        field_sql = ",".join([f'"{x.get("column_name")}"' for x in field_arr])
        value_sql = ",".join([f':{x.get("column_name")}' for x in field_arr])
        sql = f'insert into "{table_name}" ({field_sql}) values({value_sql})'
        return self.execute_arr([sql], {0: value_dict})
