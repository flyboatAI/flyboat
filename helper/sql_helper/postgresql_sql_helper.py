import re
from decimal import Decimal

import psycopg2
import psycopg2.extensions
from loguru import logger
from psycopg2.extras import DictCursor

from entity.database.database_field import DatabaseField
from entity.database.database_table import DatabaseTable
from entity.page.page import Page
from enum_type.database_field_data_type import DatabaseFieldStandardDataType
from enum_type.result_code import ResultCode
from enum_type.user_data_type import UserDataType
from error.general_error import GeneralError
from helper.sql_helper.abstract_sql_helper import AbstractTransientSqlHelper


class TransientPostgresqlSqlHelper(AbstractTransientSqlHelper):
    @staticmethod
    def split_connection_string(connection_string):
        # 使用正则表达式匹配主机地址和端口
        # jdbc:postgresql://localhost:5432/mydatabase
        match = re.match(r"jdbc:postgresql://([^:/]+)(?::(\d+))?(?:/([^?]+))?", connection_string)
        if not match:
            raise GeneralError("连接字符串解析失败, 无法解析出主机地址") from None

        host = match.group(1)
        port = int(match.group(2)) if match.group(2) else 5432  # 如果端口不存在，使用默认端口5432
        if not match.group(3):
            raise GeneralError("连接字符串解析失败, 未指定具体数据库") from None
        database = match.group(3)
        return host, port, database

    def __init__(self, connection_string, user, password):
        super().__init__(connection_string, user, password)
        host, port, database = self.split_connection_string(connection_string)
        self.host = host
        self.port = port
        self.database = database

    def fetchone(self, sql, params=None):
        """
        获取一条记录
        :param sql: sql
        :param params:
        :return: 字典
        """
        if params is None:
            params = []
        # logger.info(sql)
        cursor = self.cursor
        cursor.execute(sql, params)
        desc_tuple = cursor.description
        cursor_desc_dict = {d[0]: d for d in desc_tuple}
        # noinspection PyTypeChecker
        result: dict = cursor.fetchone()
        if result:
            result = {
                k.lower(): v if not isinstance(v, Decimal) else float(v) if cursor_desc_dict[k][5] != 0 else int(v)
                for k, v in result.items()
            }
        return result

    def fetchall(self, sql, params=None):
        """
        获取记录数组
        :param sql: sql
        :param params:
        :return: 字典数组
        """
        if params is None:
            params = []
        # logger.info(sql)
        cursor = self.cursor
        cursor.execute(sql, params)
        desc_tuple = cursor.description
        cursor_desc_dict = {d[0]: d for d in desc_tuple}
        # noinspection PyTypeChecker
        result: list[dict] = cursor.fetchall()

        result = [
            {
                k.lower(): None
                if v is None
                else v
                if cursor_desc_dict[k][1] == psycopg2.extensions.BOOLEAN
                else bytes(v)
                if cursor_desc_dict[k][1] == psycopg2.BINARY
                else v.strftime("%H:%M:%S")
                if cursor_desc_dict[k][1] == psycopg2.extensions.TIME
                else str(v)
                if cursor_desc_dict[k][1] == psycopg2.extensions.INTERVAL
                else v
                if not isinstance(v, Decimal)
                else float(v)
                if cursor_desc_dict[k][5] != 0
                else int(v)
                for k, v in d.items()
            }
            for d in result
        ]
        return result

    def fetchpage(self, sql, current, size, params=None) -> Page:
        """
        获取记录数组(分页)
        :param sql: sql
        :param current: 当前页码
        :param size: 每页条数
        :param params:
        :return: 字典数组
        """
        if params is None:
            params = []
        # logger.info(sql)
        cursor = self.cursor
        count_sql = f"select count(1) as c from ({sql}) table_alias"
        page_sql = sql + f" limit {current * size} offset {(current - 1) * size}"
        # 查询数据
        try:
            cursor.execute(page_sql, params)
            desc_tuple = cursor.description
            cursor_desc_dict = {d[0]: d for d in desc_tuple}
            # noinspection PyTypeChecker
            data: list[dict] = cursor.fetchall()
            # 查询总条数
            cursor.execute(count_sql, params)
            # noinspection PyTypeChecker
            count: dict = cursor.fetchone()
            data = [
                {
                    k.lower(): None
                    if v is None
                    else v
                    if cursor_desc_dict[k][1] == psycopg2.extensions.BOOLEAN
                    else bytes(v)
                    if cursor_desc_dict[k][1] == psycopg2.BINARY
                    else v.strftime("%H:%M:%S")
                    if cursor_desc_dict[k][1] == psycopg2.extensions.TIME
                    else str(v)
                    if cursor_desc_dict[k][1] == psycopg2.extensions.INTERVAL
                    else v
                    if not isinstance(v, Decimal)
                    else float(v)
                    if cursor_desc_dict[k][5] != 0
                    else int(v)
                    for k, v in d.items()
                }
                for d in data
            ]
            total_count = {k.lower(): v for k, v in count.items()}["c"]
            page = Page(data, total_count)
            return page
        except psycopg2.Error as err:
            logger.error(f"SQL FETCH PAGE ERROR: postgreError {err}")
            return Page()
        except Exception as err:
            logger.error(f"SQL FETCH PAGE ERROR: err {err}")
            return Page()

    def execute(self, sql, params=None):
        """
        执行 insert、update、delete
        :param sql: sql
        :param params:
        :return: 成功/失败
        """
        if params is None:
            params = []
        # logger.info(sql)
        cursor = self.cursor
        conn = self.conn
        # noinspection PyBroadException
        try:
            cursor.execute(sql, params)
            conn.commit()
        except psycopg2.Error as err:
            logger.error(f"SQL EXECUTE ERROR: postgreError {err}")
            conn.rollback()
            return ResultCode.Error.value
        return ResultCode.Success.value

    def execute_arr(self, sql_arr, params_dict=None):
        """
        执行多条 insert、update、delete
        :param sql_arr: sql 数组
        :param params_dict:
        :return: 成功/失败
        """
        if params_dict is None:
            params_dict = {}
        cursor = self.cursor
        conn = self.conn
        # noinspection PyBroadException
        try:
            for i, sql in enumerate(sql_arr):
                # logger.info(sql)
                if i in params_dict:
                    cursor.execute(sql, params_dict.get(i))
                else:
                    cursor.execute(sql, [])
            conn.commit()
        except psycopg2.Error as err:
            logger.error(f"SQL EXECUTE ERROR: postgreError {err}")
            conn.rollback()
            return ResultCode.Error.value
        except Exception as err:
            logger.error(f"SQL EXECUTE ERROR: Error {err}")
            conn.rollback()
            return ResultCode.Error.value
        return ResultCode.Success.value

    def fetch_tables(self) -> list[DatabaseTable]:
        sql = """
            select 
                table_name,
                obj_description((table_schema || '.' || table_name)::regclass, 'pg_class') as table_comment
            from 
                information_schema.tables
            where 
                table_schema = 'public'  -- 只查询public schema的表。如果要查询其他schema，修改或删除此条件
                and table_type = 'BASE TABLE'  -- 只查询基本表，排除视图
        """
        data = self.fetchall(sql, [])
        lower_tables = [self._create_database_table(field) for field in data]
        return lower_tables

    def fetch_tables_by_page(self, table_name, current, size) -> Page:
        s = f"and upper(t.table_name) like upper('%{table_name}%')" if table_name else ""
        sql = f"""
            select 
                table_name,
                obj_description((table_schema || '.' || table_name)::regclass, 'pg_class') as table_comment
            from 
                information_schema.tables
            where 
                table_schema = 'public'  -- 只查询public schema的表。如果要查询其他schema，修改或删除此条件
                and table_type = 'BASE TABLE'  -- 只查询基本表，排除视图
                {s}
        """
        page = self.fetchpage(sql, current, size)
        data = page.data
        lower_tables = [self._create_database_table(field) for field in data]
        return Page(lower_tables, page.count)

    def fetch_fields(self, table_name: str) -> list[DatabaseField]:
        sql = f"""
        select
            t1.table_name as table_name,           -- 表名称
            obj_description((t1.table_schema || '.' || t1.table_name)::regclass, 'pg_class') 
            as table_comment, -- 表描述
            t2.column_name as column_name,         -- 字段名称
            col_description(pg_table.oid, t2.ordinal_position) as column_comment, -- 字段描述
            t2.data_type as data_type,             -- 字段类型
            t2.character_maximum_length as data_length, -- 字段长度
            t2.numeric_precision as data_precision,     -- 字段精度
            t2.numeric_scale as data_scale,             -- 字段小数范围
            null as data_type_new,                 -- 组合的字段类型
            case
                when t4.column_name is not null then 1
                else 0
            end as is_pk,                         -- 是否为主键
            case
                when t2.is_nullable = 'NO' then 1
                else 0
            end as is_not_null,                   -- 是否非空
            t2.column_default as data_default     -- 默认值
        from
            information_schema.tables t1
        join
            information_schema.columns t2
        on
            t1.table_schema = t2.table_schema
            and t1.table_name = t2.table_name
        join
            pg_catalog.pg_class pg_table
        on
            pg_table.relname = t1.table_name
        left join
            (
                select 
                    kcu.table_schema,
                    kcu.table_name,
                    kcu.column_name
                from 
                    information_schema.table_constraints tc
                join 
                    information_schema.key_column_usage kcu
                on 
                    tc.constraint_name = kcu.constraint_name
                    and tc.table_schema = kcu.table_schema
                where 
                    tc.constraint_type = 'PRIMARY KEY'
                    and tc.table_schema = 'public'
                group by 
                    kcu.table_schema,
                    kcu.table_name,
                    kcu.column_name
                order by 
                    kcu.table_schema,
                    kcu.table_name
            ) t4
        on
            t2.table_schema = t4.table_schema and
            t2.table_name = t4.table_name
            and t2.column_name = t4.column_name
        where
            t1.table_schema = 'public'
            and upper(t1.table_name) = '{table_name.upper()}'
        order by
            t1.table_name, t2.ordinal_position;
        """
        data = self.fetchall(sql, [])
        lower_fields = [self._create_database_field(field) for field in data]
        return lower_fields

    def data_type_standard(self, data_type) -> DatabaseFieldStandardDataType:
        if data_type == "varchar" or data_type == "char" or data_type == "bit":
            return DatabaseFieldStandardDataType.Varchar2
        elif (
            data_type == "integer"
            or data_type == "smallint"
            or data_type == "bigint"
            or data_type == "int"
            or data_type == "boolean"
        ):
            return DatabaseFieldStandardDataType.Int
        elif data_type.startswith("decimal"):
            return DatabaseFieldStandardDataType.Decimal
        elif data_type == "date":
            return DatabaseFieldStandardDataType.Date
        elif data_type.startswith("timestamp"):
            return DatabaseFieldStandardDataType.DateTime
        elif data_type.startswith("time") or data_type == "interval":
            return DatabaseFieldStandardDataType.Time
        elif data_type == "bytea":
            return DatabaseFieldStandardDataType.Blob
        elif data_type == "text":
            return DatabaseFieldStandardDataType.Clob
        else:
            return DatabaseFieldStandardDataType.Varchar2

    def data_type_format(self, data_type: str):
        if (
            data_type == "varchar"
            or data_type == "char"
            or data_type == "bit"
            or data_type == "text"
            or data_type == "bytea"
        ):
            return UserDataType.Varchar2.value
        elif (
            data_type == "integer"
            or data_type == "smallint"
            or data_type == "bigint"
            or data_type == "int"
            or data_type == "boolean"
            or data_type == "decimal"
        ):
            return UserDataType.Number.value
        elif (
            data_type.startswith("timestamp")
            or data_type == "date"
            or data_type == "interval"
            or data_type.startswith("time")
        ):
            return UserDataType.Date.value
        else:
            return UserDataType.Varchar2.value

    def insert_by_fields(self, table_name, field_arr: list, value_dict: dict):
        """
        获取表名为 table_name 的元数据信息
        :param table_name: 表名
        :param field_arr: 列信息对象
        :param value_dict: 值的字典
        :return: 源数据列表
        """
        field_sql = ",".join([f'{x.get("column_name")}' for x in field_arr])
        value_sql = ",".join([f'%({x.get("column_name")})s' for x in field_arr])
        sql = f"insert into {table_name} ({field_sql}) values({value_sql})"
        return self.execute_arr([sql], {0: value_dict})

    def open(self):
        self.conn = psycopg2.connect(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
            cursor_factory=DictCursor,
        )
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
