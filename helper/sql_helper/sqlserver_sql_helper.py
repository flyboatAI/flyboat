import base64
import re
from decimal import Decimal

import pymssql
from loguru import logger

from entity.database.database_field import DatabaseField
from entity.database.database_table import DatabaseTable
from entity.page.page import Page
from enum_type.database_field_data_type import DatabaseFieldStandardDataType
from enum_type.result_code import ResultCode
from enum_type.user_data_type import UserDataType
from error.general_error import GeneralError
from helper.sql_helper.abstract_sql_helper import AbstractTransientSqlHelper


class TransientSqlserverSqlHelper(AbstractTransientSqlHelper):
    @staticmethod
    def split_connection_string(connection_string):
        # 使用正则表达式匹配主机地址和端口
        # "jdbc:sqlserver://localhost\SQLEXPRESS:1433;databaseName=yourDatabaseName;user=yourUsername;password=yourPassword;encrypt=true;trustServerCertificate=true"
        pattern = (
            r"jdbc:sqlserver://(?P<server>[^\\[:;]+)(\\(?P<instance>[^:;]+))?(\:(?P<port>\d+))?(;(?P<properties>.*))?"
        )
        match = re.match(pattern, connection_string)
        if not match:
            raise GeneralError("连接字符串解析失败, 无法解析出主机地址") from None
        host = match.group("server")
        instance_name = match.group("instance")
        port = match.group("port") if match.group("port") else "1433"
        properties = match.group("properties")

        # 将 properties 部分进一步拆分为键值对
        properties_dict = {}
        if properties:
            prop_pairs = properties.split(";")
            for pair in prop_pairs:
                if "=" in pair:
                    key, value = pair.split("=", 1)
                    properties_dict[key] = value
        database = properties_dict.get("databaseName")
        if not database:
            raise GeneralError("连接字符串解析失败, 未指定具体数据库") from None
        return host, port, instance_name, database

    def __init__(self, connection_string, user, password):
        super().__init__(connection_string, user, password)
        host, port, instance_name, database = self.split_connection_string(connection_string)
        self.host = host
        self.port = port
        self.instance_name = instance_name
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
        cursor.execute("set showplan_xml on")
        cursor.execute(sql)
        # noinspection PyTypeChecker
        plan: dict = cursor.fetchone()
        exec_plan = list(plan.values())[0]
        # 关闭执行计划模式
        cursor.execute("set showplan_xml off")

        # 解析执行计划中的表名称
        import xml.etree.ElementTree

        root = xml.etree.ElementTree.fromstring(exec_plan)
        namespaces = {"ns0": "http://schemas.microsoft.com/sqlserver/2004/07/showplan"}

        table_name: str | None = None
        for obj in root.findall(".//ns0:Object", namespaces):
            table_name = obj.attrib.get("Table").strip("[]")
        if not table_name:
            return []

        cursor.execute(sql, params)

        desc_tuple = cursor.description
        cursor_desc_dict = {d[0]: d for d in desc_tuple}
        # noinspection PyTypeChecker
        result: list[dict] = cursor.fetchall()

        meta_sql = f"""
                        select 
                            c.name as column_name,
                            t.name as data_type
                        from
                            sys.columns c 
                        join
                            sys.types t 
                        on 
                            c.user_type_id = t.user_type_id
                        where 
                            c.object_id = object_id('{table_name}')
                    """
        cursor.execute(meta_sql)
        # noinspection PyTypeChecker
        column_types: dict = {item["column_name"]: item["data_type"] for item in cursor.fetchall()}

        result = [
            {
                k.lower(): None
                if v is None
                else v.hex()
                if column_types.get(k) == "timestamp"
                else v.strftime("%H:%M:%S")
                if column_types.get(k) == "time"
                else v
                if column_types.get(k) == "date" or column_types.get(k) == "datetime2"
                else base64.b64encode(v).decode("ascii")
                if column_types.get(k) == "image"
                else v.decode("utf-8").replace("\u0000", "")
                if cursor_desc_dict[k][1] == 2
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
        # page_sql = sql + f" offset {(current - 1) * size} rows fetch next {current * size} rows only"
        page_sql = f"""
            with cte as (
                select row_number() over (order by (select null)) as row_num,
                    *
                from ({sql}) as origin_query
            )
            select * from cte where row_num between {(current - 1) * size + 1} and {current * size}
        """
        # 查询数据
        try:
            cursor.execute("set showplan_xml on")
            cursor.execute(sql)
            # noinspection PyTypeChecker
            plan: dict = cursor.fetchone()
            exec_plan = list(plan.values())[0]
            # 关闭执行计划模式
            cursor.execute("set showplan_xml off")

            # 解析执行计划中的表名称
            import xml.etree.ElementTree

            root = xml.etree.ElementTree.fromstring(exec_plan)
            namespaces = {"ns0": "http://schemas.microsoft.com/sqlserver/2004/07/showplan"}

            table_name: str | None = None
            for obj in root.findall(".//ns0:Object", namespaces):
                table_name = obj.attrib.get("Table").strip("[]")
            if not table_name:
                return Page()

            cursor.execute(page_sql, params)
            desc_tuple = cursor.description
            cursor_desc_dict = {d[0]: d for d in desc_tuple}
            # noinspection PyTypeChecker
            data: list[dict] = cursor.fetchall()
            # 查询总条数
            cursor.execute(count_sql, params)
            # noinspection PyTypeChecker
            count: dict = cursor.fetchone()
            meta_sql = f"""
                        select 
                            c.name as column_name,
                            t.name as data_type
                        from
                            sys.columns c 
                        join
                            sys.types t 
                        on 
                            c.user_type_id = t.user_type_id
                        where 
                            c.object_id = object_id('{table_name}')
                    """
            cursor.execute(meta_sql)
            # noinspection PyTypeChecker
            column_types: dict = {item["column_name"]: item["data_type"] for item in cursor.fetchall()}
            data = [
                {
                    k.lower(): None
                    if v is None
                    else v.hex()
                    if column_types.get(k) == "timestamp"
                    else v.strftime("%H:%M:%S")
                    if column_types.get(k) == "time"
                    else v
                    if column_types.get(k) == "date" or column_types.get(k) == "datetime2"
                    else base64.b64encode(v).decode("ascii")
                    if column_types.get(k) == "image"
                    else v.decode("utf-8").replace("\u0000", "")
                    if cursor_desc_dict[k][1] == 2
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
        except pymssql.Error as err:
            logger.error(f"SQL FETCH PAGE ERROR: mssqlError {err}")
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
        except pymssql.Error as err:
            logger.error(f"SQL EXECUTE ERROR: mssqlError {err}")
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
        except pymssql.Error as err:
            logger.error(f"SQL EXECUTE ERROR: mssqlError {err}")
            conn.rollback()
            return ResultCode.Error.value
        return ResultCode.Success.value

    def fetch_tables(self) -> list[DatabaseTable]:
        sql = (
            "select t.name as table_name, ep.value as comments "
            "from sys.tables t "
            "left join sys.extended_properties ep on "
            "ep.major_id = t.object_id and "
            "ep.minor_id = 0 and "
            "ep.class = 1 and "
            "ep.name = 'MS_Description' "
            "where schema_name(t.schema_id) = 'dbo'"
        )
        data = self.fetchall(sql, [])
        lower_tables = [self._create_database_table(field) for field in data]
        return lower_tables

    def fetch_tables_by_page(self, table_name, current, size) -> Page:
        s = f"and upper(t.name) like upper('%{table_name}%')" if table_name else ""
        # language=SQL Server
        sql = (
            "select t.name as table_name, ep.value as comments "
            "from sys.tables t "
            "left join sys.extended_properties ep on "
            "ep.major_id = t.object_id and "
            "ep.minor_id = 0 and "
            "ep.class = 1 and "
            "ep.name = 'MS_Description' "
            "where schema_name(t.schema_id) = 'dbo' "
            f"{s}"
        )

        page = self.fetchpage(sql, current, size)
        data = page.data
        lower_tables = [self._create_database_table(field) for field in data]
        return Page(lower_tables, page.count)

    def fetch_fields(self, table_name: str) -> list[DatabaseField]:
        sql = f"""
        select  
            t1.table_name as table_name,           -- 表名称
            null as table_comment,     -- 表描述
            t2.column_name as column_name,         -- 字段名称
            ep.value as column_comment,         -- 字段描述
            t2.data_type as data_type,           -- 字段类型
            t2.character_maximum_length as data_length, -- 字段长度
            t2.numeric_precision as data_precision,     -- 字段精度
            t2.numeric_scale as data_scale,             -- 字段小数范围
            null as data_type_new,                 -- 组合的字段类型
            iif(t4.column_name is not null, 1, 0) as is_pk,                         -- 是否为主键
            iif(t2.is_nullable = 'no', 1, 0) as is_not_null,                   -- 是否非空
            t2.column_default as data_default     -- 默认值
        from
            information_schema.tables t1
        join
            information_schema.columns t2
        on
            t1.table_schema = t2.table_schema
            and t1.table_name = t2.table_name
        left join sys.extended_properties ep 
        on ep.major_id=object_id(t2.table_schema + '.' + t2.table_name) 
        and ep.minor_id = t2.ordinal_position and
        ep.class = 1 and
        ep.name = 'MS_Description'
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
                group by 
                    kcu.table_schema,
                    kcu.table_name,
                    kcu.column_name
            ) t4
        on
            t2.table_schema = t4.table_schema and
            t2.table_name = t4.table_name
            and t2.column_name = t4.column_name
        where
            upper(t1.table_name) = '{table_name.upper()}' and data_type != 'timestamp'
        order by
            t1.table_name, t2.ordinal_position;
        """
        data = self.fetchall(sql, [])
        lower_fields = [self._create_database_field(field) for field in data]
        return lower_fields

    def data_type_standard(self, data_type) -> DatabaseFieldStandardDataType:
        if (
            data_type == "binary"
            or data_type == "varbinary"
            or data_type == "varchar"
            or data_type == "nvarchar"
            or data_type == "char"
            or data_type == "nchar"
        ):
            return DatabaseFieldStandardDataType.Varchar2
        elif (
            data_type == "tinyint"
            or data_type == "smallint"
            or data_type == "bit"
            or data_type == "int"
            or data_type == "bigint"
        ):
            return DatabaseFieldStandardDataType.Int
        elif (
            data_type == "money"
            or data_type == "float"
            or data_type == "numeric"
            or data_type == "real"
            or data_type == "decimal"
        ):
            return DatabaseFieldStandardDataType.Decimal
        elif data_type == "date":
            return DatabaseFieldStandardDataType.Date
        elif data_type == "datetime2" or data_type == "datetime":
            return DatabaseFieldStandardDataType.DateTime
        elif data_type.startswith("time"):
            return DatabaseFieldStandardDataType.Time
        elif data_type == "timestamp":
            return DatabaseFieldStandardDataType.Blob
        elif data_type == "image" or data_type == "text" or data_type == "ntext":
            return DatabaseFieldStandardDataType.Clob
        else:
            return DatabaseFieldStandardDataType.Varchar2

    def data_type_format(self, data_type: str):
        if (
            data_type == "image"
            or data_type == "nvarchar"
            or data_type == "char"
            or data_type == "nchar"
            or data_type == "text"
            or data_type == "ntext"
            or data_type == "binary"
            or data_type == "varbinary"
            or data_type == "timestamp"
        ):
            return UserDataType.Varchar2.value
        elif (
            data_type == "bigint"
            or data_type == "smallint"
            or data_type == "tinyint"
            or data_type == "int"
            or data_type == "bit"
            or data_type == "money"
            or data_type == "float"
            or data_type == "numeric"
            or data_type == "real"
            or data_type == "decimal"
        ):
            return UserDataType.Number.value
        elif data_type == "datetime" or data_type == "datetime2" or data_type == "date" or data_type == "time":
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
        meta_sql = f"""
                        select 
                            c.name as column_name,
                            t.name as data_type
                        from
                            sys.columns c 
                        join
                            sys.types t 
                        on 
                            c.user_type_id = t.user_type_id
                        where 
                            c.object_id = object_id('{table_name}')
                    """
        self.cursor.execute(meta_sql)
        # noinspection PyTypeChecker
        column_types: dict = {item["column_name"]: item["data_type"] for item in self.cursor.fetchall()}
        field_sql = ",".join([f'{x.get("column_name")}' for x in field_arr])
        value_sql = ",".join([f'%({x.get("column_name")})s' for x in field_arr])
        sql = f"insert into {table_name} ({field_sql}) values({value_sql})"
        return self.execute_arr(
            [sql],
            {
                0: {
                    c["column_name"]: None
                    if value_dict.get(c["column_name"]) is None
                    else base64.b64decode(value_dict.get(c["column_name"]))
                    if column_types.get(c["column_name"]) == "image"
                    else bytearray(bytes(value_dict.get(c["column_name"]), "utf-8"))
                    if column_types.get(c["column_name"]) == "varbinary"
                    or column_types.get(c["column_name"]) == "binary"
                    else value_dict.get(c["column_name"])
                    for c in field_arr
                }
            },
        )

    def open(self):
        self.conn = pymssql.connect(
            user=self.user,
            password=self.password,
            server=self.host if not self.instance_name else f"{self.host}\\{self.instance_name}",
            database=self.database,
            port=self.port,
            autocommit=False,
            as_dict=True,
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
