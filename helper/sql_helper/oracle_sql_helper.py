import re

import oracledb
from loguru import logger
from oracledb import DatabaseError

from config import setting
from config.dynamic_table_setting import TS_COLUMN_NAME
from entity.database.database_field import DatabaseField
from entity.database.database_field_standard import DatabaseFieldStandard
from entity.database.database_table import DatabaseTable
from entity.page.page import Page
from enum_type.database_field_data_type import DatabaseFieldStandardDataType
from enum_type.db_pool_type import DataBasePoolType
from enum_type.result_code import ResultCode
from enum_type.user_data_type import UserDataType
from error.general_error import GeneralError
from helper.sql_helper.abstract_sql_helper import (
    AbstractSqlHelper,
    AbstractTransientSqlHelper,
)

oracledb.defaults.fetch_lobs = False
oracle_pool = oracledb.create_pool(
    user=setting.ORACLE_USER,
    password=setting.ORACLE_PASSWORD,
    host=setting.ORACLE_HOST,
    port=setting.ORACLE_PORT,
    service_name=setting.ORACLE_SERVICE_NAME,
    min=2,
    max=5,
    increment=1,
    disable_oob=True,
)

oracle_pool2 = oracledb.create_pool(
    user=setting.ORACLE_USER2,
    password=setting.ORACLE_PASSWORD2,
    host=setting.ORACLE_HOST2,
    port=setting.ORACLE_PORT2,
    service_name=setting.ORACLE_SERVICE_NAME2,
    min=2,
    max=5,
    increment=1,
    disable_oob=True,
)


class OracleSqlHelper(AbstractSqlHelper):
    @staticmethod
    def get_pool(pool_type):
        """
        返回数据库连接池对象
        :param pool_type: 连接池类型
        :return: 业务库、动态库
        """
        return oracle_pool if pool_type == DataBasePoolType.Business else oracle_pool2

    def open(self):
        self.conn = self.pool.acquire()
        self.conn.autocommit = False
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def fetchone_without_auto_transaction(self, sql, params=None):
        return self.fetchone(sql, params)

    def fetchall_without_auto_transaction(self, sql, params=None):
        return self.fetchall(sql, params)

    def execute_without_auto_transaction(self, sql, params=None):
        if params is None:
            params = []
        # logger.info(sql)
        cursor = self.cursor
        # noinspection PyBroadException
        cursor.execute(sql, params)
        return ResultCode.Success.value

    def execute_arr_without_auto_transaction(self, sql_arr, params_dict=None):
        if params_dict is None:
            params_dict = {}
        cursor = self.cursor
        # noinspection PyBroadException
        for i, sql in enumerate(sql_arr):
            if i in params_dict:
                cursor.execute(sql, params_dict.get(i))
            else:
                cursor.execute(sql, [])
        return ResultCode.Success.value

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
        columns = [col[0].lower() for col in cursor.description]
        cursor.rowfactory = lambda *args: dict(zip(columns, args))
        result = cursor.fetchone()
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
        columns = [col[0].lower() for col in cursor.description]
        cursor.rowfactory = lambda *args: dict(zip(columns, args))
        result = cursor.fetchall()
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
        count_sql = f"select count(1) as c from ({sql})"
        page_sql = (
            f"select * from (select tt.*, rownum as rowno from ({sql}) tt "
            f"where rownum <= ({current} * {size})) table_alias "
            f"where table_alias.rowno >= (({current} - 1) * {size} + 1) "
        )
        # 查询数据
        # noinspection PyBroadException
        try:
            cursor.execute(page_sql, params)
        except Exception:
            return Page([], 0)
        columns = [col[0].lower() for col in cursor.description]
        cursor.rowfactory = lambda *args: dict(zip(columns, args))
        data = cursor.fetchall()
        # 查询总条数
        cursor.execute(count_sql, params)
        columns_count = [col[0].lower() for col in cursor.description]
        cursor.rowfactory = lambda *args: dict(zip(columns_count, args))
        count_dict = cursor.fetchone()
        count = count_dict["c"]
        page = Page(data, count)
        return page

    def fetch_fields(self, table_name: str) -> list[DatabaseField]:
        sql = (
            "select t1.table_name --表名称\n"
            "      ,t1.comments as table_comment  --表描述\n"
            "      ,t2.column_name  --字段名称\n"
            "      ,t3.comments as column_comment  --字段描述\n"
            "      ,t2.data_type  --字段类型\n"
            "      ,t2.data_length  --字段长度\n"
            "      ,t2.data_precision  --字段精度\n"
            "      ,t2.data_scale  --字段小数范围\n"
            "      ,t2.data_type_new  --组合的字段类型\n"
            "      ,case when t4.table_name is not null then 1 \n"
            "        else 0 end is_pk  --是否为主键\n"
            "      ,t2.is_not_null  --是否为空\n"
            "      ,t2.data_default  --默认值\n"
            "      ,t3.comments\n"
            "  from "
            "  ( --该用户下表名和表描述\n"
            "    select table_name, comments"
            "     from user_tab_comments "
            "    where table_type = 'TABLE'"
            "  ) t1 "
            "  left join "
            "  ( --该用户下表名、字段信息\n"
            "   select table_name  --表名\n"
            "          ,column_name  --字段名\n"
            "          ,data_type  --字段类型\n"
            "          ,data_length  --字段长度\n"
            "          ,data_precision  --字段精度(理解为整数位数)\n"
            "          ,data_scale  --字段小数位位数\n"
            "          ,case when nullable = 'N' then 1 \n"
            "            else 0 end as is_not_null  --是否非空\n"
            "          ,data_default  --默认值\n"
            "          ,case when data_precision is not null and data_scale is not null "
            "           then data_type||'('||data_precision||','||data_scale||')'\n"
            "                when data_precision is not null and data_scale is null "
            "           then data_type||'('||data_precision||')'\n"
            "                when data_precision is null and data_scale is null and data_length is not null "
            "           then data_type||'('||data_length||')'\n"
            "           else data_type end as data_type_new\n"
            "    ,column_id "
            "      from user_tab_columns"
            "  ) t2 "
            "  on t1.table_name = t2.table_name "
            "  left join "
            "  ("
            " --该用户下表名和字段描述\n"
            " select table_name, column_name, comments from user_col_comments"
            "  ) t3 "
            "  on t2.table_name = t3.table_name and t2.column_name = t3.column_name\n"
            "  left join "
            "  ( --该用户下的主键信息\n"
            "   select ucc.table_name, ucc.column_name\n"
            "     from user_cons_columns ucc, user_constraints uc\n"
            "    where uc.constraint_name = ucc.constraint_name\n"
            "       and uc.constraint_type = 'P'\n"
            "  ) t4 "
            "  on t2.table_name = t4.table_name and t2.column_name = t4.column_name\n"
            f" where upper(t2.table_name)='{table_name.upper()}' "
            "  order by t1.table_name, t2.column_id --保证字段顺序和原表字段顺序一致"
        )
        data = self.fetchall(sql, [])
        lower_fields = [self._create_database_field(field) for field in data]
        lower_fields = [f for f in lower_fields if f is not None]
        return lower_fields

    def create_dynamic_table(self, table_name: str, fields: list[DatabaseFieldStandard]):
        """
        根据 DatabaseField 列表, 动态建表
        :param table_name: 动态表名
        :param fields: DatabaseFieldStandard 列表
        :return: 建表
        """
        created_fields_list = []
        create_column_comment_sql = []
        pk_list = []
        create_primary_key_sql = None
        for field in fields:
            f = self.created_field_string_from_field_standard(field)
            created_fields_list.append(f)
            if field.column_comment:
                create_column_comment_sql.append(
                    f"comment on column {table_name}.{field.column_name.upper()} " f"is '{field.column_comment}'"
                )
            if field.is_pk:
                pk_list.append(field.column_name.upper())
        if pk_list:
            pks = ", ".join(pk_list)
            pk_name = "_".join(pk_list)
            create_primary_key_sql = (
                f"alter table {table_name} " f"add constraint {table_name}_{pk_name}_pk primary key({pks})"
            )
        custom_ts_field_name = TS_COLUMN_NAME
        if custom_ts_field_name not in [f.column_name.upper() for f in fields]:
            created_fields_list.append(f"{custom_ts_field_name} TIMESTAMP not null")
        field_content = ", ".join(created_fields_list)

        create_table_sql = f"create table {table_name} (" f"{field_content}" f")"
        sql_list = [create_table_sql]
        if create_primary_key_sql:
            sql_list.append(create_primary_key_sql)
        if create_column_comment_sql:
            sql_list.extend(create_column_comment_sql)
        return self.execute_arr(sql_list)

    def insert_field_to_dynamic_table(self, table_name: str, field: DatabaseFieldStandard):
        """
        根据 DatabaseField, 向表内动态插入字段
        :param table_name: 动态表名
        :param field: DatabaseFieldStandard
        :return: 插入结果
        """
        f = self.created_field_string_from_field_standard(field)
        sql = f"alter table {table_name} add {f}"
        return self.execute(sql)

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
        field_sql = ", ".join([f'"{x.column_name.upper()}"' for x in standard_fields])
        value_sql = ", ".join([f":{x.column_name.upper()}" for x in standard_fields])
        sql = f"insert into {table_name} ({field_sql}) values({value_sql})"
        field_names = [f.column_name.upper() for f in standard_fields]
        # 过滤不参与绑定参数的字典 key
        values = [{k.upper(): v for (k, v) in v.items() if k.upper() in field_names} for v in values]
        return self.execute_many(sql, values)

    def update_dynamic_table(self, table_name: str, value: dict, rowid: str):
        """
        更新数据至动态表
        :param table_name: 动态表名
        :param value: dict
        :param rowid: 行标识
        :return: 插入结果
        """
        keys = value.keys()
        update_field_sql = ", ".join([f"{x.upper()}=:{x.upper()}" for x in keys])
        sql = f"update {table_name} set {update_field_sql} where rowid='{rowid}'"
        params = [{k.upper(): v for (k, v) in value.items()}]
        return self.execute_many(sql, params)

    def created_field_string_from_field_standard(self, field_standard: DatabaseFieldStandard):
        reserved_words = self.reserved_words_list()
        convert_string_dict = {
            DatabaseFieldStandardDataType.Varchar2: ("VARCHAR2", ["4000"]),
            DatabaseFieldStandardDataType.DateTime: ("DATE", None),
            DatabaseFieldStandardDataType.Date: ("DATE", None),
            DatabaseFieldStandardDataType.Time: ("VARCHAR2", ["4000"]),
            DatabaseFieldStandardDataType.Int: ("NUMBER", None),
            DatabaseFieldStandardDataType.Decimal: ("NUMBER", None),
            DatabaseFieldStandardDataType.Blob: ("BLOB", None),
            DatabaseFieldStandardDataType.Clob: ("CLOB", None),
        }
        convert_string_tuple = convert_string_dict.get(field_standard.data_type)
        if not convert_string_tuple:
            return ""
        real_type = convert_string_tuple[0]
        field_name = field_standard.column_name
        upper_field_name = field_name.upper()
        if upper_field_name in reserved_words:
            raise GeneralError(f"{field_name} 为保留字, 无法作为字段英文名使用, 请修改后重试") from None
        not_null = "NOT NULL" if field_standard.is_not_null else ""
        precision_list = convert_string_tuple[1]
        if not precision_list:
            created_field_str = f"{upper_field_name} {real_type} {not_null}"
        else:
            if real_type == DatabaseFieldStandardDataType.Varchar2:
                data_length = field_standard.data_length
                precision_list = [f"{data_length}"] if data_length and data_length > 0 else precision_list
            bracket_content = ", ".join(precision_list)
            created_field_str = f"{upper_field_name} {real_type}({bracket_content}) {not_null}"

        return created_field_str

    def data_type_format(self, data_type: str):
        if (
            data_type == "VARCHAR2"
            or data_type == "NVARCHAR2"
            or data_type == "CHAR"
            or data_type == "NCHAR"
            or data_type == "LONG"
            or data_type == "BLOB"
            or data_type == "CLOB"
            or data_type == "NCLOB"
        ):
            return UserDataType.Varchar2.value
        elif (
            data_type == "NUMBER"
            or data_type == "INTEGER"
            or data_type == "FLOAT"
            or data_type == "BINARY_FLOAT"
            or data_type == "BINARY_DOUBLE"
        ):
            return UserDataType.Number.value
        elif data_type.startswith("TIMESTAMP") or data_type == "DATE":
            return UserDataType.Date.value
        else:
            return UserDataType.Varchar2.value

    def reserved_words_list(self) -> list[str]:
        # 校验字段是否是保留字
        reserved_words_list = self.fetchall("select keyword from v$reserved_words m where m.reserved='Y'")
        reserved_words = [w.get("keyword") for w in reserved_words_list]
        reserved_words.append(TS_COLUMN_NAME)  # 同时排序自定义时间排序字段
        return reserved_words

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
        except Exception as err:
            logger.error(f"SQL EXECUTE ERROR: {err}")
            conn.rollback()
            return ResultCode.Error.value
        conn.commit()
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
        except Exception as err:
            logger.error(f"SQL EXECUTE ERROR: {err}")
            conn.rollback()
            return ResultCode.Error.value
        conn.commit()
        return ResultCode.Success.value

    def execute_many(self, sql, params):
        """
        执行批量插入
        :param sql: sql
        :param params: 绑定变量, [[], []] 形式
        :return: 成功/失败 ResultCode
        """
        cursor = self.cursor
        conn = self.conn
        # noinspection PyBroadException
        try:
            cursor.executemany(sql, params)
            conn.commit()
        except Exception as err:
            logger.error(f"SQL EXECUTE ERROR: err {err}")
            conn.rollback()
            return ResultCode.Error.value
        return ResultCode.Success.value

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class TransientOracleSqlHelper(AbstractTransientSqlHelper):
    @staticmethod
    def split_connection_string(connection_string):
        # 正则表达式匹配主机、端口、服务名称或SID
        pattern = r"jdbc:oracle:thin:@(?P<host>[^:/]+)(:(?P<port>\d+))?(?P<separator>/|:)?(?P<service_name>[^:]+)?"

        match = re.match(pattern, connection_string)
        if not match:
            raise GeneralError("连接字符串解析失败, 无法解析出主机地址") from None

        host = match.group("host")
        port = match.group("port") or 1521  # 提供默认端口
        separator = match.group("separator")
        service_name_or_sid = match.group("service_name")

        # 判断是 service_name 还是 sid
        if separator == "/":
            return {
                "host": host,
                "port": int(port),
                "service_name": service_name_or_sid,
            }
        else:
            return {"host": host, "port": int(port), "sid": service_name_or_sid}

    def __init__(self, connection_string, user, password):
        super().__init__(connection_string, user, password)
        self.connect_params = self.split_connection_string(connection_string)

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
        columns = [col[0].lower() for col in cursor.description]
        cursor.rowfactory = lambda *args: dict(zip(columns, args))
        result = cursor.fetchone()
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
        columns = [col[0].lower() for col in cursor.description]
        cursor.rowfactory = lambda *args: dict(zip(columns, args))
        result = cursor.fetchall()
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
        try:
            count_sql = f"select count(1) as c from ({sql})"
            page_sql = (
                f"select * from (select tt.*, rownum as rowno from ({sql}) tt "
                f"where rownum <= ({current} * {size})) table_alias "
                f"where table_alias.rowno >= (({current} - 1) * {size} + 1) "
            )
            # 查询数据
            cursor.execute(page_sql, params)
            columns = [col[0].lower() for col in cursor.description]
            cursor.rowfactory = lambda *args: dict(zip(columns, args))
            data = cursor.fetchall()
            # 查询总条数
            cursor.execute(count_sql, params)
            columns_count = [col[0].lower() for col in cursor.description]
            cursor.rowfactory = lambda *args: dict(zip(columns_count, args))
            count_dict = cursor.fetchone()
            count = count_dict["c"]
            page = Page(data, count)
            return page
        except DatabaseError as err:
            logger.error(f"SQL FETCH PAGE ERROR: oracledb err {err}")
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
        except Exception as err:
            logger.error(f"SQL EXECUTE ERROR: {err}")
            conn.rollback()
            return ResultCode.Error.value
        conn.commit()
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
        except Exception as err:
            logger.error(f"SQL EXECUTE ERROR: {err}")
            conn.rollback()
            return ResultCode.Error.value
        conn.commit()
        return ResultCode.Success.value

    def fetch_tables(self) -> list[DatabaseTable]:
        sql = (
            "select t.table_name, c.comments "
            "from user_tables t "
            "left join user_tab_comments c "
            "on t.table_name=c.table_name "
            "and c.table_type='TABLE'"
        )
        data = self.fetchall(sql, [])
        lower_tables = [self._create_database_table(field) for field in data]
        return lower_tables

    def fetch_tables_by_page(self, table_name, current, size) -> Page:
        s = f"where upper(t.table_name) like upper('%{table_name}%')" if table_name else ""
        sql = (
            "select t.table_name, c.comments "
            "from user_tables t "
            "left join user_tab_comments c "
            "on t.table_name=c.table_name "
            "and c.table_type='TABLE' "
            f"{s}"
        )

        page = self.fetchpage(sql, current, size)
        data = page.data
        lower_tables = [self._create_database_table(field) for field in data]
        return Page(lower_tables, page.count)

    def fetch_fields(self, table_name: str) -> list[DatabaseField]:
        sql = (
            "select t1.table_name --表名称\n"
            "      ,t1.comments as table_comment  --表描述\n"
            "      ,t2.column_name  --字段名称\n"
            "      ,t3.comments as column_comment  --字段描述\n"
            "      ,t2.data_type  --字段类型\n"
            "      ,t2.data_length  --字段长度\n"
            "      ,t2.data_precision  --字段精度\n"
            "      ,t2.data_scale  --字段小数范围\n"
            "      ,t2.data_type_new  --组合的字段类型\n"
            "      ,case when t4.table_name is not null then 1 \n"
            "        else 0 end is_pk  --是否为主键\n"
            "      ,t2.is_not_null  --是否为空\n"
            "      ,t2.data_default  --默认值\n"
            "      ,t3.comments\n"
            "  from "
            "  ( --该用户下表名和表描述\n"
            "    select table_name, comments"
            "     from user_tab_comments "
            "    where table_type = 'TABLE'"
            "  ) t1 "
            "  left join "
            "  ( --该用户下表名、字段信息\n"
            "   select table_name  --表名\n"
            "          ,column_name  --字段名\n"
            "          ,data_type  --字段类型\n"
            "          ,data_length  --字段长度\n"
            "          ,data_precision  --字段精度(理解为整数位数)\n"
            "          ,data_scale  --字段小数位位数\n"
            "          ,case when nullable = 'N' then 1 \n"
            "            else 0 end as is_not_null  --是否非空\n"
            "          ,data_default  --默认值\n"
            "          ,case when data_precision is not null and data_scale is not null "
            "           then data_type||'('||data_precision||','||data_scale||')'\n"
            "                when data_precision is not null and data_scale is null "
            "           then data_type||'('||data_precision||')'\n"
            "                when data_precision is null and data_scale is null and data_length is not null "
            "           then data_type||'('||data_length||')'\n"
            "           else data_type end as data_type_new\n"
            "    ,column_id "
            "      from user_tab_columns"
            "  ) t2 "
            "  on t1.table_name = t2.table_name "
            "  left join "
            "  ("
            " --该用户下表名和字段描述\n"
            " select table_name, column_name, comments from user_col_comments"
            "  ) t3 "
            "  on t2.table_name = t3.table_name and t2.column_name = t3.column_name\n"
            "  left join "
            "  ( --该用户下的主键信息\n"
            "   select ucc.table_name, ucc.column_name\n"
            "     from user_cons_columns ucc, user_constraints uc\n"
            "    where uc.constraint_name = ucc.constraint_name\n"
            "       and uc.constraint_type = 'P'\n"
            "  ) t4 "
            "  on t2.table_name = t4.table_name and t2.column_name = t4.column_name\n"
            f" where upper(t2.table_name)='{table_name.upper()}' "
            "  order by t1.table_name, t2.column_id --保证字段顺序和原表字段顺序一致"
        )
        data = self.fetchall(sql, [])
        lower_fields = [self._create_database_field(field) for field in data]
        return lower_fields

    def data_type_standard(self, data_type: str) -> DatabaseFieldStandardDataType:
        if (
            data_type == "VARCHAR2"
            or data_type == "NVARCHAR2"
            or data_type == "CHAR"
            or data_type == "NCHAR"
            or data_type == "LONG"
        ):
            return DatabaseFieldStandardDataType.Varchar2
        elif (
            data_type == "NUMBER" or data_type == "FLOAT" or data_type == "BINARY_FLOAT" or data_type == "BINARY_DOUBLE"
        ):
            return DatabaseFieldStandardDataType.Decimal
        elif data_type.startswith("TIMESTAMP") or data_type == "DATE":
            return DatabaseFieldStandardDataType.DateTime
        elif data_type == "BLOB":
            return DatabaseFieldStandardDataType.Blob
        elif data_type == "CLOB" or data_type == "NCLOB":
            return DatabaseFieldStandardDataType.Clob
        else:
            return DatabaseFieldStandardDataType.Varchar2

    def data_type_format(self, data_type: str):
        if (
            data_type == "VARCHAR2"
            or data_type == "NVARCHAR2"
            or data_type == "CHAR"
            or data_type == "NCHAR"
            or data_type == "LONG"
            or data_type == "BLOB"
            or data_type == "CLOB"
            or data_type == "NCLOB"
        ):
            return UserDataType.Varchar2.value
        elif (
            data_type == "NUMBER"
            or data_type == "INTEGER"
            or data_type == "FLOAT"
            or data_type == "BINARY_FLOAT"
            or data_type == "BINARY_DOUBLE"
        ):
            return UserDataType.Number.value
        elif data_type.startswith("TIMESTAMP") or data_type == "DATE":
            return UserDataType.Date.value
        else:
            return UserDataType.Varchar2.value

    def open(self):
        self.conn = oracledb.connect(user=self.user, password=self.password, **self.connect_params)
        self.conn.autocommit = False
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
