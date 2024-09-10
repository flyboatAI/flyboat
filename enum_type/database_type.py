import importlib
from enum import Enum
from typing import Type

from helper.sql_helper.abstract_sql_helper import AbstractTransientSqlHelper


class DataBaseType(str, Enum):
    # 新增类型时, 枚举值与连接字符串第二个标识相同, 如连接字符串为 "jdbc:oracle:xxx", 则数据库类型枚举值为 oracle
    Oracle = "oracle"
    Dm = "dm"
    Mysql = "mysql"
    Postgresql = "postgresql"
    Sqlserver = "sqlserver"

    @staticmethod
    def _get_dbtype_by_connection_str(connection_str: str) -> str | None:
        """
        根据连接字符串返回数据库类型
        :param connection_str: 连接字符串
        :return: DataBaseType 数据库类型
        """
        # 常见的连接字符串
        # "jdbc:mysql://localhost:3306/mydatabase",
        # "jdbc:postgresql://localhost:5432/mydatabase",
        # "jdbc:sqlserver://localhost:1433;databaseName=mydatabase",
        # "jdbc:oracle:thin:@localhost:1521:orcl",
        # "jdbc:sqlite:C:/sqlite/db/chinook.db",
        # "jdbc:db2://localhost:50000/mydatabase",
        # "jdbc:mariadb://localhost:3306/mydatabase",
        # "jdbc:sybase:Tds:localhost:5007/mydatabase",
        # "jdbc:h2:tcp://localhost/~/test"
        # "jdbc:dm://localhost:5236/test?zeroDateTimeBehavior=convertToNull&amp;useUnicode=true&amp;characterEncoding=utf-8"
        connection_str_list = connection_str.split(":")
        if not connection_str_list or connection_str_list[0] != "jdbc" or len(connection_str_list) < 2:
            return None
        valid = connection_str_list[1] in (db_type for db_type in DataBaseType)
        return connection_str_list[1] if valid else None

    @classmethod
    def get_transient_class_by_connection_str(cls, connection_str: str) -> Type[AbstractTransientSqlHelper] | None:
        """
        根据连接字符串返回临时数据库对象类
        :param connection_str: 连接字符串
        :return: 数据库对象类
        """
        db_type = cls._get_dbtype_by_connection_str(connection_str)
        if db_type and db_type in DataBaseType._value2member_map_:
            module = importlib.import_module(f"helper.sql_helper.{db_type}_sql_helper")
            class_name = f"Transient{db_type.capitalize()}SqlHelper"
            sql_helper = getattr(module, class_name)
            return sql_helper
        return None
