from enum import Enum

from enum_type.user_data_type import UserDataType


class DatabaseFieldStandardDataType(str, Enum):
    Varchar2 = "VARCHAR2"
    Date = "DATE"
    DateTime = "DATETIME"
    Time = "TIME"
    Int = "INT"
    Decimal = "DECIMAL"
    Blob = "BLOB"
    Clob = "CLOB"

    @classmethod
    def get_standard_data_type_from_user_data_type(cls, user_data_type: str):
        """
        根据用户数据类型返回标准数据类型
        :param user_data_type: 用户数据类型
        :return: 标准数据类型
        """
        if user_data_type == UserDataType.Varchar2.value:
            return cls.Varchar2
        elif user_data_type == UserDataType.Number.value:
            return cls.Decimal
        elif user_data_type == UserDataType.Date.value:
            return cls.DateTime
        return cls.Varchar2
