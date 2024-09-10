from enum import Enum


class UserDataType(str, Enum):
    Varchar2 = "VARCHAR2"
    Date = "DATE"
    Number = "NUMBER"

    @classmethod
    def convert_data_type_to_instance_type(cls, data_type):
        data_type_to_instance_type_dict = {
            UserDataType.Varchar2.value: str,
            UserDataType.Date.value: "datetime64[ns]",
            UserDataType.Number.value: float,
        }
        return data_type_to_instance_type_dict.get(data_type, str)
