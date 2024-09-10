from enum_type.database_field_data_type import DatabaseFieldStandardDataType


class DatabaseFieldStandard:
    def __init__(
        self,
        column_name: str | None,
        data_type: DatabaseFieldStandardDataType | None,
        column_comment: str | None = None,
        data_length: int | None = None,
        data_precision: int | None = None,
        data_scale: int | None = None,
        is_pk: bool | None = None,
        is_not_null: bool | None = None,
    ):
        """
        数据库表字段实体类, 用于动态建表
        :param column_name: 字段名称
        :param data_type: 标准字段类型
        :param column_comment: 字段描述
        :param data_length: 字段长度
        :param data_precision: 字段精度
        :param data_scale: 字段小数范围
        :param is_pk: 是否为主键
        :param is_not_null: 是否为空
        """
        self.column_name = column_name
        self.column_comment = column_comment
        self.data_type = data_type
        self.data_length = data_length
        self.data_precision = data_precision
        self.data_scale = data_scale
        self.is_pk = is_pk
        self.is_not_null = is_not_null
