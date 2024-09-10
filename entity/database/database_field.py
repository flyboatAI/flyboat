class DatabaseField:
    def __init__(
        self,
        column_name: str | None,
        data_type: str | None,
        column_comment: str | None = None,
        data_length: int | None = None,
        data_precision: int | None = None,
        data_scale: int | None = None,
        data_type_new: str | None = None,
        is_pk: bool | None = None,
        is_not_null: bool | None = None,
        data_default=None,
    ):
        """
        初始化数据库表字段实体类
        :param column_name: 字段名称
        :param data_type: 字段类型
        :param column_comment: 字段描述
        :param data_length: 字段长度
        :param data_precision: 字段精度
        :param data_scale: 字段小数范围
        :param data_type_new: 组合的字段类型
        :param is_pk: 是否为主键
        :param is_not_null: 是否为空
        :param data_default: 默认值
        """
        self.column_name = column_name
        self.column_comment = column_comment
        self.data_type = data_type
        self.data_length = data_length
        self.data_precision = data_precision
        self.data_scale = data_scale
        self.data_type_new = data_type_new
        self.is_pk = is_pk
        self.is_not_null = is_not_null
        self.data_default = data_default


class DatabaseFieldFormat:
    def __init__(
        self,
        name: str | None,
        data_type: str | None,
        nick_name: str | None = None,
    ):
        """
        初始化数据库表字段（格式化）实体类
        :param name: 字段名称
        :param data_type: 字段类型
        :param nick_name: 字段描述
        """
        self.name = name
        self.nick_name = nick_name
        self.data_type = data_type
