from dateutil.parser import parse

from enum_type.user_data_type import UserDataType


def match_fields(data, fields, field_name="name"):
    """
    校验数据和字段字典是否匹配
    :param data: 数据
    :param fields: 字段字典数组
    :param field_name: 默认匹配名称
    :return: 是否匹配
    """
    if not data or not fields or not isinstance(data, list) or not isinstance(fields, list):
        return False

    data_field_names = list(data[0].keys())
    field_names = set([field[field_name] for field in fields])
    return field_names.issubset(data_field_names)


def reorder_key_data(key_data, fields, field_name="name"):
    """
    处理接口传递的字典 Key 乱序问题
    :param key_data: 接口传递的数据
    :param fields: 正确字典顺序
    :param field_name: 默认匹配名称
    :return:
    """
    field_names = [field[field_name] for field in fields]
    field_name_and_type_dict = {field[field_name]: field["data_type"] for field in fields}
    reorder_data = []
    for data in key_data:
        temp_data = {}
        for field_name in field_names:
            if field_name_and_type_dict[field_name] == UserDataType.Date.value:
                temp_data[field_name] = parse(data[field_name])
            else:
                temp_data[field_name] = data[field_name]
        reorder_data.append(temp_data)

    return reorder_data
