import copy


def generate_fields(name, nick_name=None, data_type=None):
    """
    生成字段字典数组
    :param name: 字段名
    :param nick_name: 别名
    :param data_type: 数据类型
    :return: 字典数组
    """
    return {
        "name": name,
        "nick_name": nick_name if nick_name else name,
        "data_type": data_type,
    }


def generate_default_column(count):
    fields = []
    for i in range(count):
        fields.append(generate_fields(f"column_{i}"))
    return fields


def merge_fields(origin_fields, added_fields, del_key="expression"):
    handle_fields = list(filter(lambda x: x.pop(del_key, None) or True, copy.deepcopy(added_fields)))
    copy_fields = copy.deepcopy(origin_fields)
    copy_fields.extend(handle_fields)
    return copy_fields
