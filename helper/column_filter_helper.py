def column_filter(data, column_fields):
    """
    过滤列后的数组
    :param data: 数据数组
    :param column_fields: 过滤字段字典数组
    :return: 过滤后的数据数组
    """
    if column_fields is None or data is None:
        return []
    column_fields_name_arr = [x["name"] for x in column_fields]
    n_arr = []
    for d in data:
        n_dict = {}
        for key in d.keys():
            if key in column_fields_name_arr:
                n_dict[key] = d[key]
        n_arr.append(n_dict)

    return n_arr


def role_filter(role, filter_fields):
    """
    过滤列后的数组
    :param role: 角色数据
    :param filter_fields: 过滤字段字典数组
    :return: 过滤后的角色数组
    """
    column_fields_name_arr = [x.get("name") for x in filter_fields]
    n_arr = []
    for r in role:
        name = r.get("name")
        if not name:
            return n_arr
        if name in column_fields_name_arr:
            n_arr.append(r)
    return n_arr
