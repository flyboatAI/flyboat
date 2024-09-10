from itertools import groupby
from operator import itemgetter


def inner_join(data_0, data_1, field_0, field_1, fields):
    """
    字典数组内连接
    :param data_0: 端口0的字典数组
    :param data_1: 端口1的字典数组
    :param field_0: 端口0的字典数组连接 key
    :param field_1: 端口1的字典数组连接 key
    :param fields: 过滤字段数组
    :return:
    """
    arr = []
    if not data_1 or not data_0 or not fields:
        return arr

    field_0_name = field_0["name"]
    field_1_name = field_1["name"]

    for d0 in data_0:
        for d1 in data_1:
            if field_0_name in d0 and field_1_name in d1 and d0[field_0_name] == d1[field_1_name]:
                x = {}
                for field_name in fields:
                    if field_name in d0:
                        x[field_name] = d0[field_name]
                    elif field_name in d1:
                        x[field_name] = d1[field_name]
                if x:
                    arr.append(x)

    return arr


def left_join(data_0, data_1, field_0, field_1, fields):
    """
    字典数组左连接
    :param data_0: 端口0的字典数组
    :param data_1: 端口1的字典数组
    :param field_0: 端口0的字典数组连接 key
    :param field_1: 端口1的字典数组连接 key
    :param fields: 过滤字段数组
    :return:
    """
    arr = []
    if not data_0 or not fields:
        return arr

    field_0_name = field_0["name"]
    field_1_name = field_1["name"]

    for d0 in data_0:
        joined = False
        for d1 in data_1:
            x = {}
            if field_0_name in d0 and field_1_name in d1 and d0[field_0_name] == d1[field_1_name]:
                joined = True
                for field_name in fields:
                    if field_name in d0:
                        x[field_name] = d0[field_name]
                    elif field_name in d1:
                        x[field_name] = d1[field_name]
                if x:
                    arr.append(x)
        if not joined and field_0_name in d0:
            x = {}
            for field_name in fields:
                if field_name in d0:
                    x[field_name] = d0[field_name]
                else:
                    x[field_name] = None
            if x:
                arr.append(x)
    return arr


def full_join(data_0, data_1, field_0, field_1, fields):
    """
    字典数组全连接
    :param data_0: 端口0的字典数组
    :param data_1: 端口1的字典数组
    :param field_0: 端口0的字典数组连接 key
    :param field_1: 端口1的字典数组连接 key
    :param fields: 过滤字段数组
    :return:
    """
    arr = []
    if not fields:
        return arr

    arr = left_join(data_0, data_1, field_0, field_1, fields)

    field_0_name = field_0["name"]
    field_1_name = field_1["name"]

    for d1 in data_1:
        joined = False
        for d0 in data_0:
            if field_0_name in d0 and field_1_name in d1 and d0[field_0_name] == d1[field_1_name]:
                joined = True
                break
        if not joined:
            x = {}
            for field_name in fields:
                if field_name in d1:
                    x[field_name] = d1[field_name]
                else:
                    x[field_name] = None
            if x:
                arr.append(x)

    return arr


def distinct_list_of_dict(list_of_dict, key):
    key = itemgetter(key)
    list_of_dict = sorted(list_of_dict, key=key)
    return [next(v) for _, v in groupby(list_of_dict, key=key)]
