def dict_key_rename(arr, prefix, exclude_key):
    """
    字典 key 改名
    :param arr: 需要修改的字典数组
    :param prefix: key 要追加的前缀
    :param exclude_key: 排除要追加前缀的 key
    :return: 改名后的字典数组
    """
    if not prefix or not arr:
        return None
    n_a = []
    for d in arr:
        n_d = dict()
        for k in d.keys():
            if exclude_key == k:
                n_k = k
            else:
                n_k = prefix + k
            n_d[n_k] = d[k]
        n_a.append(n_d)
    return n_a


def field_name_add_prefix(arr, prefix, exclude_key):
    """
    字典数组种每个字典的 name 字段值改名
    :param exclude_key: 不需要修改的 key
    :param arr: 需要修改的字典数组
    :param prefix: name 要追加的前缀
    :return: 改名后的字典数组
    """
    if arr is None or not prefix:
        return None
    for m in arr:
        if m["name"] != exclude_key:
            m["name"] = prefix + m["name"]
    return arr


def dict_rename(arr, element_id):
    """
    字典 key 改名
    :param arr: 需要修改的字典数组
    :param element_id: key 要追加的 id
    :return: 改名后的字典数组
    """
    if not element_id or not arr:
        return None
    n_a = []
    for d in arr:
        n_d = dict()
        for k in d.keys():
            n_k = k + f".{element_id}"
            n_d[n_k] = d[k]
        n_a.append(n_d)
    return n_a


def field_rename(arr, element_id):
    """
    字典数组种每个字典的 name 字段值改名
    :param arr: 需要修改的字典数组
    :param element_id: name 要追加的 id
    :return: 改名后的字典数组
    """
    if arr is None or not element_id:
        return None
    for m in arr:
        m["name"] = m["name"] + f".{element_id}"
    return arr


def scaler_rename(scaler, element_id):
    """
    数据连接后 scaler 改 key 名称
    :param scaler: 最大最小值字典
    :param element_id: name 要追加的 id
    :return: 改名后的字典
    """
    rename_scaler = dict()
    for k in scaler.keys():
        n_k = k + f".{element_id}"
        rename_scaler[n_k] = scaler[k]
    return rename_scaler


def rename(arr, name_dict_arr):
    """
    字典数组种每个字典的 name 字段值改名
    :param arr: 需要修改的字典数组
    :param name_dict_arr:
    :return: 改名后的字典数组
    """
    if arr is None:
        return None
    rename_arr = []
    for m in arr:
        rename_dict = {}
        for i, k in enumerate(m):
            rename_dict[name_dict_arr[i]["name"]] = m[k]
        rename_arr.append(rename_dict)
    return rename_arr
