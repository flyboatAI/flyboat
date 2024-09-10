def list_of_dict_to_matrix(data):
    """
    字典数组 to 矩阵
    :param data: 字典数组
    :return: 矩阵
    """
    if not data or not isinstance(data, list):
        return []
    return [list(d.values()) for d in data]


def matrix_to_list_of_dict(matrix, fields):
    """
    矩阵 to 字典数组
    :param matrix: 矩阵
    :param fields: fields 数组
    :return: 字典数组
    """
    if not fields or not matrix:
        return []
    if len(fields) != len(matrix[0]):
        return []
    keys = [list(field["name"]) for field in fields]
    return [dict(zip(keys, vector)) for vector in matrix]
