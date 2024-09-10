import numpy
import pandas as pd

from enum_type.role_type import RoleType
from enum_type.user_data_type import UserDataType
from helper.fields_helper import generate_fields


def matrix_data_format_conversion(conversion_data):
    """
    pca, 因子, critic, topsis, 灰色分析算子的矩阵变换
    :param conversion_data:
    :return:
    """
    role = []
    for col in conversion_data.columns:
        if col == "特征" or col == "项":
            role.append(generate_fields(col, data_type=UserDataType.Varchar2.value))
        else:
            role.append(generate_fields(col, data_type=UserDataType.Number.value))
    dict_list = conversion_data.to_dict(orient="records")
    return dict_list, role


def data_format_conversion(data, role_settings):
    """
    数据格式化
    :param data:
    :param role_settings:
    :return:
    """
    # 拆分特征与标签列
    x_role_arr = [x.get("name") for x in role_settings if x.get("role_type") == RoleType.X.value]
    y_role_arr = [x.get("name") for x in role_settings if x.get("role_type") == RoleType.Y.value]
    # 根据标签获取数据
    train_data = pd.DataFrame(data)

    return x_role_arr, y_role_arr, train_data


def dict_array_build(matrix, fields):
    """
    将 Matrix 根据 [{"name": "", "nick_name": "", "data_type": "NUMBER"}] 转为字典数组
    :param matrix: 二维数组数据
    :param fields: 字段信息
    :return: 字典数组
    """
    arr = []
    if matrix is None or fields is None:
        return arr

    names = [field.get("name") for field in fields if field.get("name") is not None]
    if not names:
        return arr

    for vector in matrix:
        if isinstance(vector, list):
            prop = dict(zip(names, vector))
        else:
            prop = dict(zip(names, [vector]))
        arr.append(prop)

    return arr


def prediction_matrix_build(data):
    """
    将预测数据(字典数组)转为 numpy Matrix
    :param data:
    :return:
    """
    x_matrix = prediction_data_role_handle(data)
    return numpy.array(x_matrix)


def prediction_data_role_handle(data):
    """
    将预测数据(字典数组)转为二维数组
    :param data: 字典数组
    :return: 二维数组
    """
    x_matrix = []
    if not data:
        return x_matrix
    keys = data[0].keys()
    for d in data:
        x_arr = []
        for x_key in keys:
            x = d[x_key]
            x_arr.append(x)
        x_matrix.append(x_arr)

    return x_matrix


def train_matrix_build(data, role_settings):
    """
    根据角色信息将字典数组转为 Matrix
    :param data:
    :param role_settings:
    :return:
    """
    x_role_arr = [x.get("name") for x in role_settings if x.get("role_type") == RoleType.X.value]
    y_role_arr = [x.get("name") for x in role_settings if x.get("role_type") == RoleType.Y.value]

    x_matrix, y_matrix = train_data_role_handle(data, x_role_arr, y_role_arr)
    return numpy.array(x_matrix), numpy.array(y_matrix)


def get_x_count(role_settings):
    return len([x.get("name") for x in role_settings if x.get("role_type") == RoleType.X.value])


def get_y_count(role_settings):
    return len([x.get("name") for x in role_settings if x.get("role_type") == RoleType.Y.value])


def train_data_role_handle(data, x_role_arr, y_role_arr):
    """
    根据自变量因变量数组, 处理数据
    :param data: 初始数据
    :param x_role_arr: 自变量数组
    :param y_role_arr: 因变量数组
    :return: 处理后的自变量二维数组、因变量二维数组元组
    """
    x_matrix = []
    y_matrix = []

    for d in data:
        x_arr = []
        y_arr = []
        for x_role in x_role_arr:
            x = d[x_role]
            x_arr.append(x)
        for y_role in y_role_arr:
            y = d[y_role]
            y_arr.append(y)
        x_matrix.append(x_arr)
        y_matrix.append(y_arr)

    return x_matrix, y_matrix
