import json

from enum_type.user_data_type import UserDataType
from helper.data_store_helper import output_port_record_sql
from helper.fields_helper import generate_fields
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED


def init_element(version_id, element_id, user_id, node_type):
    """
    初始化算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param node_type: 节点类型
    :return: 配置成功/失败
    """
    UNUSED(node_type)
    output_configuration_store_sql = output_port_record_sql(version_id, element_id, user_id)

    kmo_fields = [
        generate_fields("KMO", nick_name="KMO", data_type=UserDataType.Number.value),
        generate_fields(
            "Bartlett 球形度检验 近似卡方",
            nick_name="Bartlett 球形度检验 近似卡方",
            data_type=UserDataType.Number.value,
        ),
        generate_fields(
            "Bartlett 球形度检验 P值",
            nick_name="Bartlett 球形度检验 P值",
            data_type=UserDataType.Number.value,
        ),
    ]
    variance_table_fields = [
        generate_fields("特征根", nick_name="特征根", data_type=UserDataType.Number.value),
        generate_fields("方差解释率", nick_name="方差解释率", data_type=UserDataType.Number.value),
        generate_fields(
            "累积方差解释率",
            nick_name="累积方差解释率",
            data_type=UserDataType.Number.value,
        ),
        generate_fields(
            "旋转前主成分_特征根",
            nick_name="旋转前主成分_特征根",
            data_type=UserDataType.Number.value,
        ),
        generate_fields(
            "旋转前主成分_方差解释率",
            nick_name="旋转前主成分_方差解释率",
            data_type=UserDataType.Number.value,
        ),
        generate_fields(
            "旋转前主成分_累积方差解释率",
            nick_name="旋转前主成分_累积方差解释率",
            data_type=UserDataType.Number.value,
        ),
        generate_fields(
            "旋转后主成分_特征根",
            nick_name="旋转后主成分_特征根",
            data_type=UserDataType.Number.value,
        ),
        generate_fields(
            "旋转后主成分_方差解释率",
            nick_name="旋转后主成分_方差解释率",
            data_type=UserDataType.Number.value,
        ),
        generate_fields(
            "旋转后主成分_累积方差解释率",
            nick_name="旋转后主成分_累积方差解释率",
            data_type=UserDataType.Number.value,
        ),
    ]
    loading_table_fields = [
        generate_fields("特征", nick_name="特征", data_type=UserDataType.Varchar2.value),
        generate_fields("主成分1", nick_name="主成分1", data_type=UserDataType.Number.value),
        generate_fields(
            "共同度（公因子方差）",
            nick_name="共同度（公因子方差）",
            data_type=UserDataType.Number.value,
        ),
    ]
    liner_table_fields = [
        generate_fields("特征", nick_name="特征", data_type=UserDataType.Varchar2.value),
        generate_fields("主成分1", nick_name="主成分1", data_type=UserDataType.Number.value),
        generate_fields(
            "综合得分系数",
            nick_name="综合得分系数",
            data_type=UserDataType.Number.value,
        ),
        generate_fields("权重(%)", nick_name="权重(%)", data_type=UserDataType.Number.value),
    ]
    component_table_fields = [
        generate_fields("特征", nick_name="特征", data_type=UserDataType.Varchar2.value),
        generate_fields("主成分1", nick_name="主成分1", data_type=UserDataType.Number.value),
    ]
    eigenvalue_fields = [
        generate_fields("x", nick_name="成分", data_type=UserDataType.Varchar2.value),
        generate_fields("y", nick_name="特征根", data_type=UserDataType.Number.value),
    ]
    fields_arr = [
        kmo_fields,
        variance_table_fields,
        loading_table_fields,
        liner_table_fields,
        component_table_fields,
        eigenvalue_fields,
    ]
    fields_arr_json = json.dumps(fields_arr)
    role_arr_json = json.dumps([])
    return db_helper1.execute_arr([output_configuration_store_sql], {0: [fields_arr_json, role_arr_json]})
