from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from helper.result_helper import execute_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.time_helper import current_time
from helper.warning_helper import UNUSED
from helper.wrapper_helper import valid_init_sql


def get(version_id, element_id):
    """
    获取数据算子弹窗数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :return: {
                "c": 1.0,
                "gamma": "scale",
                "coef0": 0,
                "shrinking": False,
                "tol": 1e-3,
                "max_iter": -1
             }
    """
    algorithm_sql = (
        f"select c, gamma, coef0, shrinking, max_iter, tol "
        f"from ml_svr_regression_element "
        f"where version_id='{version_id}' "
        f"and id='{element_id}' "
        f"and is_enabled={Enabled.Yes.value}"
    )
    algorithm_dict = db_helper1.fetchone(algorithm_sql, [])
    if not algorithm_dict:
        return execute_success(
            data={
                "c": 1.0,
                "gamma": "scale",
                "coef0": 0,
                "shrinking": 0,
                "tol": 1e-3,
                "max_iter": -1,
            }
        )

    return execute_success(data=algorithm_dict)


def configuration(version_id, element_id, user_id, C, gamma, coef0, shrinking, max_iter, tol):
    """
    用于保存算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param C: 惩罚项参数，控制对误差的惩罚程度。较大的C表示模型对误差的容忍度较低。
    :param gamma: 核函数系数。可以是 scale，auto 或一个具体的值。
    :param coef0: 核函数的独立项。通常在多项式核和sigmoid核中使用。
    :param shrinking: 是否使用启发式收缩。如果设置为True，则会在每个步骤中考虑变量的数量，从而加快运算速度。
    :param max_iter: 求解器迭代的最大次数。-1表示没有限制
    :param tol: 允许的误差容忍度。算法会在误差达到该值时停止迭代。
    :return: 配置成功/失败
    """
    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    configuration_sql = generate_configuration_sql(
        version_id, element_id, user_id, C, gamma, coef0, shrinking, max_iter, tol
    )
    return db_helper1.execute_arr([configuration_sql])


def generate_configuration_sql(version_id, element_id, user_id, C, gamma, coef0, shrinking, max_iter, tol):
    """
    生成插入或更新算子 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param C: 惩罚项参数，控制对误差的惩罚程度。较大的C表示模型对误差的容忍度较低。
    :param gamma: 核函数系数。可以是 scale，auto 或一个具体的值。
    :param coef0: 核函数的独立项。通常在多项式核和 sigmoid 核中使用。
    :param shrinking: 是否使用启发式收缩。如果设置为 True，则会在每个步骤中考虑变量的数量，从而加快运算速度。
    :param max_iter: 允许的误差容忍度。算法会在误差达到该值时停止迭代。
    :param tol: 求解器迭代的最大次数。-1表示没有限制。
    :return: SQL
    """
    now = current_time()
    if C is None:
        C = 1.0
    if gamma is None:
        gamma = "scale"
    if coef0 is None:
        coef0 = 0
    if shrinking is None:
        shrinking = 0
    if tol is None:
        tol = 1e-3
    if max_iter is None:
        max_iter = -1

    # 插入或更新
    configuration_sql = (
        f"merge into ml_svr_regression_element t1 "
        f"using (select "
        f"'{element_id}' id, "
        f"'{version_id}' version_id, "
        f"'{user_id}' user_id,"
        f"{C} C, "
        f"'{gamma}' gamma, "
        f"{coef0} coef0, "
        f"{shrinking} shrinking, "
        f"{tol} tol, "
        f"{max_iter} max_iter "
        f"from dual) t2 on (t1.id = t2.id and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, version_id, "
        f"C,gamma,coef0, shrinking, max_iter, tol, is_enabled) "
        f"values"
        f"(t2.id, "
        f"t2.user_id, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, "
        f"t2.C, "
        f"t2.gamma,"
        f"t2.coef0, "
        f"t2.shrinking, "
        f"t2.max_iter,"
        f"t2.tol,"
        f"{Enabled.Yes.value}) "
        f"when matched then "
        f"update set "
        f"t1.C=t2.C, "
        f"t1.max_iter=t2.max_iter, "
        f"t1.gamma=t2.gamma, "
        f"t1.coef0=t2.coef0, "
        f"t1.shrinking=t2.shrinking, "
        f"t1.tol=t2.tol, "
        f"t1.is_enabled={Enabled.Yes.value}"
    )
    return configuration_sql


def init_element(version_id, element_id, user_id, node_type):
    """
    初始化算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param node_type: 节点类型
    :return: 配置成功/失败
    """
    init_sql = generate_init_sql(version_id, element_id, user_id, node_type)
    if init_sql is None:
        return ResultCode.Success.value
    return db_helper1.execute_arr([init_sql])


@valid_init_sql
def generate_init_sql(version_id, element_id, user_id, node_type):
    """
    生成算子配置初始化的 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param node_type: 节点类型
    :return: SQL
    """
    UNUSED(node_type)
    now = current_time()
    # 初始化算子数据
    # noinspection SqlResolve
    init_sql = (
        f"insert into ml_svr_regression_element"
        f"(id, create_user, create_time, "
        f"version_id, "
        f"C, gamma, coef0, shrinking, max_iter, tol, "
        f"is_enabled)"
        f"values"
        f"('{element_id}', "
        f"'{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"0.0, "
        f"'scale', "
        f"0, "
        f"1, "
        f"-1, "
        f"0.0001, "
        f"{Enabled.No.value})"
    )
    return init_sql


def copy_element(new_version_id, old_version_id):
    """
    拷贝算子数据
    :param new_version_id: 新版本标识
    :param old_version_id: 旧版本标识
    :return: 拷贝成功/失败
    """
    copy_sql = generate_copy_sql(new_version_id, old_version_id)
    return db_helper1.execute(copy_sql)


def generate_copy_sql(new_version_id, old_version_id):
    """
    生成算子拷贝的 SQL 语句
    :param new_version_id: 新版本标识
    :param old_version_id: 旧版本标识
    :return: SQL
    """
    now = current_time()
    # 拷贝算子数据
    copy_sql = (
        f"insert into ml_svr_regression_element "
        f"select id, create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', C,gamma,coef0, shrinking,max_iter, tol , is_enabled "
        f"from ml_svr_regression_element "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql
