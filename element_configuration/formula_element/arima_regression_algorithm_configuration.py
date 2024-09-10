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
    :return:
    """
    algorithm_sql = (
        f"select p, d, q "
        f"from ml_arima_regression_element "
        f"where version_id='{version_id}' "
        f"and id='{element_id}' "
        f"and is_enabled={Enabled.Yes.value}"
    )
    algorithm_dict = db_helper1.fetchone(algorithm_sql, [])
    if not algorithm_dict:
        return execute_success(data={"p": 2, "d": 2, "q": 2})

    return execute_success(data=algorithm_dict)


def configuration(version_id, element_id, user_id, p, d, q):
    """
    用于保存算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param p: 自回归（AR）的阶数，表示模型考虑过去的几个时刻的数据
    :param d: 差分次数，用于使时间序列稳定，通常通过观察趋势和季节性来确定
    :param q: 移动平均（MA）的阶数，表示模型考虑前几个误差的滞后值
    :return: 配置成功/失败
    """
    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    configuration_sql = generate_configuration_sql(version_id, element_id, user_id, p, d, q)
    return db_helper1.execute_arr([configuration_sql])


def generate_configuration_sql(version_id, element_id, user_id, p, d, q):
    """
    生成插入或更新算子 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param p: 自回归（AR）的阶数，表示模型考虑过去的几个时刻的数据
    :param d: 差分次数，用于使时间序列稳定，通常通过观察趋势和季节性来确定
    :param q: 移动平均（MA）的阶数，表示模型考虑前几个误差的滞后值
    :return: SQL
    """
    now = current_time()
    if p is None:
        p = 2
    if d is None:
        d = 2
    if q is None:
        q = 2
    # 插入或更新
    configuration_sql = (
        f"merge into ml_arima_regression_element t1 "
        f"using (select "
        f"'{element_id}' id, "
        f"'{version_id}' version_id, "
        f"'{user_id}' user_id,"
        f"{p} p, "
        f"{d} d, "
        f"{q} q, "
        f"{0} step "
        f"from dual) t2 on (t1.id = t2.id and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, version_id, "
        f"p, d, q, step, is_enabled) "
        f"values"
        f"(t2.id, "
        f"t2.user_id, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, "
        f"t2.p, "
        f"t2.d, "
        f"t2.q, "
        f"t2.step,"
        f"{Enabled.Yes.value}) "
        f"when matched then "
        f"update set "
        f"t1.p=t2.p, "
        f"t1.d=t2.d, "
        f"t1.q=t2.q, "
        f"t1.step=0,"
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
        f"insert into ml_arima_regression_element"
        f"(id, create_user, create_time, version_id,"
        f"p, d, q, step, "
        f"is_enabled)"
        f"values"
        f"('{element_id}', "
        f"'{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"2, "
        f"2, "
        f"2, "
        f"0, "
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
        f"insert into ml_arima_regression_element "
        f"select id, create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', p, d, q, step, "
        f"is_enabled "
        f"from ml_arima_regression_element "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql
