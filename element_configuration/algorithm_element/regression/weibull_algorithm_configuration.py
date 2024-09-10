import json

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
    # calc_method
    # is_multimodal
    # number_of_peaks
    # number_of_peaks_region
    # m_a_list
    # optimization_method
    # lr
    # epoch

    algorithm_sql = (
        f"select calc_method, is_multimodal, number_of_peaks, "
        f"number_of_peaks_region, m_a_list, "
        f"optimization_method, lr, epoch "
        f"from ml_weibull_element "
        f"where version_id='{version_id}' "
        f"and id='{element_id}' "
        f"and is_enabled={Enabled.Yes.value}"
    )
    algorithm_dict = db_helper1.fetchone(algorithm_sql, [])
    if not algorithm_dict:
        return execute_success(
            data={
                "calc_method": "linear_regression",
                "is_multimodal": 0,
                "number_of_peaks": 1,
                "number_of_peaks_region": [[]],
                "m_a_list": [[4, 1]],
                "optimization_method": "adam",
                "lr": 1e-3,
                "epoch": 1000,
            }
        )
    if algorithm_dict["number_of_peaks_region"] is not None:
        algorithm_dict["number_of_peaks_region"] = json.loads(algorithm_dict["number_of_peaks_region"])
    if algorithm_dict["m_a_list"] is not None:
        algorithm_dict["m_a_list"] = json.loads(algorithm_dict["m_a_list"])
    return execute_success(data=algorithm_dict)


def configuration(
    version_id,
    element_id,
    user_id,
    calc_method,
    is_multimodal,
    number_of_peaks,
    number_of_peaks_region,
    m_a_list,
    optimization_method,
    lr,
    epoch,
):
    """
    用于保存算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param calc_method: 计算方法可选参数 ['linear_regression', 'bp_regression']
    :param is_multimodal: 是否为多峰威布尔分布,默认False，单峰
    :param number_of_peaks: 如果是多峰威布尔分布，峰的数量
    :param number_of_peaks_region: 如果是多峰威布尔分布，每个峰的起始位置和节数位置
    :param m_a_list: 如果是多峰威布尔分布，每个峰的预估m和a
    :param optimization_method: 如果使用bp回归的话，使用的学习率优化方法['adam','sgd']
    :param lr: 学习率
    :param epoch: 在使用bp回归时，迭代次数
    :return: 配置成功/失败
    """
    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    configuration_sql = generate_configuration_sql(
        version_id,
        element_id,
        user_id,
        calc_method,
        is_multimodal,
        number_of_peaks,
        optimization_method,
        lr,
        epoch,
    )
    if number_of_peaks_region is None:
        number_of_peaks_region = [[]]

    number_of_peaks_region_json = json.dumps(number_of_peaks_region)
    m_a_list_json = json.dumps(m_a_list)
    return db_helper1.execute_arr(
        [configuration_sql],
        {
            0: [
                number_of_peaks_region_json,
                m_a_list_json,
                number_of_peaks_region_json,
                m_a_list_json,
            ]
        },
    )


def generate_configuration_sql(
    version_id,
    element_id,
    user_id,
    calc_method,
    is_multimodal,
    number_of_peaks,
    optimization_method,
    lr,
    epoch,
):
    """
    生成插入或更新算子 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param calc_method: 计算方法可选参数 ['linear_regression', 'bp_regression']
    :param is_multimodal: 是否为多峰威布尔分布,默认False，单峰
    :param number_of_peaks: 如果是多峰威布尔分布，峰的数量
    :param optimization_method: 如果使用bp回归的话，使用的学习率优化方法['adam','sgd']
    :param lr: 学习率
    :param epoch: 在使用bp回归时，迭代次数
    :return: SQL
    """
    now = current_time()
    if epoch is None:
        epoch = 1000
    if calc_method is None:
        calc_method = "linear_regression"
    if is_multimodal is None:
        is_multimodal = 0
    if number_of_peaks is None:
        number_of_peaks = 1
    if optimization_method is None:
        optimization_method = "sgd"
    if lr is None:
        lr = 1e-3
    # 插入或更新
    configuration_sql = (
        f"merge into ml_weibull_element t1 "
        f"using (select "
        f"'{element_id}' id, "
        f"'{version_id}' version_id, "
        f"'{user_id}' user_id,"
        f"'{calc_method}' calc_method, "
        f"{is_multimodal} is_multimodal, "
        f"{number_of_peaks} number_of_peaks, "
        f"'{optimization_method}' optimization_method, "
        f"{lr} lr, "
        f"{epoch} epoch "
        f"from dual) t2 on (t1.id = t2.id and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, version_id, "
        f"calc_method, is_multimodal, number_of_peaks, "
        f"number_of_peaks_region, "
        f"m_a_list, "
        f"optimization_method, lr, epoch, "
        f"is_enabled) "
        f"values"
        f"(t2.id, "
        f"t2.user_id, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, "
        f"t2.calc_method, "
        f"t2.is_multimodal,"
        f"t2.number_of_peaks,"
        f":1,"
        f":2,"
        f"t2.optimization_method,"
        f"t2.lr,"
        f"t2.epoch,"
        f"{Enabled.Yes.value}) "
        f"when matched then "
        f"update set "
        f"t1.calc_method=t2.calc_method, "
        f"t1.is_multimodal=t2.is_multimodal, "
        f"t1.number_of_peaks=t2.number_of_peaks, "
        f"t1.number_of_peaks_region=:3, "
        f"t1.m_a_list=:4, "
        f"t1.optimization_method=t2.optimization_method, "
        f"t1.lr=t2.lr, "
        f"t1.epoch=t2.epoch, "
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
        f"insert into ml_weibull_element"
        f"(id, create_user, create_time, "
        f"version_id, calc_method, is_multimodal, number_of_peaks,"
        f"number_of_peaks_region,m_a_list,optimization_method,lr,epoch, "
        f"is_enabled)"
        f"values"
        f"('{element_id}', "
        f"'{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"'linear_regression', "
        f"0, "
        f"1, "
        f"'[[]]', "
        f"'[[4, 1]]', "
        f"'sgd', "
        f"0.0001, "
        f"1000, "
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
        f"insert into ml_weibull_element "
        f"select id, create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', calc_method, is_multimodal, number_of_peaks, "
        f"number_of_peaks_region, "
        f"m_a_list, optimization_method, "
        f"lr, epoch, is_enabled "
        f"from ml_weibull_element "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql
