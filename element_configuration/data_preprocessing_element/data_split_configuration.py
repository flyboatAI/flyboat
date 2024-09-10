import json
import sys

from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from enum_type.split_type import SplitType
from error.element_configuration_config_error import ElementConfigurationConfigError
from helper.data_store_helper import output_port_record_sql
from helper.element_port_helper import get_fields_and_role
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
                "split_type": 0,
                "random_seed": None,
                "train_percent": 80,
                "test_percent": 20,
                "valid_percent": 0,
                "configured": 0
              }
    """
    data_split_sql = (
        f"select split_type, random_seed, "
        f"train_percent, test_percent, valid_percent "
        f"from ml_data_split_element "
        f"where version_id='{version_id}' "
        f"and id='{element_id}' "
        f"and is_enabled={Enabled.Yes.value}"
    )
    data_split_dict = db_helper1.fetchone(data_split_sql, [])
    sql = (
        f"select count(1) as c from ml_data_split_sample_data "
        f"where element_id='{element_id}' and "
        f"version_id='{version_id}'"
    )
    count_dict = db_helper1.fetchone(sql)
    configured = 0
    if count_dict and count_dict.get("c"):
        configured = 1

    if not data_split_dict:
        return execute_success(
            data={
                "split_type": SplitType.TrainTest.value,
                "random_seed": None,
                "train_percent": 80,
                "test_percent": 20,
                "valid_percent": 0,
                "configured": configured,
            }
        )
    data_split_dict["configured"] = configured
    return execute_success(data=data_split_dict)


def configuration(
    version_id,
    element_id,
    user_id,
    prev_node_output_port,
    prev_node_type,
    prev_element_id,
    split_type,
    random_seed,
    train_percent,
    test_percent,
    valid_percent,
):
    """
    用于保存算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param prev_node_output_port: 所连接算子的输出端口
    :param prev_node_type: 所连接算子的类型
    :param prev_element_id: 所连接算子的标识
    :param split_type: 切割类型
    :param random_seed: 随机种子
    :param train_percent: 训练集百分比
    :param test_percent: 测试集百分比
    :param valid_percent: 验证集百分比
    :return: 配置成功/失败
    """
    query_result = get_fields_and_role(prev_node_output_port, prev_node_type, prev_element_id, version_id, db_helper1)
    if query_result.code != ResultCode.Success.value:
        raise ElementConfigurationConfigError(sys._getframe().f_code.co_name, "获取前置算子失败") from None
    fields = query_result.fields
    role = query_result.role
    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    arr_count = 2 if split_type == SplitType.TrainTest.value else 3
    fields_arr = [fields] * arr_count
    role_arr = [role] * arr_count if role is not None else []
    fields_arr_json = json.dumps(fields_arr)
    role_arr_json = json.dumps(role_arr)
    output_configuration_store_sql = output_port_record_sql(version_id, element_id, user_id)

    configuration_sql = generate_configuration_sql(
        version_id,
        element_id,
        user_id,
        split_type,
        random_seed,
        train_percent,
        test_percent,
        valid_percent,
    )
    delete_sql = generate_delete_sample_data_sql(version_id, element_id)
    return db_helper1.execute_arr(
        [output_configuration_store_sql, configuration_sql, delete_sql],
        {0: [fields_arr_json, role_arr_json]},
    )


def generate_configuration_sql(
    version_id,
    element_id,
    user_id,
    split_type,
    random_seed,
    train_percent,
    test_percent,
    valid_percent,
):
    """
    生成插入或更新算子 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param split_type: 切分类型
    :param random_seed: 种子
    :param train_percent: 训练集百分比
    :param test_percent: 测试集百分比
    :param valid_percent: 验证集百分比
    :return:
    """
    now = current_time()
    if random_seed is None:
        random_seed = "null"
    # 插入或更新
    configuration_sql = (
        f"merge into ml_data_split_element t1 "
        f"using (select "
        f"'{element_id}' id, "
        f"'{version_id}' version_id, "
        f"'{split_type}' split_type, "
        f"{random_seed} random_seed, "
        f"{train_percent} train_percent, "
        f"{test_percent} test_percent, "
        f"{valid_percent} valid_percent, "
        f"'{user_id}' user_id from dual) t2 on (t1.id = t2.id "
        f"and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, "
        f"version_id, split_type, random_seed, "
        f"train_percent, test_percent, valid_percent, "
        f"is_enabled) values"
        f"(t2.id, "
        f"t2.user_id, "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, "
        f"t2.split_type, "
        f"t2.random_seed, "
        f"t2.train_percent, t2.test_percent, t2.valid_percent, "
        f"{Enabled.Yes.value}) "
        f"when matched then "
        f"update set t1.split_type=t2.split_type, "
        f"t1.random_seed=t2.random_seed, "
        f"t1.train_percent=t2.train_percent, "
        f"t1.test_percent=t2.test_percent, "
        f"t1.valid_percent=t2.valid_percent, "
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
        f"insert into ml_data_split_element"
        f"(id, create_user, create_time, "
        f"version_id, split_type, random_seed, train_percent, "
        f"test_percent, valid_percent, "
        f"is_enabled)"
        f"values"
        f"('{element_id}', "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"'{SplitType.TrainTest.value}', "
        f"null, "
        f"80, "
        f"20, "
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
        f"insert into ml_data_split_element "
        f"select id, create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', split_type, random_seed, "
        f"train_percent, test_percent, valid_percent, is_enabled "
        f"from ml_data_split_element "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql


def generate_delete_sample_data_sql(version_id, element_id):
    delete_sql = (
        f"delete from ml_data_split_sample_data " f"where element_id='{element_id}' " f"and version_id='{version_id}'"
    )
    return delete_sql


def disable_element_sql_list(version_id, element_id):
    return [generate_delete_sample_data_sql(version_id, element_id)]
