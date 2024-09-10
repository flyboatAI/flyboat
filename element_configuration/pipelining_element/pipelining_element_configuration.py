from enum_type.enabled import Enabled
from enum_type.result_code import ResultCode
from error.init_error import InitError
from helper.sql_helper.init_sql_helper import db_helper1
from helper.time_helper import current_time
from helper.warning_helper import UNUSED
from helper.wrapper_helper import valid_init_sql


def init_element(version_id, element_id, user_id, node_type, **kwargs):
    """
    初始化算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param node_type: 节点类型
    :return: 配置成功/失败
    """
    init_sql = generate_init_sql(version_id, element_id, user_id, node_type, **kwargs)
    if init_sql is None:
        return ResultCode.Success.value
    return db_helper1.execute_arr([init_sql])


@valid_init_sql
def generate_init_sql(version_id, element_id, user_id, node_type, **kwargs):
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
    pipelining_element_id = kwargs.get("pipelining_element_id", None)
    if pipelining_element_id is None:
        raise InitError("pipelining_element_id 参数未配置") from None

    # 初始化算子数据
    # noinspection SqlResolve
    init_sql = (
        f"insert into ml_pipelining_element"
        f"(id, create_user, create_time, "
        f"version_id, pipelining_element_id, "
        f"is_enabled) "
        f"select "
        f"'{element_id}', "
        f"'{user_id}', to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"'{pipelining_element_id}', "
        f"{Enabled.Yes.value} "
        f"from dual"
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
    # 拷贝算子数据
    copy_sql = (
        f"insert into ml_pipelining_element "
        f"select id, create_user, to_date('{current_time()}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', pipelining_element_id, is_enabled "
        f"from ml_pipelining_element "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql
