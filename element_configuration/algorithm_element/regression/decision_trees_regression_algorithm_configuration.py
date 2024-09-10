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
    :return:{
                'max_depth': 6,
                'min_samples_split': 2,
                'min_samples_leaf': 4,
                'max_leaf_nodes': 5,
                'max_features': 'log2',
                'criterion': 'squared_error'
            }
    """
    algorithm_sql = (
        f"select max_depth, min_samples_split, min_samples_leaf, "
        f"max_leaf_nodes, max_features, criterion "
        f"from ml_decision_trees_regression_element "
        f"where version_id='{version_id}' "
        f"and id='{element_id}' "
        f"and is_enabled={Enabled.Yes.value}"
    )
    algorithm_dict = db_helper1.fetchone(algorithm_sql, [])
    if not algorithm_dict:
        return execute_success(
            data={
                "max_depth": 6,
                "min_samples_split": 2,
                "min_samples_leaf": 4,
                "max_leaf_nodes": 5,
                "max_features": "log2",
                "criterion": "squared_error",
            }
        )

    return execute_success(data=algorithm_dict)


def configuration(
    version_id,
    element_id,
    user_id,
    max_depth,
    min_samples_split,
    min_samples_leaf,
    max_leaf_nodes,
    max_features,
    criterion,
):
    """
    用于保存算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param max_depth: 控制树的最大深度
    :param min_samples_split: 节点分裂所需的最小样本数
    :param min_samples_leaf: 叶子节点所需的最小样本数
    :param max_leaf_nodes: 最大叶子节点数量
    :param max_features: 分裂时考虑的最大特征数量（以2为底的对数）
    :param criterion: 衡量分裂质量的指标，均方误差
    :return: 配置成功/失败
    """
    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    configuration_sql = generate_configuration_sql(
        version_id,
        element_id,
        user_id,
        max_depth,
        min_samples_split,
        min_samples_leaf,
        max_leaf_nodes,
        max_features,
        criterion,
    )
    return db_helper1.execute_arr([configuration_sql])


def generate_configuration_sql(
    version_id,
    element_id,
    user_id,
    max_depth,
    min_samples_split,
    min_samples_leaf,
    max_leaf_nodes,
    max_features,
    criterion,
):
    """
    生成插入或更新算子 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param max_depth: 控制树的最大深度
    :param min_samples_split: 节点分裂所需的最小样本数
    :param min_samples_leaf: 叶子节点所需的最小样本数
    :param max_leaf_nodes: 最大叶子节点数量
    :param max_features: 分裂时考虑的最大特征数量（以2为底的对数）
    :param criterion: 衡量分裂质量的指标，均方误差
    :return: SQL
    """
    now = current_time()
    if max_depth is None:
        max_depth = 6
    if min_samples_split is None:
        min_samples_split = 2
    if min_samples_leaf is None:
        min_samples_leaf = 4
    if max_leaf_nodes is None:
        max_leaf_nodes = 5
    if max_features is None:
        max_features = "log2"
    if criterion is None:
        criterion = "squared_error"
    # 插入或更新
    configuration_sql = (
        f"merge into ml_decision_trees_regression_element t1 "
        f"using (select "
        f"'{element_id}' id, "
        f"'{version_id}' version_id, "
        f"'{user_id}' user_id,"
        f"{max_depth} max_depth, "
        f"{min_samples_split} min_samples_split, "
        f"{min_samples_leaf} min_samples_leaf, "
        f"{max_leaf_nodes} max_leaf_nodes, "
        f"'{max_features}' max_features, "
        f"'{criterion}' criterion "
        f"from dual) t2 on (t1.id = t2.id and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, version_id, "
        f"max_depth, min_samples_split, min_samples_leaf,max_leaf_nodes, "
        f"max_features, criterion, is_enabled) "
        f"values"
        f"(t2.id, "
        f"t2.user_id, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, "
        f"t2.max_depth, "
        f"t2.min_samples_split,"
        f"t2.min_samples_leaf,"
        f"t2.max_leaf_nodes,"
        f"t2.max_features,"
        f"t2.criterion,"
        f"{Enabled.Yes.value}) "
        f"when matched then "
        f"update set "
        f"t1.max_depth=t2.max_depth, "
        f"t1.min_samples_split=t2.min_samples_split, "
        f"t1.min_samples_leaf=t2.min_samples_leaf, "
        f"t1.max_leaf_nodes=t2.max_leaf_nodes, "
        f"t1.max_features=t2.max_features, "
        f"t1.criterion=t2.criterion, "
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
        f"insert into ml_decision_trees_regression_element"
        f"(id, create_user, create_time, "
        f"version_id, max_depth, min_samples_split, "
        f"min_samples_leaf, max_leaf_nodes,max_features,criterion, "
        f"is_enabled)"
        f"values"
        f"('{element_id}', "
        f"'{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"100, "
        f"2, "
        f"1, "
        f"100, "
        f"'auto', "
        f"'squared_error', "
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
    # 拷贝算子数据
    copy_sql = (
        f"insert into ml_decision_trees_regression_element "
        f"select id, create_user, to_date('{current_time()}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', max_depth, min_samples_split, min_samples_leaf, "
        f"max_leaf_nodes,max_features,criterion, is_enabled "
        f"from ml_decision_trees_regression_element "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql
