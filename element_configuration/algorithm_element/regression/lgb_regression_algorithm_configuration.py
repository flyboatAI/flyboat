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
                'boosting_type': 'gbdt',
                'objective': 'regression',
                'num_leaves': 31,
                'learning_rate': 0.1,
                'feature_fraction': 0.9,
                'bagging_fraction': 0.8,
                'num_boost_round': 5000,
                'max_depth': 1000
              }
    """
    algorithm_sql = (
        f"select boosting_type, max_depth, num_leaves, "
        f"learning_rate, feature_fraction, "
        f"bagging_fraction, num_boost_round,objective "
        f"from ml_lgb_regression_element "
        f"where version_id='{version_id}' "
        f"and id='{element_id}' "
        f"and is_enabled={Enabled.Yes.value}"
    )
    algorithm_dict = db_helper1.fetchone(algorithm_sql, [])
    if not algorithm_dict:
        return execute_success(
            data={
                "boosting_type": "gbdt",
                "objective": "regression",
                "num_leaves": 31,
                "learning_rate": 0.1,
                "feature_fraction": 0.9,
                "bagging_fraction": 0.8,
                "num_boost_round": 5000,
                "max_depth": 1000,
            }
        )

    return execute_success(data=algorithm_dict)


def configuration(
    version_id,
    element_id,
    user_id,
    boosting_type,
    num_leaves,
    learning_rate,
    feature_fraction,
    max_depth,
    bagging_fraction,
    num_boost_round,
    objective,
):
    """
    用于保存算子配置数据
    :param max_depth: 最大迭代深度
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param boosting_type: 提升类型，可以是gbdt，dart，goss 或 rf。默认为 gbdt，表示使用梯度提升树。
    :param num_leaves: 每棵树上的最大叶子节点数。控制模型的复杂度，较大的值可以提高精度，但也可能导致过拟合。
    :param learning_rate: 学习率，控制每棵树的贡献。较小的学习率可以使得模型更加鲁棒，但训练时间会更长。
    :param feature_fraction: 每棵树训练时使用的特征比例。
    :param bagging_fraction: 每次迭代时用于训练的数据比例。
    :param num_boost_round: 树的数量
    :param objective:设置lgb分类或者回归
    :return: 配置成功/失败
    """
    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    configuration_sql = generate_configuration_sql(
        version_id,
        element_id,
        user_id,
        boosting_type,
        num_leaves,
        learning_rate,
        feature_fraction,
        max_depth,
        bagging_fraction,
        num_boost_round,
        objective,
    )
    return db_helper1.execute_arr([configuration_sql])


def generate_configuration_sql(
    version_id,
    element_id,
    user_id,
    boosting_type,
    num_leaves,
    learning_rate,
    feature_fraction,
    max_depth,
    bagging_fraction,
    num_boost_round,
    objective,
):
    """
    生成插入或更新算子 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param boosting_type: 提升类型，可以是 gbdt，dart，goss 或 rf。默认为 gbdt，表示使用梯度提升树。
    :param num_leaves: 每棵树上的最大叶子节点数。控制模型的复杂度，较大的值可以提高精度，但也可能导致过拟合。
    :param learning_rate: 学习率，控制每棵树的贡献。较小的学习率可以使得模型更加鲁棒，但训练时间会更长。
    :param feature_fraction: 每棵树训练时使用的特征比例。
    :param max_depth: 最大迭代深度
    :param bagging_fraction: 每次迭代时用于训练的数据比例。
    :param num_boost_round: 树的数量
    :param objective:设置lgb分类或者回归
    :return: SQL
    """
    now = current_time()
    if boosting_type is None:
        boosting_type = "gbdt"
    if num_leaves is None:
        num_leaves = 31
    if learning_rate is None:
        learning_rate = 0.1
    if feature_fraction is None:
        feature_fraction = 0.9
    if bagging_fraction is None:
        bagging_fraction = 0.8
    if num_boost_round is None:
        num_boost_round = 5000
    if max_depth is None:
        max_depth = 1000
    if objective is None:
        objective = "regression"
    # 插入或更新
    configuration_sql = (
        f"merge into ml_lgb_regression_element t1 "
        f"using (select "
        f"'{element_id}' id, "
        f"'{version_id}' version_id, "
        f"'{user_id}' user_id,"
        f"'{boosting_type}' boosting_type, "
        f"{num_leaves} num_leaves, "
        f"{learning_rate} learning_rate, "
        f"{feature_fraction} feature_fraction, "
        f"{max_depth} max_depth,"
        f"{bagging_fraction} bagging_fraction, "
        f"{num_boost_round} num_boost_round, "
        f"'{objective}' objective "
        f"from dual) t2 on (t1.id = t2.id and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, version_id, "
        f"boosting_type, num_leaves, learning_rate, feature_fraction, "
        f"max_depth, bagging_fraction, num_boost_round, objective, is_enabled) "
        f"values"
        f"(t2.id, "
        f"t2.user_id, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, "
        f"t2.boosting_type, "
        f"t2.num_leaves, "
        f"t2.learning_rate, "
        f"t2.feature_fraction,"
        f"t2.max_depth,"
        f"t2.bagging_fraction,"
        f"t2.num_boost_round,"
        f"t2.objective,"
        f"{Enabled.Yes.value}) "
        f"when matched then "
        f"update set "
        f"t1.boosting_type=t2.boosting_type, "
        f"t1.num_leaves=t2.num_leaves, "
        f"t1.learning_rate=t2.learning_rate, "
        f"t1.feature_fraction=t2.feature_fraction, "
        f"t1.max_depth=t2.max_depth,"
        f"t1.bagging_fraction=t2.bagging_fraction, "
        f"t1.num_boost_round=t2.num_boost_round, "
        f"t1.objective=t2.objective, "
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
        f"insert into ml_lgb_regression_element"
        f"(id, create_user, create_time, "
        f"version_id, "
        f"boosting_type, num_leaves, "
        f"learning_rate, feature_fraction, max_depth, bagging_fraction, "
        f"num_boost_round,objective,"
        f"is_enabled)"
        f"values"
        f"('{element_id}', "
        f"'{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"'gbdt', "
        f"31, "
        f"0.001, "
        f"0.9, "
        f"1000, "
        f"0.8,"
        f"5000, "
        f"'regression',"
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
        f"insert into ml_lgb_regression_element "
        f"select id, create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', boosting_type, num_leaves, learning_rate, "
        f"feature_fraction, bagging_fraction, max_depth, objective, num_boost_round, is_enabled "
        f"from ml_lgb_regression_element "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql
