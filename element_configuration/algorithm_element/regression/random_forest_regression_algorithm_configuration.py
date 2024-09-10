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
                'n_estimators': 100,
                'criterion': 'squared_error',
                'max_depth': 100,
                'min_samples_split': 2,
                'min_samples_leaf': 1,
                'min_weight_fraction_leaf': 0.0,
                'max_features': 'sqrt',
                'max_leaf_nodes': 20,
                'min_impurity_decrease': 0.0,
                'bootstrap': 1,
                'random_state': 40
            }
    """
    algorithm_sql = (
        f"select n_estimators, criterion, max_depth, "
        f"min_samples_split,min_samples_leaf, "
        f"min_weight_fraction_leaf,max_features,"
        f"max_leaf_nodes,min_impurity_decrease,bootstrap,random_state "
        f"from ml_random_forest_regression_element "
        f"where version_id='{version_id}' "
        f"and id='{element_id}' "
        f"and is_enabled={Enabled.Yes.value}"
    )
    algorithm_dict = db_helper1.fetchone(algorithm_sql, [])
    if not algorithm_dict:
        return execute_success(
            data={
                "n_estimators": 100,
                "criterion": "squared_error",
                "max_depth": 100,
                "min_samples_split": 2,
                "min_samples_leaf": 1,
                "min_weight_fraction_leaf": 0.0,
                "max_features": "sqrt",
                "max_leaf_nodes": 20,
                "min_impurity_decrease": 0.0,
                "bootstrap": 1,
                "random_state": 40,
            }
        )

    return execute_success(data=algorithm_dict)


def configuration(
    version_id,
    element_id,
    user_id,
    n_estimators,
    criterion,
    max_depth,
    min_samples_split,
    min_samples_leaf,
    min_weight_fraction_leaf,
    max_features,
    max_leaf_nodes,
    min_impurity_decrease,
    bootstrap,
    random_state,
):
    """
    用于保存算子配置数据
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户表示
    :param n_estimators: 决策树的数量，一般来说数量越多越好，但是会增加计算时间
    :param criterion: 用于衡量分裂质量的指标，一般用均方误差
    :param max_depth: 每棵树的最大深度，设为None表示树可以无限生长，小心过拟合
    :param min_samples_split: 进行划分所需的最小样本数，可以控制过拟合
    :param min_samples_leaf: 叶子节点所需的最小样本数，可以控制过拟合
    :param min_weight_fraction_leaf: 叶子节点所需的最小加权分数
    :param max_features: 搜索最佳分割时考虑的特征数量，一般设置为 'auto' 表示考虑所有特征
    :param max_leaf_nodes: 叶子节点的最大数量，用于防止过拟合
    :param min_impurity_decrease: 如果分裂导致杂质减少大于或等于这个值，则进行分裂
    :param bootstrap: 是否使用放回抽样
    :param random_state: 随机种子，设为固定的值可以复现随机性
    :return: 配置成功/失败
    """
    # 点击确定按钮，保存输出端口信息和配置信息至记录表和配置表中
    configuration_sql = generate_configuration_sql(
        version_id,
        element_id,
        user_id,
        n_estimators,
        criterion,
        max_depth,
        min_samples_split,
        min_samples_leaf,
        min_weight_fraction_leaf,
        max_features,
        max_leaf_nodes,
        min_impurity_decrease,
        bootstrap,
        random_state,
    )
    return db_helper1.execute_arr([configuration_sql])


def generate_configuration_sql(
    version_id,
    element_id,
    user_id,
    n_estimators,
    criterion,
    max_depth,
    min_samples_split,
    min_samples_leaf,
    min_weight_fraction_leaf,
    max_features,
    max_leaf_nodes,
    min_impurity_decrease,
    bootstrap,
    random_state,
):
    """
    生成插入或更新算子 SQL 语句
    :param version_id: 版本标识
    :param element_id: 算子标识
    :param user_id: 用户标识
    :param n_estimators: 决策树的数量，一般来说数量越多越好，但是会增加计算时间
    :param criterion: 用于衡量分裂质量的指标，一般用均方误差
    :param max_depth: 每棵树的最大深度，设为None表示树可以无限生长，小心过拟合
    :param min_samples_split: 进行划分所需的最小样本数，可以控制过拟合
    :param min_samples_leaf: 叶子节点所需的最小样本数，可以控制过拟合
    :param min_weight_fraction_leaf: 叶子节点所需的最小加权分数
    :param max_features: 搜索最佳分割时考虑的特征数量，一般设置为 'auto' 表示考虑所有特征
    :param max_leaf_nodes: 叶子节点的最大数量，用于防止过拟合
    :param min_impurity_decrease: 如果分裂导致杂质减少大于或等于这个值，则进行分裂
    :param bootstrap: 是否使用放回抽样
    :param random_state: 随机种子，设为固定的值可以复现随机性
    :return: SQL
    """
    now = current_time()
    if n_estimators is None:
        n_estimators = 100
    if criterion is None:
        criterion = "squared_error"
    if max_depth is None:
        max_depth = 100
    if min_samples_split is None:
        min_samples_split = 2
    if min_samples_leaf is None:
        min_samples_leaf = 1
    if min_weight_fraction_leaf is None:
        min_weight_fraction_leaf = 0.0
    if max_features is None:
        max_features = "sqrt"
    if max_leaf_nodes is None:
        max_leaf_nodes = 20
    if min_impurity_decrease is None:
        min_impurity_decrease = 0.0
    if bootstrap is None:
        bootstrap = 1
    if random_state is None:
        random_state = 40

    # 插入或更新
    configuration_sql = (
        f"merge into ml_random_forest_regression_element t1 "
        f"using (select "
        f"'{element_id}' id, "
        f"'{version_id}' version_id, "
        f"'{user_id}' user_id,"
        f"'{criterion}' criterion,"
        f"{max_depth} max_depth,"
        f"{n_estimators} n_estimators, "
        f"{min_samples_split} min_samples_split, "
        f"{min_samples_leaf} min_samples_leaf, "
        f"{min_weight_fraction_leaf} min_weight_fraction_leaf, "
        f"'{max_features}' max_features, "
        f"{max_leaf_nodes} max_leaf_nodes, "
        f"{min_impurity_decrease} min_impurity_decrease, "
        f"{bootstrap} bootstrap, "
        f"{random_state} random_state "
        f"from dual) t2 on (t1.id = t2.id and t1.version_id=t2.version_id) "
        f"when not matched then "
        f"insert (id, create_user, create_time, version_id, "
        f"n_estimators, criterion, max_depth, min_samples_split,"
        f"min_samples_leaf, min_weight_fraction_leaf, max_features, max_leaf_nodes,"
        f" min_impurity_decrease, bootstrap, random_state, is_enabled) "
        f"values"
        f"(t2.id, "
        f"t2.user_id, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"t2.version_id, "
        f"t2.n_estimators,"
        f"t2.criterion, "
        f"t2.max_depth, "
        f"t2.min_samples_split, "
        f"t2.min_samples_leaf, "
        f"t2.min_weight_fraction_leaf, "
        f"t2.max_features, "
        f"t2.max_leaf_nodes, "
        f"t2.min_impurity_decrease, "
        f"t2.bootstrap, "
        f"t2.random_state, "
        f"{Enabled.Yes.value}) "
        f"when matched then "
        f"update set "
        f"t1.n_estimators=t2.n_estimators, "
        f"t1.criterion=t2.criterion, "
        f"t1.max_depth=t2.max_depth, "
        f"t1.min_samples_split=t2.min_samples_split, "
        f"t1.min_samples_leaf=t2.min_samples_leaf, "
        f"t1.min_weight_fraction_leaf=t2.min_weight_fraction_leaf, "
        f"t1.max_features=t2.max_features, "
        f"t1.max_leaf_nodes=t2.max_leaf_nodes, "
        f"t1.min_impurity_decrease=t2.min_impurity_decrease, "
        f"t1.bootstrap=t2.bootstrap, "
        f"t1.random_state=t2.random_state, "
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
        f"insert into ml_random_forest_regression_element"
        f"(id, create_user, create_time, "
        f"version_id, n_estimators, criterion,"
        f" max_depth, min_samples_split,min_samples_leaf,"
        f"min_weight_fraction_leaf,max_features,max_leaf_nodes,"
        f"min_impurity_decrease,bootstrap,random_state,"
        f"is_enabled)"
        f"values"
        f"('{element_id}', "
        f"'{user_id}', "
        f"to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{version_id}', "
        f"100, "
        f"'squared_error', "
        f"200, "
        f"2, "
        f"1, "
        f"0.0, "
        f"'sqrt', "
        f"100, "
        f"0.0, "
        f"1, "
        f"40, "
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
        f"insert into ml_random_forest_regression_element "
        f"select id, create_user, to_date('{now}', 'yyyy-mm-dd hh24:mi:ss'), "
        f"'{new_version_id}', n_estimators, criterion, max_depth, min_samples_split,min_samples_leaf, "
        f"min_weight_fraction_leaf,max_features,max_leaf_nodes,min_impurity_decrease,bootstrap,random_state,"
        f"is_enabled "
        f"from ml_random_forest_regression_element "
        f"where version_id='{old_version_id}'"
    )

    return copy_sql
