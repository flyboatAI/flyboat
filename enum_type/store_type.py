from enum import Enum


class StoreType(str, Enum):
    # 区间
    Range = "range"
    # 表
    Table = "table"

    # 角色设置
    Role = "role"

    # 数据分割
    Split = "split"

    # 模型输出
    ModelFileOutput = "model_file_output"

    # 模型文件
    ModelFile = "model_file"

    # 散点图
    Scatter = "scatter"
    # 折线图
    Line = "line"
    # 柱状图
    Histogram = "histogram"
    # 饼图
    Pie = "pie"
    # 雷达图
    Spider = "spider"
    # 气泡图
    Bubble = "bubble"
    # 树图
    Tree = "tree"
    # 盒须图
    Box = "box"

    # 回归评估
    RegressionEvaluate = "regression_evaluate"

    # 数据复制
    DataReplication = "data_replication"

    # 主成分分析
    PCA = "pca"

    # 因子分析
    Factor = "factor"

    # Critic 权重计算
    Critic = "critic"

    # CriticTopsis 评分
    CriticTopsis = "critic_topsis"

    # 灰色连接
    GreyRelation = "grey_relation"

    # 管道算子
    PipeliningElement = "pipelining_element"
