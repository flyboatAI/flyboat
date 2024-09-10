# ⣿⣿⣿⠟⠛⠛⠻⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡟⢋⣩⣉⢻
# ⣿⣿⣿⠀⣿⣶⣕⣈⠹⠿⠿⠿⠿⠟⠛⣛⢋⣰⠣⣿⣿⠀⣿
# ⣿⣿⣿⡀⣿⣿⣿⣧⢻⣿⣶⣷⣿⣿⣿⣿⣿⣿⠿⠶⡝⠀⣿
# ⣿⣿⣿⣷⠘⣿⣿⣿⢏⣿⣿⣋⣀⣈⣻⣿⣿⣷⣤⣤⣿⡐⢿
# ⣿⣿⣿⣿⣆⢩⣝⣫⣾⣿⣿⣿⣿⡟⠿⠿⠦⠀⠸⠿⣻⣿⡄⢻
# ⣿⣿⣿⣿⣿⡄⢻⣿⣿⣿⣿⣿⣿⣿⣿⣶⣶⣾⣿⣿⣿⣿⠇⣼
# ⣿⣿⣿⣿⣿⣿⡄⢿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡟⣰
# ⣿⣿⣿⣿⣿⣿⠇⣼⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⢀⣿
# ⣿⣿⣿⣿⣿⠏⢰⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⢸⣿
# ⣿⣿⣿⣿⠟⣰⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠀⣿
# ⣿⣿⣿⠋⣴⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡄⣿
# ⣿⣿⠋⣼⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡇⢸

# 管道算子
PIPELINING_ELEMENT = "pipelining_element"

# 输入输出
SYNC_INPUT = "sync_input"
SYNC_OUTPUT = "sync_output"
DATABASE_INPUT = "database_input"
DATABASE_OUTPUT = "database_output"
DATA_MODEL = "data_model"
MODEL_FILE_OUTPUT = "model_file_output"
MODEL_SYNC_OUTPUT = "model_sync_output"

# 数据预处理
DATA_FILTER = "data_filter"
DATA_SPLIT = "data_split"
ROLE_SETTING = "role_setting"
COLUMN_DATA_PROCESS = "column_data_process"
COLUMN_TYPE_CONVERT = "column_type_convert"
DATA_REPLICATION = "data_replication"
MIN_MAX_SCALER = "min_max_scaler"
POLYNOMIAL_PROCESSING = "polynomial_processing"
PCA_PROCESSING = "pca_processing"
MONTE_CARLO_GENERATE = "monte_carlo_generate"
INVERSE_MIN_MAX_SCALER = "inverse_min_max_scaler"
DATA_JOIN = "data_join"
COLUMN_FILTER = "column_filter"
COLUMN_ADD = "column_add"

# 评估分析
REGRESSION_EVALUATE = "regression_evaluate"
CLASSIFICATION_EVALUATE = "classification_evaluate"

# 图表分析
SCATTER_PLOT_ANALYZE = "scatter_plot_analyze"
LINE_PLOT_ANALYZE = "line_plot_analyze"
HISTOGRAM_PLOT_ANALYZE = "histogram_plot_analyze"
PIE_PLOT_ANALYZE = "pie_plot_analyze"
SPIDER_PLOT_ANALYZE = "spider_plot_analyze"
BUBBLE_PLOT_ANALYZE = "bubble_plot_analyze"
TREE_PLOT_ANALYZE = "tree_plot_analyze"
BOX_PLOT_ANALYZE = "box_plot_analyze"

# 模型应用
MODEL_FILE = "model_file"
MODEL_APPLY = "model_apply"

# 算法算子
LINEAR_REGRESSION_ALGORITHM = "linear_regression_algorithm"
RIDGE_REGRESSION_ALGORITHM = "ridge_regression_algorithm"
PLS_REGRESSION_ALGORITHM = "pls_regression_algorithm"
SVR_REGRESSION_ALGORITHM = "svr_regression_algorithm"
LGB_REGRESSION_ALGORITHM = "lgb_regression_algorithm"
K_MEANS_ALGORITHM = "k_means_algorithm"
DECISION_TREES_REGRESSION_ALGORITHM = "decision_trees_regression_algorithm"
RANDOM_FOREST_REGRESSION_ALGORITHM = "random_forest_regression_algorithm"
ARIMA_REGRESSION_ALGORITHM = "arima_regression_algorithm"
GM1N_ALGORITHM = "gm1n_algorithm"
BP_REGRESSION_ALGORITHM = "bp_regression_algorithm"
LSTM_REGRESSION_ALGORITHM = "lstm_regression_algorithm"
ANALOGY_ESTIMATION_ALGORITHM = "analogy_estimation_algorithm"
EXPONENTIAL_REGRESSION_ALGORITHM = "exponential_regression_algorithm"
LOGARITHM_REGRESSION_ALGORITHM = "logarithm_regression_algorithm"
WEIBULL_ALGORITHM = "weibull_algorithm"
CUSTOM_ALGORITHM_FILE = "custom_algorithm_file"

# 文本算法
CUT_WORD_ALGORITHM = "cut_word_algorithm"
WORD_TAG_ALGORITHM = "word_tag_algorithm"
WORD_COUNT_ALGORITHM = "word_count_algorithm"

# 自定义公式
CUSTOM_FORMULA = "custom_formula"

# 数据分析
CI_ANALYZE = "ci_analyze"
PCA_ANALYZE = "pca_analyze"
CRITIC_ANALYZE = "critic_analyze"
CRITIC_TOPSIS_ANALYZE = "critic_topsis_analyze"
FACTOR_ANALYZE = "factor_analyze"
GREY_RELATION_ANALYZE = "grey_relation_analyze"
SOBOL_ANALYZE = "sobol_analyze"

# 常量
ERROR_CONFIG_DAG_DICT_MAGIC = "0x1348ad6"

EMPTY_MODULE = "empty module"
INIT_ELEMENT_FUNC_NAME = "init_element"
DELETE_ELEMENT_FUNC_NAME = "delete_element"
COPY_ELEMENT_FUNC_NAME = "copy_element"
INSIGHT_ELEMENT_FUNC_NAME = "insight_element"
DISABLE_ELEMENT_SQL_LIST_FUNC_NAME = "disable_element_sql_list"

CLASS_NAME = "class_name"
ELEMENT_NAME = "element_name"
TABLE_NAME = "table_name"
CLASS_MODULE = "class_module"
INIT_MODULE = "init_module"
DELETE_MODULE = "delete_module"
COPY_MODULE = "copy_module"
INSIGHT_MODULE = "insight_module"
DISABLE_MODULE = "disable_module"

# GLOBAL CONFIGURATION DICTIONARY
# class_name 用于与配置配合, 获取对应算子类型所对应的类, 用于反射执行流水线方法
# element_name 用于与 WebSocket 配合, 获取对应算子类型所对应的名称
# table_name 用于获取算子算子配置表, 验证是否配置完毕可用
# init_module 用于放置到画布中时初始化算子
# copy_module 用于拷贝流水线版本时操作数据

#####################################################################################
#                               FBI WARNING                                         #
#   if add any new element you need the information into the element_info_dict      #
#                                                                                   #
#####################################################################################

element_info_dict = {
    PIPELINING_ELEMENT: {
        CLASS_MODULE: "element.pipelining_element.pipelining_element",
        CLASS_NAME: "PipeliningElement",
        ELEMENT_NAME: "流水线管道",
        TABLE_NAME: "ml_pipelining_element",
        INIT_MODULE: "element_configuration.pipelining_element.pipelining_element_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.pipelining_element.pipelining_element_configuration",
        INSIGHT_MODULE: "element_configuration.multi_tab_structured_data_insight_configuration",
    },
    SYNC_INPUT: {
        CLASS_MODULE: "element.input_output_element.sync_input",
        CLASS_NAME: "SyncInput",
        ELEMENT_NAME: "同步输入",
        TABLE_NAME: "ml_sync_input_element",
        INIT_MODULE: "element_configuration.input_output_element.sync_input_configuration",
        DELETE_MODULE: "element_configuration.input_output_element.sync_input_configuration",
        COPY_MODULE: "element_configuration.input_output_element.sync_input_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    SYNC_OUTPUT: {
        CLASS_MODULE: "element.input_output_element.sync_output",
        CLASS_NAME: "SyncOutput",
        ELEMENT_NAME: "同步输出",
        TABLE_NAME: "ml_sync_output_element",
        INIT_MODULE: "element_configuration.input_output_element.sync_output_configuration",
        DELETE_MODULE: "element_configuration.input_output_element.sync_output_configuration",
        COPY_MODULE: "element_configuration.input_output_element.sync_output_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    DATABASE_INPUT: {
        CLASS_MODULE: "element.input_output_element.database_input",
        CLASS_NAME: "DatabaseInput",
        ELEMENT_NAME: "数据库输入",
        TABLE_NAME: "ml_database_input_element",
        INIT_MODULE: "element_configuration.input_output_element.database_input_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.input_output_element.database_input_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    DATABASE_OUTPUT: {
        CLASS_MODULE: "element.input_output_element.database_output",
        CLASS_NAME: "DatabaseOutput",
        ELEMENT_NAME: "数据库输出",
        TABLE_NAME: "ml_database_output_element",
        INIT_MODULE: "element_configuration.input_output_element.database_output_configuration",
        DELETE_MODULE: "element_configuration.input_output_element.database_output_configuration",
        COPY_MODULE: "element_configuration.input_output_element.database_output_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    MODEL_SYNC_OUTPUT: {
        CLASS_MODULE: "element.input_output_element.model_sync_output",
        CLASS_NAME: "ModelSyncOutput",
        ELEMENT_NAME: "模型同步输出",
        TABLE_NAME: "ml_model_sync_output_element",
        INIT_MODULE: "element_configuration.input_output_element.model_sync_output_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.input_output_element.model_sync_output_configuration",
        INSIGHT_MODULE: EMPTY_MODULE,
    },
    DATA_MODEL: {
        CLASS_MODULE: "element.input_output_element.data_model",
        CLASS_NAME: "DataModel",
        ELEMENT_NAME: "数据模型输入",
        TABLE_NAME: "ml_data_model_element",
        INIT_MODULE: "element_configuration.input_output_element.data_model_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.input_output_element.data_model_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    MODEL_FILE_OUTPUT: {
        CLASS_MODULE: "element.input_output_element.model_file_output",
        CLASS_NAME: "ModelFileOutput",
        ELEMENT_NAME: "模型文件输出",
        TABLE_NAME: "ml_model_output_element",
        INIT_MODULE: "element_configuration.input_output_element.model_file_output_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.input_output_element.model_file_output_configuration",
        INSIGHT_MODULE: "element_configuration.semi_structured_data_insight_configuration",
    },
    DATA_FILTER: {
        CLASS_MODULE: "element.data_preprocessing_element.data_filter",
        CLASS_NAME: "DataFilter",
        ELEMENT_NAME: "数据过滤",
        TABLE_NAME: "ml_data_filter_element",
        INIT_MODULE: "element_configuration.data_preprocessing_element.data_filter_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_preprocessing_element.data_filter_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    DATA_SPLIT: {
        CLASS_MODULE: "element.data_preprocessing_element.data_split",
        CLASS_NAME: "DataSplit",
        ELEMENT_NAME: "数据切分",
        TABLE_NAME: "ml_data_split_element",
        INIT_MODULE: "element_configuration.data_preprocessing_element.data_split_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_preprocessing_element.data_split_configuration",
        INSIGHT_MODULE: "element_configuration.multi_tab_structured_data_insight_configuration",
        DISABLE_MODULE: "element_configuration.data_preprocessing_element.data_split_configuration",
    },
    ROLE_SETTING: {
        CLASS_MODULE: "element.data_preprocessing_element.role_setting",
        CLASS_NAME: "RoleSetting",
        ELEMENT_NAME: "角色设置",
        TABLE_NAME: "ml_role_setting_element",
        INIT_MODULE: "element_configuration.data_preprocessing_element.role_setting_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_preprocessing_element.role_setting_configuration",
        INSIGHT_MODULE: "element_configuration.semi_structured_data_insight_configuration",
    },
    COLUMN_DATA_PROCESS: {
        CLASS_MODULE: "element.data_preprocessing_element.column_data_process",
        CLASS_NAME: "ColumnDataProcess",
        ELEMENT_NAME: "列数据处理",
        TABLE_NAME: "ml_column_data_process_element",
        INIT_MODULE: "element_configuration.data_preprocessing_element.column_data_process_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_preprocessing_element.column_data_process_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    COLUMN_TYPE_CONVERT: {
        CLASS_MODULE: "element.data_preprocessing_element.column_type_convert",
        CLASS_NAME: "ColumnTypeConvert",
        ELEMENT_NAME: "列类型转换",
        TABLE_NAME: "ml_column_type_convert_element",
        INIT_MODULE: "element_configuration.data_preprocessing_element.column_type_convert_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_preprocessing_element.column_type_convert_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    DATA_REPLICATION: {
        CLASS_MODULE: "element.data_preprocessing_element.data_replication",
        CLASS_NAME: "DataReplication",
        ELEMENT_NAME: "数据复制",
        TABLE_NAME: "ml_data_replication_element",
        INIT_MODULE: "element_configuration.data_preprocessing_element.data_replication_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_preprocessing_element.data_replication_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    MIN_MAX_SCALER: {
        CLASS_MODULE: "element.data_preprocessing_element.min_max_scaler",
        CLASS_NAME: "MinMaxScalerElement",
        ELEMENT_NAME: "归一化",
        TABLE_NAME: "ml_min_max_scaler_element",
        INIT_MODULE: "element_configuration.data_preprocessing_element.min_max_scaler_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_preprocessing_element.min_max_scaler_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    POLYNOMIAL_PROCESSING: {
        CLASS_MODULE: "element.data_preprocessing_element.polynomial_processing",
        CLASS_NAME: "PolynomialProcessingElement",
        ELEMENT_NAME: "多项式特征生成",
        TABLE_NAME: "ml_polynomial_processing_element",
        INIT_MODULE: "element_configuration.data_preprocessing_element.polynomial_processing_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_preprocessing_element.polynomial_processing_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    PCA_PROCESSING: {
        CLASS_MODULE: "element.data_preprocessing_element.pca_processing",
        CLASS_NAME: "PcaProcessingElement",
        ELEMENT_NAME: "PCA降维",
        TABLE_NAME: "ml_pca_decomposition_element",
        INIT_MODULE: "element_configuration.data_preprocessing_element.pca_processing_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_preprocessing_element.pca_processing_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    INVERSE_MIN_MAX_SCALER: {
        CLASS_MODULE: "element.data_preprocessing_element.inverse_min_max_scaler",
        CLASS_NAME: "InverseMinMaxScalerElement",
        ELEMENT_NAME: "逆归一化",
        TABLE_NAME: "ml_inverse_min_max_scaler_element",
        INIT_MODULE: "element_configuration.data_preprocessing_element.inverse_min_max_scaler_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_preprocessing_element.inverse_min_max_scaler_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    DATA_JOIN: {
        CLASS_MODULE: "element.data_fusion_element.data_join",
        CLASS_NAME: "DataJoin",
        ELEMENT_NAME: "数据连接",
        TABLE_NAME: "ml_data_join_element",
        INIT_MODULE: "element_configuration.data_fusion_element.data_join_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_fusion_element.data_join_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    COLUMN_FILTER: {
        CLASS_MODULE: "element.data_fusion_element.column_filter",
        CLASS_NAME: "ColumnFilter",
        ELEMENT_NAME: "数据列过滤",
        TABLE_NAME: "ml_column_filter_element",
        INIT_MODULE: "element_configuration.data_fusion_element.column_filter_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_fusion_element.column_filter_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    COLUMN_ADD: {
        CLASS_MODULE: "element.data_fusion_element.column_add",
        CLASS_NAME: "ColumnAdd",
        ELEMENT_NAME: "数据列新增",
        TABLE_NAME: "ml_column_add_element",
        INIT_MODULE: "element_configuration.data_fusion_element.column_add_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_fusion_element.column_add_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    MONTE_CARLO_GENERATE: {
        CLASS_MODULE: "element.input_output_element.monte_carlo_generate",
        CLASS_NAME: "MonteCarloGenerateElement",
        ELEMENT_NAME: "蒙特卡洛数据生成",
        TABLE_NAME: "ml_monte_carlo_generate_element",
        INIT_MODULE: "element_configuration.input_output_element.monte_carlo_generate_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.input_output_element.monte_carlo_generate_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    REGRESSION_EVALUATE: {
        CLASS_MODULE: "element.evaluate_element.regression_evaluate",
        CLASS_NAME: "RegressionEvaluate",
        ELEMENT_NAME: "回归评估",
        TABLE_NAME: "ml_regression_evaluate_element",
        INIT_MODULE: "element_configuration.evaluate_element.regression_evaluate_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.evaluate_element.regression_evaluate_configuration",
        INSIGHT_MODULE: "element_configuration.multi_tab_structured_data_insight_configuration",
    },
    CLASSIFICATION_EVALUATE: {
        CLASS_MODULE: "element.evaluate_element.classification_evaluate",
        CLASS_NAME: "ClassificationEvaluate",
        ELEMENT_NAME: "分类评估",
        TABLE_NAME: "ml_classification_evaluate_element",
        INIT_MODULE: "element_configuration.evaluate_element.classification_evaluate_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.evaluate_element.classification_evaluate_configuration",
        INSIGHT_MODULE: "element_configuration.multi_tab_structured_data_insight_configuration",
    },
    SCATTER_PLOT_ANALYZE: {
        CLASS_MODULE: "element.data_analyze_element.scatter_plot_analyze",
        CLASS_NAME: "ScatterPlotAnalyze",
        ELEMENT_NAME: "散点图分析",
        TABLE_NAME: "ml_scatter_plot_analyze_element",
        INIT_MODULE: "element_configuration.data_analyze_element.scatter_plot_analyze_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_analyze_element.scatter_plot_analyze_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    LINE_PLOT_ANALYZE: {
        CLASS_MODULE: "element.data_analyze_element.line_plot_analyze",
        CLASS_NAME: "LinePlotAnalyze",
        ELEMENT_NAME: "折线图分析",
        TABLE_NAME: "ml_line_plot_analyze_element",
        INIT_MODULE: "element_configuration.data_analyze_element.line_plot_analyze_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_analyze_element.line_plot_analyze_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    HISTOGRAM_PLOT_ANALYZE: {
        CLASS_MODULE: "element.data_analyze_element.histogram_plot_analyze",
        CLASS_NAME: "HistogramPlotAnalyze",
        ELEMENT_NAME: "柱状图分析",
        TABLE_NAME: "ml_histogram_plot_analyze_element",
        INIT_MODULE: "element_configuration.data_analyze_element.histogram_plot_analyze_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_analyze_element.histogram_plot_analyze_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    PIE_PLOT_ANALYZE: {
        CLASS_MODULE: "element.data_analyze_element.pie_plot_analyze",
        CLASS_NAME: "PiePlotAnalyze",
        ELEMENT_NAME: "饼图分析",
        TABLE_NAME: "ml_pie_plot_analyze_element",
        INIT_MODULE: "element_configuration.data_analyze_element.pie_plot_analyze_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_analyze_element.pie_plot_analyze_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    SPIDER_PLOT_ANALYZE: {
        CLASS_MODULE: "element.data_analyze_element.spider_plot_analyze",
        CLASS_NAME: "SpiderPlotAnalyze",
        ELEMENT_NAME: "雷达图分析",
        TABLE_NAME: "ml_spider_plot_analyze_element",
        INIT_MODULE: "element_configuration.data_analyze_element.spider_plot_analyze_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_analyze_element.spider_plot_analyze_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    BUBBLE_PLOT_ANALYZE: {
        CLASS_MODULE: "element.data_analyze_element.bubble_plot_analyze",
        CLASS_NAME: "BubblePlotAnalyze",
        ELEMENT_NAME: "气泡图分析",
        TABLE_NAME: "ml_bubble_plot_analyze_element",
        INIT_MODULE: "element_configuration.data_analyze_element.bubble_plot_analyze_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_analyze_element.bubble_plot_analyze_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    TREE_PLOT_ANALYZE: {
        CLASS_MODULE: "element.data_analyze_element.tree_plot_analyze",
        CLASS_NAME: "TreePlotAnalyze",
        ELEMENT_NAME: "树图分析",
        TABLE_NAME: "ml_tree_plot_analyze_element",
        INIT_MODULE: "element_configuration.data_analyze_element.tree_plot_analyze_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_analyze_element.tree_plot_analyze_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    BOX_PLOT_ANALYZE: {
        CLASS_MODULE: "element.data_analyze_element.box_plot_analyze",
        CLASS_NAME: "BoxPlotAnalyze",
        ELEMENT_NAME: "盒须图分析",
        TABLE_NAME: "ml_box_plot_analyze_element",
        INIT_MODULE: "element_configuration.data_analyze_element.box_plot_analyze_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_analyze_element.box_plot_analyze_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    CI_ANALYZE: {
        CLASS_MODULE: "element.data_analyze_element.ci_analyze",
        CLASS_NAME: "CIAnalyze",
        ELEMENT_NAME: "置信度计算",
        TABLE_NAME: None,
        INIT_MODULE: "element_configuration.data_analyze_element.ci_analyze_configuration",
        DELETE_MODULE: EMPTY_MODULE,
        COPY_MODULE: EMPTY_MODULE,
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    MODEL_FILE: {
        CLASS_MODULE: "element.model_operation_element.model_file",
        CLASS_NAME: "ModelFile",
        ELEMENT_NAME: "模型文件",
        TABLE_NAME: "ml_model_file_element",
        INIT_MODULE: "element_configuration.model_operation_element.model_file_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.model_operation_element.model_file_configuration",
        INSIGHT_MODULE: "element_configuration.semi_structured_data_insight_configuration",
    },
    MODEL_APPLY: {
        CLASS_MODULE: "element.model_operation_element.model_apply",
        CLASS_NAME: "ModelApply",
        ELEMENT_NAME: "模型应用",
        TABLE_NAME: "ml_model_apply_element",
        INIT_MODULE: "element_configuration.model_operation_element.model_apply_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.model_operation_element.model_apply_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    LINEAR_REGRESSION_ALGORITHM: {
        CLASS_MODULE: "element.algorithm_element.regression" ".linear_regression_algorithm",
        CLASS_NAME: "LinearRegressionAlgorithm",
        ELEMENT_NAME: "线性回归算法",
        TABLE_NAME: "ml_linear_regression_element",
        INIT_MODULE: "element_configuration.algorithm_element" ".regression.linear_regression_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.algorithm_element" ".regression.linear_regression_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    RIDGE_REGRESSION_ALGORITHM: {
        CLASS_MODULE: "element.algorithm_element.regression" ".ridge_regression_algorithm",
        CLASS_NAME: "RidgeRegressionAlgorithm",
        ELEMENT_NAME: "岭回归算法",
        TABLE_NAME: "ml_ridge_regression_element",
        INIT_MODULE: "element_configuration.algorithm_element" ".regression.ridge_regression_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.algorithm_element" ".regression.ridge_regression_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    PLS_REGRESSION_ALGORITHM: {
        CLASS_MODULE: "element.algorithm_element.regression" ".pls_regression_algorithm",
        CLASS_NAME: "PLSRegressionAlgorithm",
        ELEMENT_NAME: "偏最小二乘回归算法",
        TABLE_NAME: "ml_pls_regression_element",
        INIT_MODULE: "element_configuration.algorithm_element" ".regression.pls_regression_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.algorithm_element" ".regression.pls_regression_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    SVR_REGRESSION_ALGORITHM: {
        CLASS_MODULE: "element.algorithm_element.regression" ".svr_regression_algorithm",
        CLASS_NAME: "SVRRegressionAlgorithm",
        ELEMENT_NAME: "支持向量机回归算法(SVR)",
        TABLE_NAME: "ml_svr_regression_element",
        INIT_MODULE: "element_configuration.algorithm_element" ".regression.svr_regression_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.algorithm_element" ".regression.svr_regression_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    LGB_REGRESSION_ALGORITHM: {
        CLASS_MODULE: "element.algorithm_element.regression" ".lgb_regression_algorithm",
        CLASS_NAME: "LGBRegressionAlgorithm",
        ELEMENT_NAME: "梯度提升树回归算法(LGB)",
        TABLE_NAME: "ml_lgb_regression_element",
        INIT_MODULE: "element_configuration.algorithm_element" ".regression.lgb_regression_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.algorithm_element" ".regression.lgb_regression_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    DECISION_TREES_REGRESSION_ALGORITHM: {
        CLASS_MODULE: "element.algorithm_element.regression" ".decision_trees_regression_algorithm",
        CLASS_NAME: "DecisionTreesRegressionAlgorithm",
        ELEMENT_NAME: "决策回归树算法",
        TABLE_NAME: "ml_decision_trees_regression_element",
        INIT_MODULE: "element_configuration.algorithm_element"
        ".regression.decision_trees_regression_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.algorithm_element"
        ".regression.decision_trees_regression_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    RANDOM_FOREST_REGRESSION_ALGORITHM: {
        CLASS_MODULE: "element.algorithm_element.regression" ".random_forest_regression_algorithm",
        CLASS_NAME: "RandomForestRegressionAlgorithm",
        ELEMENT_NAME: "随机森林回归算法",
        TABLE_NAME: "ml_random_forest_regression_element",
        INIT_MODULE: "element_configuration.algorithm_element"
        ".regression.random_forest_regression_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.algorithm_element"
        ".regression.random_forest_regression_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    ARIMA_REGRESSION_ALGORITHM: {
        CLASS_MODULE: "element.formula_element" ".arima_regression_algorithm",
        CLASS_NAME: "ARIMARegressionAlgorithm",
        ELEMENT_NAME: "时间序列回归算法(ARIMA)",
        TABLE_NAME: "ml_arima_regression_element",
        INIT_MODULE: "element_configuration.formula_element" ".arima_regression_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.formula_element" ".arima_regression_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    GM1N_ALGORITHM: {
        CLASS_MODULE: "element.formula_element" ".gm1n_formula",
        CLASS_NAME: "GM1NAlgorithm",
        ELEMENT_NAME: "灰度预测算法(GM1N)",
        TABLE_NAME: None,
        INIT_MODULE: "element_configuration.formula_element.gm1n_formula_configuration",
        DELETE_MODULE: EMPTY_MODULE,
        COPY_MODULE: EMPTY_MODULE,
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    K_MEANS_ALGORITHM: {
        CLASS_MODULE: "element.algorithm_element.cluster" ".k_means_algorithm",
        CLASS_NAME: "KMeansElement",
        ELEMENT_NAME: "聚类算法(KMEANS)",
        TABLE_NAME: "ml_k_means_element",
        INIT_MODULE: "element_configuration.algorithm_element" ".cluster.k_means_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.algorithm_element" ".cluster.k_means_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    BP_REGRESSION_ALGORITHM: {
        CLASS_MODULE: "element.algorithm_element.regression" ".bp_regression_algorithm",
        CLASS_NAME: "BpRegressionAlgorithm",
        ELEMENT_NAME: "反向传播神经网络回归算法(BP)",
        TABLE_NAME: "ml_bp_regression_element",
        INIT_MODULE: "element_configuration.algorithm_element" ".regression.bp_regression_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.algorithm_element" ".regression.bp_regression_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    LSTM_REGRESSION_ALGORITHM: {
        CLASS_MODULE: "element.algorithm_element.regression" ".lstm_regression_algorithm",
        CLASS_NAME: "LstmRegressionAlgorithm",
        ELEMENT_NAME: "循环神经网络回归算法(LSTM)",
        TABLE_NAME: "ml_lstm_regression_element",
        INIT_MODULE: "element_configuration.algorithm_element" ".regression.lstm_regression_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.algorithm_element" ".regression.lstm_regression_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    CUSTOM_ALGORITHM_FILE: {
        CLASS_MODULE: "element.algorithm_element.custom" ".custom_algorithm_file",
        CLASS_NAME: "CustomAlgorithmFile",
        ELEMENT_NAME: "自定义算法",
        TABLE_NAME: "ml_custom_algorithm_element",
        INIT_MODULE: "element_configuration.algorithm_element" ".custom.custom_algorithm_file_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.algorithm_element" ".custom.custom_algorithm_file_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    ANALOGY_ESTIMATION_ALGORITHM: {
        CLASS_MODULE: "element.formula_element" ".analogy_estimation_algorithm",
        CLASS_NAME: "AnalogyEstimationAlgorithm",
        ELEMENT_NAME: "类比法",
        TABLE_NAME: "ml_analogy_estimation_element",
        INIT_MODULE: "element_configuration.formula_element" ".analogy_estimation_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.formula_element" ".analogy_estimation_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    EXPONENTIAL_REGRESSION_ALGORITHM: {
        CLASS_MODULE: "element.algorithm_element.regression" ".exponential_regression_algorithm",
        CLASS_NAME: "ExponentialRegressionAlgorithm",
        ELEMENT_NAME: "指数回归算法",
        TABLE_NAME: "ml_exponential_regression_element",
        INIT_MODULE: "element_configuration.algorithm_element.regression"
        ".exponential_regression_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.algorithm_element.regression"
        ".exponential_regression_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    LOGARITHM_REGRESSION_ALGORITHM: {
        CLASS_MODULE: "element.algorithm_element.regression" ".logarithm_regression_algorithm",
        CLASS_NAME: "LogarithmRegressionAlgorithm",
        ELEMENT_NAME: "对数回归算法",
        TABLE_NAME: "ml_logarithm_regression_element",
        INIT_MODULE: "element_configuration.algorithm_element.regression"
        ".logarithm_regression_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.algorithm_element.regression"
        ".logarithm_regression_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    WEIBULL_ALGORITHM: {
        CLASS_MODULE: "element.algorithm_element.regression" ".weibull_algorithm",
        CLASS_NAME: "WeibullAlgorithm",
        ELEMENT_NAME: "威布尔算法",
        TABLE_NAME: "ml_weibull_element",
        INIT_MODULE: "element_configuration.algorithm_element.regression" ".weibull_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.algorithm_element.regression" ".weibull_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    CUSTOM_FORMULA: {
        CLASS_MODULE: "element.formula_element" ".custom_formula",
        CLASS_NAME: "CustomFormula",
        ELEMENT_NAME: "自定义公式",
        TABLE_NAME: "ml_custom_formula_element",
        INIT_MODULE: "element_configuration.formula_element" ".custom_formula_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.formula_element" ".custom_formula_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    CUT_WORD_ALGORITHM: {
        CLASS_MODULE: "element.algorithm_element.text" ".cut_word_algorithm",
        CLASS_NAME: "CutWordAlgorithm",
        ELEMENT_NAME: "分词算法",
        TABLE_NAME: "ml_text_cut_element",
        INIT_MODULE: "element_configuration.algorithm_element.text" ".cut_word_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.algorithm_element.text" ".cut_word_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    WORD_TAG_ALGORITHM: {
        CLASS_MODULE: "element.algorithm_element.text" ".word_tag_algorithm",
        CLASS_NAME: "WordTagAlgorithm",
        ELEMENT_NAME: "关键词提取算法",
        TABLE_NAME: "ml_text_tag_element",
        INIT_MODULE: "element_configuration.algorithm_element.text" ".word_tag_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.algorithm_element.text" ".word_tag_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    WORD_COUNT_ALGORITHM: {
        CLASS_MODULE: "element.algorithm_element.text" ".word_count_algorithm",
        CLASS_NAME: "WordCountAlgorithm",
        ELEMENT_NAME: "词频统计算法",
        TABLE_NAME: "ml_text_count_element",
        INIT_MODULE: "element_configuration.algorithm_element.text" ".word_count_algorithm_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.algorithm_element.text" ".word_count_algorithm_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    PCA_ANALYZE: {
        CLASS_MODULE: "element.data_analyze_element.pca_analyze",
        CLASS_NAME: "PCAAnalyze",
        ELEMENT_NAME: "主成分分析",
        TABLE_NAME: None,
        INIT_MODULE: "element_configuration.data_analyze_element.pca_analyze_configuration",
        DELETE_MODULE: EMPTY_MODULE,
        COPY_MODULE: EMPTY_MODULE,
        INSIGHT_MODULE: "element_configuration.multi_tab_structured_data_insight_configuration",
    },
    CRITIC_ANALYZE: {
        CLASS_MODULE: "element.data_analyze_element.critic_analyze",
        CLASS_NAME: "CriticAnalyze",
        ELEMENT_NAME: "客观赋权法(CRITIC)",
        TABLE_NAME: None,
        INIT_MODULE: "element_configuration.data_analyze_element.critic_analyze_configuration",
        DELETE_MODULE: EMPTY_MODULE,
        COPY_MODULE: EMPTY_MODULE,
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
    CRITIC_TOPSIS_ANALYZE: {
        CLASS_MODULE: "element.data_analyze_element.critic_topsis_analyze",
        CLASS_NAME: "CriticTopsisAnalyze",
        ELEMENT_NAME: "综合评价法(CRITIC TOPSIS)",
        TABLE_NAME: None,
        INIT_MODULE: "element_configuration.data_analyze_element.critic_topsis_analyze_configuration",
        DELETE_MODULE: EMPTY_MODULE,
        COPY_MODULE: EMPTY_MODULE,
        INSIGHT_MODULE: "element_configuration.multi_tab_structured_data_insight_configuration",
    },
    FACTOR_ANALYZE: {
        CLASS_MODULE: "element.data_analyze_element.factor_analyze",
        CLASS_NAME: "FactorAnalyze",
        ELEMENT_NAME: "因子分析",
        TABLE_NAME: None,
        INIT_MODULE: "element_configuration.data_analyze_element.factor_analyze_configuration",
        DELETE_MODULE: EMPTY_MODULE,
        COPY_MODULE: EMPTY_MODULE,
        INSIGHT_MODULE: "element_configuration.multi_tab_structured_data_insight_configuration",
    },
    GREY_RELATION_ANALYZE: {
        CLASS_MODULE: "element.data_analyze_element.grey_relation_analyze",
        CLASS_NAME: "GreyRelationAnalyze",
        ELEMENT_NAME: "灰色关联",
        TABLE_NAME: "ml_grey_relation_analyze_element",
        INIT_MODULE: "element_configuration.data_analyze_element" ".grey_relation_analyze_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_analyze_element" ".grey_relation_analyze_configuration",
        INSIGHT_MODULE: "element_configuration.multi_tab_structured_data_insight_configuration",
    },
    SOBOL_ANALYZE: {
        CLASS_MODULE: "element.data_analyze_element.sobol_analyze",
        CLASS_NAME: "SobolAnalyze",
        ELEMENT_NAME: "敏感度检测(SOBOL)",
        TABLE_NAME: "ml_sobol_analyze_element",
        INIT_MODULE: "element_configuration.data_analyze_element" ".sobol_analyze_configuration",
        DELETE_MODULE: "element_configuration.init_configuration",
        COPY_MODULE: "element_configuration.data_analyze_element" ".sobol_analyze_configuration",
        INSIGHT_MODULE: "element_configuration.structured_data_insight_configuration",
    },
}
