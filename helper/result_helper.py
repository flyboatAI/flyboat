from enum_type.result_code import ResultCode


class ExecuteResult:
    def __init__(self, code, message, data):
        """
        初始化方法
        :param code: 结果码
        :param message: 信息
        :param data: 数据
        """
        self.code = code
        self.message = message
        self.data = data
        pass


class FetchResult:
    def __init__(self, code, result):
        """
        初始化方法
        :param code: 结果码
        :param result: 结果信息
        """
        self.code = code
        self.result = result
        pass


class ProcessResult:
    def __init__(
        self,
        code,
        dependency_arr,
        data_arr,
        fields_arr,
        model_arr,
        scaler_arr,
        role_arr,
    ):
        """
        初始化方法
        :param code: 结果码
        :param dependency_arr: 依赖
        :param data_arr: 数据
        :param fields_arr: 字段
        :param model_arr: 模型
        :param scaler_arr: 缩放器
        :param role_arr: 角色
        """
        self.code = code
        self.dependency_arr = dependency_arr
        self.data_arr = data_arr
        self.fields_arr = fields_arr
        self.model_arr = model_arr
        self.scaler_arr = scaler_arr
        self.role_arr = role_arr
        pass


def process_success(
    code=ResultCode.Success.value,
    message="",
    dependency_arr=None,
    data_arr=None,
    fields_arr=None,
    model_arr=None,
    scaler_arr=None,
    role_arr=None,
):
    """
    算子执行成功返回的数据
    :param message: 消息
    :param code: 执行状态
    :param dependency_arr: 依赖的算子 id 数组
    :param data_arr: 传递的数据数组
    :param fields_arr: 传递的字段数组
    :param model_arr: 传递的模型数组
    :param scaler_arr: 传递的缩放器数组
    :param role_arr: 传递的角色数组
    :return: 传递信息
    """
    if dependency_arr is None:
        dependency_arr = []
    if data_arr is None:
        data_arr = []
    if fields_arr is None:
        fields_arr = []
    if model_arr is None:
        model_arr = []
    if scaler_arr is None:
        scaler_arr = []
    if role_arr is None:
        role_arr = []
    return (
        code,
        message,
        dependency_arr,
        data_arr,
        fields_arr,
        role_arr,
        model_arr,
        scaler_arr,
    )


def process_error(message=""):
    """
    算子执行失败返回的数据
    :param message: 消息
    :return: error
    """
    return process_success(code=ResultCode.Error.value, message=message)


def process_cancel():
    """
    算子执行取消
    :return: error
    """
    return process_success(code=ResultCode.Cancel.value)


def execute_success(code=ResultCode.Success.value, message="", data=None):
    """
    执行引擎执行成功返回数据
    :param code: 代码
    :param message: 消息
    :param data: 数据
    :return: 状态
    """
    return ExecuteResult(code, message, data)


def execute_cancel(code=ResultCode.Cancel.value, message="", data=None):
    """
    执行引擎执行成功返回数据
    :param code: 代码
    :param message: 消息
    :param data: 数据
    :return: 状态
    """
    return ExecuteResult(code, message, data)


def execute_error(message=""):
    """
    执行引擎执行失败返回数据
    :param message:
    :return: 状态
    """
    return execute_success(ResultCode.Error.value, message)


def fetch_success(code=ResultCode.Success.value, result=None):
    return FetchResult(code, result)


def fetch_error():
    return fetch_success(ResultCode.Error.value)
