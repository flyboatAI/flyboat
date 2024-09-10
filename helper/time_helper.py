from datetime import datetime


def current_time():
    """
    获取当前时间格式化字符串
    :return: 当前时间格式化字符串
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def current_time_m():
    """
    获取当前时间
    :return: 当前时间
    """
    return datetime.now()
