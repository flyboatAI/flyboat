from datetime import datetime

from config import setting


def excel_folder():
    """
    下载 EXCEL 文件相对存储目录
    :return: 文件相对存储目录
    """
    relative_save_folder = folder("excel")
    return relative_save_folder


def model_folder():
    """
    获取模型相对存储目录
    :return: 模型相对存储目录
    """
    relative_save_folder = folder("model")
    return relative_save_folder


def csv_folder():
    """
    获取 csv 文件相对存储目录
    :return: csv 文件相对存储目录
    """
    relative_save_folder = folder("csv")
    return relative_save_folder


def json_folder():
    """
    获取 csv 文件相对存储目录
    :return: csv 文件相对存储目录
    """
    relative_save_folder = folder("json")
    return relative_save_folder


def notebook_folder():
    """
    获取 notebook 文件相对存储目录
    :return: notebook 文件相对存储目录
    """
    relative_save_folder = folder("notebook")
    return relative_save_folder


def folder(prefix):
    today = datetime.now().strftime("%Y%m%d")
    separator = "/"  # os.sep
    relative_save_folder = f"{setting.SYSTEM_NAME}{separator}{prefix}{separator}{today}{separator}"

    return relative_save_folder
