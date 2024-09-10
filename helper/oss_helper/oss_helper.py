import abc
import importlib

from config.setting import Oss
from enum_type.oss_type import OSSType
from error.no_such_oss_type_error import NoSuchOssTypeError


class OSSHelper(abc.ABC):
    @abc.abstractmethod
    def upload_model_to_s3(self, content: bytes, file_name: str):
        """
        上传模型文件到 bucket 中
        :param content: 文件内容
        :param file_name: 文件名
        :return: key
        """
        pass

    @abc.abstractmethod
    def save_model_to_s3(self, model, key):
        """
        将模型保存到 bucket 中
        :param model: 模型文件
        :param key: 桶内 key
        :return: None
        """
        pass

    @abc.abstractmethod
    def load_model_from_s3(self, key):
        """
        根据 key 获取模型文件
        :param key: 桶内 key
        :return: 模型文件
        """

    @abc.abstractmethod
    def create_json_file(self, data: str):
        """
        对象存储根据 data 创建 JSON 类型文件
        :param data: 文件内容数据
        :return: 文件标识
        """
        pass

    @abc.abstractmethod
    def get_json_file_data(self, key):
        """
        根据路径获取存储在对象存储文件中的 JSON 数据
        :param key: 文件路径
        :return: 文件内容数据
        """
        pass

    @abc.abstractmethod
    def copy_json_file(self, key):
        """
        复制对象存储中的文件到新路径中
        :param key: 旧文件路径
        :return: 新文件路径
        """
        pass

    @abc.abstractmethod
    def generate_s3_presigned_url_for_excel(self, key, expiration=3600):
        """
        生成临时下载 URL
        :param key: key
        :param expiration: 过期时间
        :return: 预下载 URL | None
        """
        pass

    @abc.abstractmethod
    def generate_s3_presigned_url_for_image(self, key, expiration=36000):
        """
        生成临时下载 URL
        :param key: key
        :param expiration: 过期时间
        :return: 预下载 URL | None
        """
        pass

    @abc.abstractmethod
    def generate_s3_presigned_url_for_model(self, key, expiration=36000):
        """
        生成临时下载 URL
        :param key: key
        :param expiration: 过期时间
        :return: 预下载 URL | None
        """
        pass

    @abc.abstractmethod
    def create_excel_file(self, data: list[dict], fields=None):
        """
        对象存储根据 data 创建 EXCEL 文件
        :param data: 文件内容数据
        :param fields: 文件字段数据
        :return: 文件标识
        """
        pass

    @abc.abstractmethod
    def create_csv_file(self, data: list[dict], fields=None):
        """
        对象存储根据 data 创建 CSV 文件
        :param data: 文件内容数据
        :param fields: 文件字段数据
        :return: 文件标识
        """
        pass

    @abc.abstractmethod
    def read_csv_file(self, key, fields: str):
        """
        对象存储根据 data 创建 CSV 文件
        :param key: key
        :param fields: 文件字段数据
        :return: 文件内容
        """
        pass

    def create_notebook(self, key, content: str):
        """
        创建 notebook 文件, 如果存在则覆盖
        :param key: key
        :param content: 文件内容
        :return:
        """
        pass

    def load_notebook(self, key, local_path):
        """
        加载 notebook 文件到本地
        :param key: key
        :param local_path: 本地路径
        :return:
        """
        pass


def create_oss_helper() -> OSSHelper:
    if Oss in OSSType._value2member_map_:
        module = importlib.import_module(f"helper.oss_helper.{Oss}_helper")
        class_name = f"{Oss.capitalize()}Helper"
        helper = getattr(module, class_name)
        return helper()
    raise NoSuchOssTypeError(error_message="不支持的对象存储类型") from None


oss_helper1 = create_oss_helper()
