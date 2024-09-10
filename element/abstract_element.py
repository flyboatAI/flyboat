from abc import abstractmethod

from helper.oss_helper.oss_helper import oss_helper1
from helper.sql_helper.init_sql_helper import db_helper1


class AbstractElement:
    def __init__(self, element_id, version_id, user_id):
        """
        初始化方法
        :param element_id: 算子标识
        :param version_id: 版本标识
        :param user_id: 用户标识
        """
        self.e_id = element_id
        self.v_id = version_id
        self.u_id = user_id
        pass

    @abstractmethod
    def element_process(
        self,
        process_id,
        dependency_id_arr,
        data_arr,
        fields_arr,
        role_arr,
        model_arr,
        scaler_arr,
        **kwargs,
    ):
        """算子执行函数"""
        """抽象方法，所有算子被执行引擎调用时，均执行该方法"""

    @staticmethod
    def insert_process_pipelining(sql, params=None):
        """
        根据 SQL 将执行记录插入到记录表中
        :param sql: SQL
        :param params: 动态参数
        :return: 插入成功 or 失败
        """
        return db_helper1.execute(sql, params)

    @staticmethod
    def create_csv_file(data, fields=None):
        return oss_helper1.create_csv_file(data, fields)
