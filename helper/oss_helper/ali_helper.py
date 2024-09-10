import io
import shutil
import tempfile

import joblib
import numpy as np
import oss2
import pandas as pd
from oss2.exceptions import NoSuchKey
from pandas.errors import EmptyDataError

from config import setting
from enum_type.result_code import ResultCode
from error.data_process_error import DataProcessError
from helper.column_type_convert_helper import (
    column_type_convert_from_data_to_df,
    column_type_convert_from_df_to_df,
)
from helper.folder_helper import csv_folder, excel_folder, json_folder, model_folder
from helper.generate_helper import generate_uuid
from helper.oss_helper.oss_helper import OSSHelper


class AliHelper(OSSHelper):
    def upload_model_to_s3(self, content: bytes, file_name: str):
        extend_name = ""
        name_and_extend_list = file_name.split(".")
        if len(name_and_extend_list) > 1:
            extend_name = name_and_extend_list[-1]
        relative_folder = model_folder()
        model_name = generate_uuid()
        key = relative_folder + str(model_name) + extend_name
        s3 = self.get_bucket_client(setting.ALI_MODEL_BUCKET)
        s3.put_object(key, content)
        return key

    def save_model_to_s3(self, model, key):
        s3 = self.get_bucket_client(setting.ALI_MODEL_BUCKET)
        try:
            with tempfile.TemporaryFile() as fp:
                joblib.dump(model, fp)
                fp.seek(0)
                s3.put_object(key, fp)
        except Exception:
            raise DataProcessError("模型存储失败") from None

    def load_model_from_s3(self, key):
        s3 = self.get_bucket_client(setting.ALI_MODEL_BUCKET)
        try:
            with tempfile.TemporaryFile() as fp:
                obj_stream = s3.get_object(key)
                shutil.copyfileobj(obj_stream, fp)
                fp.seek(0)
                return joblib.load(fp)
        except NoSuchKey:
            raise DataProcessError("指定的模型不存在") from None
        except Exception:
            raise DataProcessError("模型加载失败") from None

    def create_json_file(self, data):
        relative_folder = json_folder()
        json_name = generate_uuid()
        key = relative_folder + str(json_name) + ".json"
        s3 = self.get_bucket_client(setting.ALI_JSON_BUCKET)

        with io.StringIO() as json_buffer:
            # noinspection PyTypeChecker
            json_buffer.write(data)
            response = s3.put_object(key, json_buffer.getvalue())
            status = self.valid_s3_response(response)
            if status == 200:
                return key

        raise DataProcessError("json 数据存储失败") from None

    def get_json_file_data(self, key):
        if not key.startswith(setting.SYSTEM_NAME):
            return key

        s3 = self.get_bucket_client(setting.ALI_JSON_BUCKET)
        try:
            obj_stream = s3.get_object(key)
            byte_value = obj_stream.read()
            return byte_value.decode()
        except NoSuchKey:
            raise DataProcessError("指定的文件不存在") from None
        except Exception:
            raise DataProcessError("文件加载失败") from None

    def copy_json_file(self, key):
        if not key.startswith(setting.SYSTEM_NAME):
            return key

        relative_folder = json_folder()
        json_name = generate_uuid()
        new_key = relative_folder + str(json_name) + ".json"

        s3 = self.get_bucket_client(setting.ALI_JSON_BUCKET)
        try:
            response = s3.copy_object(setting.ALI_JSON_BUCKET, key, new_key)
            status = self.valid_s3_response(response)
            if status == 200:
                return new_key
            return None
        except NoSuchKey:
            raise DataProcessError("指定的文件不存在") from None
        except Exception:
            raise DataProcessError("文件拷贝失败") from None

    def generate_s3_presigned_url_for_excel(self, key, bucket=setting.ALI_EXCEL_BUCKET, expiration=3600):
        if not key:
            return None
        s3 = self.get_bucket_client(bucket)
        # noinspection PyBroadException
        try:
            headers = dict()
            headers["content-disposition"] = "attachment"
            file_name = key.split("/")[-1]
            query_params = {"response-content-disposition": f"attachment; filename={file_name}"}
            url = s3.sign_url(
                "GET",
                key,
                expiration,
                headers=headers,
                slash_safe=True,
                params=query_params,
            )
        except Exception:
            return None
        return url

    def generate_s3_presigned_url_for_image(self, key, bucket=setting.ALI_IMAGE_BUCKET, expiration=36000):
        if not key:
            return None
        s3 = self.get_bucket_client(bucket)
        # noinspection PyBroadException
        try:
            headers = dict()
            headers["content-disposition"] = "inline"
            url = s3.sign_url("GET", key, expiration, headers=headers, slash_safe=True)
        except Exception:
            return None
        return url

    def generate_s3_presigned_url_for_model(self, key, bucket=setting.ALI_MODEL_BUCKET, expiration=36000):
        if not key:
            return None
        s3 = self.get_bucket_client(bucket)
        # noinspection PyBroadException
        try:
            headers = dict()
            headers["content-disposition"] = "attachment"
            file_name = key.split("/")[-1]
            query_params = {"response-content-disposition": f"attachment; filename={file_name}"}
            url = s3.sign_url(
                "GET",
                key,
                expiration,
                headers=headers,
                slash_safe=True,
                params=query_params,
            )
        except Exception:
            return None
        return url

    def create_excel_file(self, data, fields=None):
        relative_folder = excel_folder()
        excel_name = generate_uuid()
        key = relative_folder + str(excel_name) + ".xlsx"
        if fields:
            convert_result = column_type_convert_from_data_to_df(data, fields, "data_type")
            if convert_result.code != ResultCode.Success.value:
                raise DataProcessError("数据转换失败") from None
            df = convert_result.data
        else:
            df = pd.DataFrame(data)
        s3 = self.get_bucket_client(setting.ALI_EXCEL_BUCKET)
        header = [f["nick_name"] for f in fields]
        with io.BytesIO() as excel_buffer:
            writer = pd.ExcelWriter(excel_buffer, engine="xlsxwriter")
            # noinspection PyTypeChecker
            df.to_excel(writer, index=False, na_rep="", header=header)
            writer.close()
            response = s3.put_object(key, excel_buffer.getvalue())
            status = self.valid_s3_response(response)
            if status == 200:
                return key

        raise DataProcessError("excel 数据存储失败") from None

    def create_csv_file(self, data, fields=None):
        relative_folder = csv_folder()
        csv_name = generate_uuid()
        key = relative_folder + str(csv_name) + ".csv"
        if fields:
            convert_result = column_type_convert_from_data_to_df(data, fields, "data_type")
            if convert_result.code != ResultCode.Success.value:
                raise DataProcessError("数据转换失败") from None
            df = convert_result.data
        else:
            df = pd.DataFrame(data)
        s3 = self.get_bucket_client(setting.ALI_CSV_BUCKET)

        with io.StringIO() as csv_buffer:
            # noinspection PyTypeChecker
            df.to_csv(csv_buffer, index=False, na_rep="")
            response = s3.put_object(key, csv_buffer.getvalue())
            status = self.valid_s3_response(response)
            if status == 200:
                return key

        raise DataProcessError("csv 数据存储失败") from None

    def read_csv_file(self, key, fields):
        """
        对象存储根据 data 创建 CSV 文件
        :param key: key
        :param fields: 文件字段数据
        :return: 文件内容
        """
        d = []
        try:
            df = pd.read_csv(self.generate_s3_presigned_url_for_excel(key, setting.ALI_CSV_BUCKET))
            if fields:
                convert_result = column_type_convert_from_df_to_df(df, fields, "data_type")
                if convert_result.code == ResultCode.Success.value:
                    df = convert_result.data
            df = df.replace(np.nan, None).replace(np.inf, "inf").replace(-np.inf, "-inf").replace({pd.NaT: None})
            d = df.to_dict("records")
        except (EmptyDataError, FileNotFoundError):
            pass
        return d

    def create_notebook(self, key, content: str):
        """
        创建 notebook 文件, 如果存在则覆盖
        :param key: key
        :param content: 文件内容
        :return:
        """
        s3 = self.get_bucket_client(setting.ALI_NOTEBOOK_BUCKET)
        try:
            with tempfile.TemporaryFile() as fp:
                fp.write(content.encode("utf-8"))
                fp.seek(0)
                s3.put_object(key, fp)
        except Exception:
            raise DataProcessError("notebook 文件存储失败") from None

    def load_notebook(self, key, local_path):
        """
        加载 notebook 文件到本地
        :param key: key
        :param local_path: 本地路径
        :return:
        """
        s3 = self.get_bucket_client(setting.ALI_NOTEBOOK_BUCKET)
        try:
            s3.get_object_to_file(key, local_path)
        except Exception:
            raise DataProcessError("notebook 文件加载失败") from None

    @staticmethod
    def get_bucket_client(bucket_name):
        auth = oss2.Auth(setting.ALI_ACCESS_KEY, setting.ALI_SECRET_KEY)
        cname = setting.ALI_CNAME
        bucket = oss2.Bucket(auth, cname, bucket_name, is_cname=True)
        return bucket

    @staticmethod
    def valid_s3_response(response):
        status = response.status
        return status
