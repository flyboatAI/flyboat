import io
import tempfile

import boto3
import botocore.exceptions
import joblib
import numpy as np
import pandas as pd

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


class S3Helper(OSSHelper):
    def upload_model_to_s3(self, content: bytes, file_name: str):
        extend_name = ""
        name_and_extend_list = file_name.split(".")
        if len(name_and_extend_list) > 1:
            extend_name = name_and_extend_list[-1]
        relative_folder = model_folder()
        model_name = generate_uuid()
        key = relative_folder + str(model_name) + extend_name
        self.make_bucket_if_not_exist(setting.MODEL_BUCKET)
        s3 = self.get_s3_client()
        s3.put_object(Body=content, Bucket=setting.MODEL_BUCKET, Key=key)
        return key

    def save_model_to_s3(self, model, key):
        self.make_bucket_if_not_exist(setting.MODEL_BUCKET)
        s3 = self.get_s3_client()
        try:
            with tempfile.TemporaryFile() as fp:
                joblib.dump(model, fp)
                fp.seek(0)
                s3.put_object(Body=fp.read(), Bucket=setting.MODEL_BUCKET, Key=key)
        except Exception:
            raise DataProcessError("模型存储失败") from None

    def load_model_from_s3(self, key):
        s3 = self.get_s3_client()
        try:
            with tempfile.TemporaryFile() as fp:
                s3.download_fileobj(Fileobj=fp, Bucket=setting.MODEL_BUCKET, Key=key)
                fp.seek(0)
                return joblib.load(fp)
        except Exception:
            raise DataProcessError("模型加载失败") from None

    def create_json_file(self, data):
        relative_folder = json_folder()
        json_name = generate_uuid()
        key = relative_folder + str(json_name) + ".json"

        self.make_bucket_if_not_exist(setting.JSON_BUCKET)
        s3 = self.get_s3_client()

        with io.StringIO() as json_buffer:
            # noinspection PyTypeChecker
            json_buffer.write(data)
            response = s3.put_object(Bucket=setting.JSON_BUCKET, Key=key, Body=json_buffer.getvalue())
            status = self.valid_s3_response(response)
            if status == 200:
                return key

        raise DataProcessError("json 数据存储失败") from None

    def get_json_file_data(self, key):
        if not key.startswith(setting.SYSTEM_NAME):
            return key

        s3 = self.get_s3_client()

        with io.BytesIO() as bytes_buffer:
            s3.download_fileobj(Bucket=setting.JSON_BUCKET, Key=key, Fileobj=bytes_buffer)
            byte_value = bytes_buffer.getvalue()
            return byte_value.decode()

    def copy_json_file(self, key):
        if not key.startswith(setting.SYSTEM_NAME):
            return key

        relative_folder = json_folder()
        json_name = generate_uuid()
        new_key = relative_folder + str(json_name) + ".json"
        copy_source = {"Bucket": setting.JSON_BUCKET, "Key": key}
        s3 = self.get_s3_client()
        s3.copy(copy_source, setting.JSON_BUCKET, new_key)
        return new_key

    def generate_s3_presigned_url_for_excel(self, key, bucket=setting.EXCEL_BUCKET, expiration=3600):
        if not key:
            return None
        s3 = self.get_s3_client()
        # noinspection PyBroadException
        try:
            response = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=expiration,
            )

        except Exception:
            return None
        return response

    def generate_s3_presigned_url_for_image(self, key, bucket=setting.IMAGE_BUCKET, expiration=36000):
        if not key:
            return None
        s3 = self.get_s3_client()
        # noinspection PyBroadException
        try:
            response = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=expiration,
            )

        except Exception:
            return None
        return response

    def generate_s3_presigned_url_for_model(self, key, bucket=setting.MODEL_BUCKET, expiration=36000):
        if not key:
            return None
        s3 = self.get_s3_client()
        # noinspection PyBroadException
        try:
            response = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": key},
                ExpiresIn=expiration,
            )

        except Exception:
            return None
        return response

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
        self.make_bucket_if_not_exist(setting.EXCEL_BUCKET)
        s3 = self.get_s3_client()
        header = [f["nick_name"] for f in fields]
        with io.BytesIO() as excel_buffer:
            writer = pd.ExcelWriter(excel_buffer, engine="xlsxwriter")
            # noinspection PyTypeChecker
            df.to_excel(writer, index=False, na_rep="", header=header)
            writer.close()
            response = s3.put_object(Bucket=setting.EXCEL_BUCKET, Key=key, Body=excel_buffer.getvalue())
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
        self.make_bucket_if_not_exist(setting.CSV_BUCKET)
        s3 = self.get_s3_client()

        with io.StringIO() as csv_buffer:
            # noinspection PyTypeChecker
            df.to_csv(csv_buffer, index=False, na_rep="")
            response = s3.put_object(Bucket=setting.CSV_BUCKET, Key=key, Body=csv_buffer.getvalue())
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
        # noinspection PyBroadException
        try:
            df = pd.read_csv(
                f"s3://{setting.CSV_BUCKET}/{key}",
                storage_options={
                    "key": setting.MINIO_ACCESS_KEY,
                    "secret": setting.MINIO_SECRET_KEY,
                    "client_kwargs": {"endpoint_url": setting.MINIO_SCHEME_AND_URL},
                },
            )
            if fields:
                convert_result = column_type_convert_from_df_to_df(df, fields, "data_type")
                if convert_result.code == ResultCode.Success.value:
                    df = convert_result.data
            df = df.replace(np.nan, None).replace(np.inf, "inf").replace(-np.inf, "-inf").replace({pd.NaT: None})
            d = df.to_dict("records")
        except Exception:
            pass
        return d

    def create_notebook(self, key, content: str):
        """
        创建 notebook 文件, 如果存在则覆盖
        :param key: key
        :param content: 文件内容
        :return:
        """
        self.make_bucket_if_not_exist(setting.NOTEBOOK_BUCKET)
        s3 = self.get_s3_client()
        try:
            with tempfile.TemporaryFile() as fp:
                fp.write(content.encode("utf-8"))
                fp.seek(0)
                s3.put_object(Body=fp.read(), Bucket=setting.NOTEBOOK_BUCKET, Key=key)
        except Exception:
            raise DataProcessError("notebook 文件存储失败") from None

    def load_notebook(self, key, local_path):
        """
        加载 notebook 文件到本地
        :param key: key
        :param local_path: 本地路径
        :return:
        """
        s3 = self.get_s3_client()
        try:
            s3.download_file(setting.NOTEBOOK_BUCKET, key, local_path)
        except Exception:
            raise DataProcessError("notebook 文件加载失败") from None

    @staticmethod
    def get_s3_client():
        s3 = boto3.client(
            "s3",
            endpoint_url=setting.MINIO_SCHEME_AND_URL,
            aws_access_key_id=setting.MINIO_ACCESS_KEY,
            aws_secret_access_key=setting.MINIO_SECRET_KEY,
            verify=setting.MINIO_SCHEME_AND_URL.startswith("https"),
        )
        return s3

    def make_bucket_if_not_exist(self, bucket) -> None:
        s3 = self.get_s3_client()
        try:
            s3.head_bucket(Bucket=bucket)
        except botocore.exceptions.ClientError:
            s3.create_bucket(Bucket=bucket, ACL="public-read")

    @staticmethod
    def valid_s3_response(response):
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        return status
