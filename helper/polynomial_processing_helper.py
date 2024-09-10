# -*- encoding: utf-8 -*-
import pandas as pd

from enum_type.user_data_type import UserDataType
from helper.fields_helper import generate_fields


class FeatureProcessing:
    def __init__(self, data_set, features_col, label_col=None):
        if label_col is None:
            label_col = []
        self.data_set = data_set
        self.features_col = features_col
        self.label_col = label_col

    def run_processing(self):
        """
        :return: DataFrame
        """


class PolynomialProcessing(FeatureProcessing):
    def run_processing(self, degree=2, interaction_only=False, include_bias=False):
        from sklearn.preprocessing import PolynomialFeatures  # 导入多项式处理

        col_list = []
        poly = PolynomialFeatures(degree=degree, interaction_only=interaction_only, include_bias=include_bias)
        features_col_name = self.features_col
        df = poly.fit_transform(self.data_set[features_col_name])
        df = pd.DataFrame(df)
        self.data_set = self.data_set.drop(features_col_name, axis=1)

        df_processing = pd.concat([self.data_set, df], axis=1)
        col_list.extend(self.data_set.columns)
        col_list.extend(list(poly.get_feature_names_out()))
        df_processing.columns = col_list
        field = []
        for col in df_processing.columns:
            field.append(generate_fields(str(col), data_type=UserDataType.Number.value))
        return df_processing.to_dict("records"), field


def polynomial_format(fields, degree, interaction_only):
    # 创建一个空的列名表
    col_list = []
    for k in fields:
        col_list.append(k["name"])
    df = pd.DataFrame(columns=col_list)
    # 给这个df一组数据
    new_data = []
    for i in range(len(col_list)):
        new_data.append(1)
    df.loc[len(df)] = new_data
    # 将这个数据表给多项式特征表, 获取所有的列名
    new_df, new_fields = PolynomialProcessing(features_col=col_list, label_col=[], data_set=df).run_processing(
        degree=degree, interaction_only=interaction_only
    )

    # 返回需要的列名
    return new_fields


def data_conversion(label_role, label_data):
    feature_col = [col for col in label_role]
    # 根据标签获取数据
    train_data = pd.DataFrame(label_data)

    return feature_col, train_data
