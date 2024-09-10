import copy
import json
import sys

import numpy as np
import pandas as pd
import torch
from scipy.optimize import curve_fit
from torch import optim

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.library_type import LibraryType
from enum_type.result_code import ResultCode
from error.data_process_error import DataProcessError
from error.execute_error import ExecuteError
from error.predict_error import PredictError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.error_helper import translate_error_message
from helper.matrix_helper import train_matrix_build
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED
from helper.websocket_helper import generate_running_message, send_message


class WeibullAlgorithm(AbstractElement):
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
        """
        Weibull算子实现
        输入端口: 一个D(data)端口
        输出端口: 一个M(model)端口
        +--------+
        |        |
        |        |
        D        M
        |        |
        |        |
        +--------+
        :param process_id: 流水标识
        :param dependency_id_arr: 依赖的算子标识数组
        :param data_arr: 接收的数据数组
        :param fields_arr: 接收的数据列头数据数组
        :param model_arr: 接收的模型数组
        :param scaler_arr: 接收的缩放器数组
        :param role_arr: 接收的角色数组
        :return: 生产的模型
        """
        element_name = f'{kwargs.get("element_name")}算子'

        UNUSED(fields_arr, model_arr, process_id)
        if not data_arr or not role_arr or not fields_arr:
            raise ExecuteError(
                sys._getframe().f_code.co_name,
                f"{element_name}接收的数据或角色或字段为空",
            ) from None
        weibull_regression_sql = (
            f"select calc_method, is_multimodal, number_of_peaks,"
            f"number_of_peaks_region,m_a_list,"
            f" optimization_method,lr,epoch "
            f"from ml_weibull_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        # 根据角色数据组装训练数据
        role_setting_arr = port(0, role_arr)
        train = port(0, data_arr)
        fields = port(0, fields_arr)
        if not role_setting_arr:
            raise ExecuteError(sys._getframe().f_code.co_name, f"执行{element_name}前, 未进行角色配置") from None
        _, y_matrix = train_matrix_build(train, role_setting_arr)

        # 是否存在初始值标志
        weibull_dict = db_helper1.fetchone(weibull_regression_sql, [])
        if (
            not weibull_dict
            or "calc_method" not in weibull_dict
            or "is_multimodal" not in weibull_dict
            or "number_of_peaks" not in weibull_dict
            or "number_of_peaks_region" not in weibull_dict
            or "m_a_list" not in weibull_dict
            or "optimization_method" not in weibull_dict
            or "lr" not in weibull_dict
            or "epoch" not in weibull_dict
        ):
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None

        """
        :param calc_method: 计算方法可选参数 ['linear_regression', 'bp_regression']   
        :param is_multimodal: 是否为多峰威布尔分布,默认False, 单峰   
        :param number_of_peaks: 如果是多峰威布尔分布, 峰的数量  
        :param number_of_peaks_region: 如果是多峰威布尔分布, 每个峰的起始位置和节数位置 
        :param m_a_list: 如果是多峰威布尔分布, 每个峰的预估m和a 
        :param optimization_method: 如果使用bp回归的话, 使用的学习率优化方法['adam','sgd'] 
        :param lr: 学习率 
        :param epoch: 在使用bp回归时, 迭代次数   
        """
        calc_method = weibull_dict["calc_method"]
        is_multimodal = weibull_dict["is_multimodal"]
        number_of_peaks = weibull_dict["number_of_peaks"]
        number_of_peaks_region = json.loads(weibull_dict["number_of_peaks_region"])
        m_a_list = json.loads(weibull_dict["m_a_list"])
        optimization_method = weibull_dict["optimization_method"]
        lr = weibull_dict["lr"]
        epoch = weibull_dict["epoch"]
        if is_multimodal:
            # 多峰
            if calc_method == "linear_regression":
                md = Weibull(
                    data=y_matrix,
                    feature_col=["x"],
                    calc_method=calc_method,
                    is_multimodal=is_multimodal,
                    number_of_peaks=number_of_peaks,
                    number_of_peaks_region=number_of_peaks_region,
                    m_a_list=m_a_list,
                )
            elif calc_method == "bp_regression":
                md = Weibull(
                    data=y_matrix,
                    feature_col=["x"],
                    calc_method=calc_method,
                    is_multimodal=is_multimodal,
                    number_of_peaks=number_of_peaks,
                    number_of_peaks_region=number_of_peaks_region,
                    m_a_list=m_a_list,
                    optimization_method=optimization_method,
                    lr=lr,
                    epoch=epoch,
                )
            else:
                raise DataProcessError("计算方法参数不在数据字典中, 请重新获取") from None
        else:
            if calc_method == "linear_regression":
                md = Weibull(
                    y_matrix,
                    feature_col=["x"],
                    calc_method=calc_method,
                    m_a_list=m_a_list,
                )
            elif calc_method == "bp_regression":
                md = Weibull(
                    data=y_matrix,
                    feature_col=["x"],
                    calc_method=calc_method,
                    is_multimodal=is_multimodal,
                    number_of_peaks=None,
                    optimization_method=optimization_method,
                    lr=lr,
                    epoch=epoch,
                    m_a_list=m_a_list,
                )
            else:
                raise DataProcessError("计算方法参数不在数据字典中, 请重新获取") from None

        # model = Weibull(data=y_matrix, m=m, a=a, epoch=epoch)
        try:
            connect_id = kwargs.get("connect_id")
            websocket = kwargs.get("websocket")
            element_name = kwargs.get("element_name")
            # m, a = model.estimate(connect_id, websocket, element_name)
            m, a = md.run(connect_id, websocket, element_name)
            store_sql = process_data_store(
                process_id,
                self.v_id,
                self.e_id,
                self.u_id,
            )
            relative_path = self.create_csv_file(train, fields)
            if fields is None:
                fields = []
            fields_json = json.dumps(fields)
            store_result = self.insert_process_pipelining(store_sql, [relative_path, fields_json])
            if store_result != ResultCode.Success.value:
                raise StoreError(
                    sys._getframe().f_code.co_name,
                    self.u_id,
                    f"{element_name}过程数据存储失败",
                ) from None
            return process_success(
                model_arr=[
                    {
                        "model": {"m": m, "a": a},
                        "library_type": LibraryType.WeibullParameter.value,
                    }
                ]
            )
        except Exception as e:
            raise PredictError(translate_error_message(str(e))) from None

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)


class WeibullRun:
    def __init__(self, cost_time, m, a, is_multimodal=False, number_of_peaks=1):
        self.cost_time = cost_time
        self.m = m
        self.a = a
        self.is_multimodal = is_multimodal
        if self.is_multimodal:
            self.number_of_peaks = number_of_peaks
        else:
            self.number_of_peaks = 1

    def run(self):
        sum_time = 0
        df_value = {"b": []}
        for i in range(self.number_of_peaks):
            x_data = self.data_format(int(self.cost_time[i]["time"]))
            y_pre = self.weibull_pdf(x_data, self.cost_time[i]["total_cost"], self.m[i], self.a[i])
            y_pre = np.around(y_pre, 5)
            df_value["b"].extend(y_pre)
            sum_time += int(self.cost_time[i]["time"])
        df_value["a"] = [x + 1 for x in range(sum_time)]
        df = pd.DataFrame.from_records(df_value, columns=["a", "b"])
        return df

    @staticmethod
    def data_format(t):
        x_data = np.array([x + 1 for x in range(t)], dtype="float32")
        return x_data

    def single_pre(self, x_data, k, m, a):
        result = []
        for x in x_data:
            y_pre = self.weibull_pdf(x, k, m, a)
            result.append(y_pre)
        return result

    @staticmethod
    def weibull_pdf(x, k, m, a):
        exponent = -a * (x**m)
        return k * (m * a * (x ** (m - 1)) * np.exp(exponent))


class Weibull:
    """
    Weibull 计算
    """

    def __init__(
        self,
        data,
        feature_col,
        calc_method="linear_regression",
        is_multimodal=False,
        number_of_peaks=None,
        number_of_peaks_region=None,
        m_a_list=None,
        optimization_method="adam",
        lr=0.0001,
        epoch=10000,
    ):
        """
        初始化方法
        :param data: 需要计算的数据, dataframe类型
        :param feature_col: 计算的特征列
        :param calc_method: 计算方法可选参数 ['linear_regression', 'bp_regression']   字典
        :param is_multimodal: 是否为多峰威布尔分布,默认False, 单峰   整形
        :param number_of_peaks: 如果是多峰威布尔分布, 峰的数量  整型
        :param number_of_peaks_region: 如果是多峰威布尔分布, 每个峰的起始位置和节数位置 clob
        :param m_a_list: 如果是多峰威布尔分布, 每个峰的预估m和a clob
        :param optimization_method: 如果使用bp回归的话, 使用的学习率优化方法['adam','sgd'] 字典
        :param lr: 学习率 float
        :param epoch: 在使用bp回归时，迭代次数  int
        """
        self.k = None
        # self.data = copy.deepcopy(data)
        copy_data = copy.deepcopy(data)
        copy_data = copy_data.flatten()
        self.data = pd.DataFrame({"x": copy_data})
        self.feature_col = feature_col
        if len(feature_col) > 1:
            raise DataProcessError("特征数量过多，请重新配置") from None
        self.x_data = self.data[feature_col].values
        self.calc_method = calc_method
        self.is_multimodal = is_multimodal

        if self.is_multimodal and len(number_of_peaks_region) != number_of_peaks:
            raise DataProcessError("峰的分布情况与数量配置不对应，请重新配置") from None
        self.number_of_peaks = number_of_peaks
        self.number_of_peaks_region = number_of_peaks_region
        self.m_a_list = m_a_list

        self.optimization_method = optimization_method
        self.lr = lr
        self.epoch = epoch
        if not self.is_multimodal:
            self.m = self.m_a_list[0][0]
            self.a = self.m_a_list[0][1]

    def run(self, connect_id, websocket, element_name):
        if self.is_multimodal:
            # 多峰
            x_train, y_train, k_train = self.multimodal_data_format(self.data, self.feature_col, self.calc_method)
            m_list, a_list = self.multimodal_weibull(
                x_train,
                y_train,
                k_train,
                self.calc_method,
                connect_id,
                websocket,
                element_name,
            )
        else:
            # 单峰
            x, y, k = self.single_data_format(self.data, self.feature_col, self.calc_method)
            m, a = self.single_weibull(x, y, k, self.calc_method, connect_id, websocket, element_name)
            m_list = [m]
            a_list = [a]
        return m_list, a_list

    def single_data_format(self, data, feature_col, calc_method):
        # 单峰
        x_data = data[feature_col].values.flatten()
        if calc_method == "bp_regression":
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            x = torch.tensor(
                np.array(range(1, len(x_data) + 1)),
                dtype=torch.float32,
                requires_grad=False,
            ).to(device)
            y = torch.tensor(x_data, dtype=torch.float32, requires_grad=False).to(device)
            k = float(np.sum(x_data))
            return x, y, k
        elif calc_method == "linear_regression":
            x = np.array(range(1, len(x_data) + 1), dtype="float32")
            y = x_data
            k = float(np.sum(x_data))
            self.k = k
            return x, y, k
        else:
            raise DataProcessError("计算方法参数不在数据字典中，请重新获取") from None

    def multimodal_data_format(self, data, feature_col, calc_method):
        x_train = []
        y_train = []
        k_train = []
        for i in range(self.number_of_peaks):
            position_start = int(self.number_of_peaks_region[i][0] - 1)
            position_end = int(self.number_of_peaks_region[i][1])
            try:
                df = data.iloc[position_start:position_end]
            except Exception:
                raise DataProcessError("峰的分布起始、终止位置输入错误，请重新配置") from None
            x, y, k = self.single_data_format(df, feature_col, calc_method)
            x_train.append(x)
            y_train.append(y)
            k_train.append(k)
        return x_train, y_train, k_train

    def single_weibull(self, x, y, k, calc_method, connect_id, websocket, element_name):
        if calc_method == "bp_regression":
            m, a = self.single_bp_regression(
                x,
                y,
                k,
                self.m,
                self.a,
                self.optimization_method,
                self.lr,
                self.epoch,
                connect_id,
                websocket,
                element_name,
            )
        elif calc_method == "linear_regression":
            m, a = self.single_linear_regression(x, y, k, self.m, self.a)
        else:
            m = 0
            a = 0
        return m, a

    def single_bp_regression(self, x, y, k, m, a, opt, lr, epoch, connect_id, websocket, element_name):
        m_estimate = torch.tensor(float(m), requires_grad=True)
        a_estimate = torch.tensor(float(a), requires_grad=True)
        # 定义优化器
        if opt == "adam":
            optimizer = optim.Adam([m_estimate, a_estimate], lr=lr)
        elif opt == "sgd":
            optimizer = optim.SGD([m_estimate, a_estimate], lr=lr)
        else:
            raise DataProcessError("优化器参数不在数据字典中，请重新获取") from None

        # 开始训练
        for epoch in range(epoch):
            # 计算分布
            predicted = self.single_bp_weibull_pdf(x, k, m_estimate, a_estimate)
            # mse
            loss = ((predicted - y) ** 2).mean()

            # mae
            # loss = torch.abs(predicted - self.y).mean()

            # 反向传播和优化
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            if (epoch + 1) % 100 == 0:
                msg = generate_running_message(
                    connect_id,
                    f"{element_name}节点执行进度: Epoch[{epoch + 1} / {self.epoch}], "
                    f"Loss: {loss.item():.4f}, "
                    f"m: {m_estimate.item():.4f}, "
                    f"a: {a_estimate.item():.4f}",
                )
                send_message(websocket, msg)
                print(
                    f"Epoch [{epoch + 1}/{self.epoch}], Loss: {loss.item():.4f}, "
                    f"m: {m_estimate.item():.4f}, a: {a_estimate.item():.4f}"
                )

        m = m_estimate.item()
        a = a_estimate.item()
        return m, a

    def single_linear_regression(self, x, y, k, m, a):
        self.k = k
        re = curve_fit(self.single_liner_weibull_pdf, x, y, p0=[m, a])
        params = re[0]
        m = params[0]
        a = params[1]
        return m, a

    @staticmethod
    def single_bp_weibull_pdf(x, k, m, a):
        return k * (m * a * torch.pow(x, m - 1) * torch.exp(-a * torch.pow(x, m)))

    def single_liner_weibull_pdf(self, x, m, a):
        exponent = -a * (x**m)
        return self.k * (m * a * (x ** (m - 1)) * np.exp(exponent))

    def multimodal_weibull(
        self,
        x_train,
        y_train,
        k_train,
        calc_method,
        connect_id,
        websocket,
        element_name,
    ):
        m_list = []
        a_list = []
        for x, y, k, m_a in zip(x_train, y_train, k_train, self.m_a_list):
            if calc_method == "bp_regression":
                m, a = self.single_bp_regression(
                    x,
                    y,
                    k,
                    m_a[0],
                    m_a[1],
                    self.optimization_method,
                    self.lr,
                    self.epoch,
                    connect_id,
                    websocket,
                    element_name,
                )
            elif calc_method == "linear_regression":
                m, a = self.single_linear_regression(x, y, k, m_a[0], m_a[1])
            else:
                m = 0
                a = 0
            m_list.append(m)
            a_list.append(a)
        return m_list, a_list
