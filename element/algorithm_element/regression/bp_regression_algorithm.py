import json
import os
import sys

import torch
import torch.nn as nn

from element.abstract_element import AbstractElement
from enum_type.enabled import Enabled
from enum_type.library_type import LibraryType
from enum_type.process_status_type import ProcessStatusType
from enum_type.result_code import ResultCode
from error.execute_error import ExecuteError
from error.predict_error import PredictError
from error.store_error import StoreError
from helper.data_store_helper import process_data_store
from helper.element_port_helper import port
from helper.error_helper import translate_error_message
from helper.matrix_helper import get_x_count, get_y_count, train_matrix_build
from helper.result_helper import process_success
from helper.sql_helper.init_sql_helper import db_helper1
from helper.warning_helper import UNUSED
from helper.websocket_helper import generate_running_message, send_message


class BpRegressionAlgorithm(AbstractElement):
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
        bp回归算法算子实现
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

        bp_regression_sql = (
            f"select epoch, lr, number_of_node "
            f"from ml_bp_regression_element "
            f"where id='{self.e_id}' and "
            f"version_id='{self.v_id}' and "
            f"is_enabled={Enabled.Yes.value}"
        )

        # 根据角色数据组装训练数据
        role_setting_arr = port(0, role_arr)
        train = port(0, data_arr)
        # if not train or len(train) < 10:
        #     return process_error(message=f"执行该算法数据过少, 请重新配置数据")
        fields = port(0, fields_arr)
        if not role_setting_arr:
            raise ExecuteError(sys._getframe().f_code.co_name, f"执行{element_name}前, 未进行角色配置") from None
        x_matrix, y_matrix = train_matrix_build(train, role_setting_arr)
        x_count = get_x_count(role_setting_arr)
        y_count = get_y_count(role_setting_arr)

        bp_regression_dict = db_helper1.fetchone(bp_regression_sql, [])
        if not bp_regression_dict or "epoch" not in bp_regression_dict or "lr" not in bp_regression_dict:
            raise ExecuteError(sys._getframe().f_code.co_name, f"{element_name}未配置完毕") from None
        """
        epoch: 迭代次数
        lr: 学习速率
        """
        epoch = bp_regression_dict["epoch"]
        lr = bp_regression_dict["lr"]
        number_of_node = bp_regression_dict["number_of_node"]
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        # noinspection PyTypeChecker
        reg = DeepLearningBP(
            x_train=x_matrix,
            y_train=y_matrix,
            input_size=x_count,
            output_size=y_count,
            epoch=epoch,
            lr=lr,
            number_of_node=number_of_node,
            device=device,
        )

        try:
            connect_id = kwargs.get("connect_id")
            websocket = kwargs.get("websocket")
            element_name = kwargs.get("element_name")
            shared_mem = kwargs.get("shared_mem")

            model = reg.train(connect_id, websocket, element_name, shared_mem)

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
            return process_success(model_arr=[{"model": model, "library_type": LibraryType.Pytorch.value}])
        except Exception as e:
            raise PredictError(translate_error_message(str(e))) from None

    def __init__(self, e_id, v_id, u_id):
        super().__init__(e_id, v_id, u_id)


class BP(nn.Module):
    def __init__(self, in_features, out_features, number_of_node):
        super(BP, self).__init__()
        # self.N = nn.LayerNorm(in_features)
        self.hidden = nn.Linear(in_features, number_of_node)
        self.output = nn.Linear(number_of_node, out_features)

    def forward(self, x):
        # y = self.N(n)
        x = torch.relu(self.hidden(x))
        x = self.output(x)
        return x


class DeepLearningBP:
    def __init__(
        self,
        x_train,
        y_train,
        input_size,
        output_size,
        epoch=1000,
        lr=0.001,
        number_of_node=32,
        device="cpu",
    ):
        self.x_train = torch.tensor(x_train, dtype=torch.float32).to(device=device)
        self.y_train = torch.tensor(y_train, dtype=torch.float32).to(device=device)
        self.input_size = input_size
        self.output_size = output_size
        self.epoch = epoch
        self.lr = lr
        self.number_of_node = number_of_node
        self.device = device
        # self.data_loader = DataLoader(self.data, batch_size=550, shuffle=True)

    def train(self, connect_id, websocket, element_name, shared_mem):
        md = BP(self.input_size, self.output_size, self.number_of_node)
        # noinspection PyTypeChecker
        md.to(device=self.device)
        mse_loss = nn.MSELoss()
        md_optim = torch.optim.Adam(md.parameters(), lr=self.lr)
        pid = os.getpid()
        for epoch in range(self.epoch):
            md_optim.zero_grad()
            y_pre = md.forward(self.x_train)
            loss = mse_loss(y_pre, self.y_train)
            loss.backward()
            md_optim.step()

            m = shared_mem.get(f"{pid}_{connect_id}")
            if m and m == ProcessStatusType.Kill.value:
                return md

            if epoch % 100 == 0:
                msg = generate_running_message(
                    connect_id,
                    f"{element_name}节点执行进度: "
                    f"{(epoch/self.epoch) * 100:.3f}%, "
                    f"Loss: {loss.cpu().detach().item()}",
                )
                send_message(websocket, msg)
                print(f"Epoch:{epoch},Loss:{loss.cpu().detach().item()}")
        return md
