import numpy as np
import pandas as pd

from enum_type.role_type import RoleType
from error.data_process_error import DataProcessError


def convert_data_gm1n(train_data, role_setting_arr, test_data):
    x_role_arr = [x.get("name") for x in role_setting_arr if x.get("role_type") == RoleType.X.value]
    y_role_arr = [x.get("name") for x in role_setting_arr if x.get("role_type") == RoleType.Y.value]
    train_data = pd.DataFrame(train_data)
    test_data = pd.DataFrame(test_data)
    if not y_role_arr:
        raise DataProcessError("只能设置一个因变量") from None
    test_data[y_role_arr[0]] = [np.nan for _ in range(len(test_data))]
    # 拼接
    train_data = pd.concat((train_data, test_data), axis=0, ignore_index=True)
    return x_role_arr, y_role_arr, train_data
