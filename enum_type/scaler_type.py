from enum import Enum


class ScalerType(str, Enum):
    No = "no"
    StandardScaler = "standard_scaler"
    MinMaxScaler = "min_max_scaler"
