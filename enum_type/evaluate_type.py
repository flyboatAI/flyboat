from enum import Enum


class EvaluateType(str, Enum):
    R2 = "r2"
    MSE = "mse"
    MAE = "mae"
