from enum import Enum


class LibraryType(str, Enum):
    Sklearn = "sklearn"
    Prophet = "prophet"
    Grey = "grey"
    Pytorch = "pytorch"
    ExponentialParameter = "exponential_parameter"
    LogarithmParameter = "logarithm_parameter"
    WeibullParameter = "weibull_parameter"
