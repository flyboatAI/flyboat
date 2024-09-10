from enum import Enum


class DataProcessType(str, Enum):
    BoxCox = "boxcox"
    NpLog = "nplog"
