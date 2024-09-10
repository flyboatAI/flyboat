from enum import Enum


class ResultCode(int, Enum):
    Success = 0
    Error = 1
    Cancel = 2
