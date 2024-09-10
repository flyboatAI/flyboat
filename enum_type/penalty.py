from enum import Enum


class Penalty(str, Enum):
    L1 = "l1"
    L2 = "l2"
    L1L2 = "l1l2"
