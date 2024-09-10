from enum import Enum


class SplitType(str, Enum):
    TrainTest = "train_test"
    TrainTestValid = "train_test_valid"
