from enum import Enum


class ElementConfigType(str, Enum):
    Input = "input"
    Output = "output"
    ModelOutput = "model_output"
