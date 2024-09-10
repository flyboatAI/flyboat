from enum import Enum


class ProcessStatusType(str, Enum):
    Running = "running"
    Kill = "kill"
