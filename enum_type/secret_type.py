from enum import Enum


class SecretType(int, Enum):
    Pipelining = 0
    App = 1
