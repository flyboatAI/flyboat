from enum import Enum


class ConnectType(str, Enum):
    Create = "create"
    Open = "open"
    Connecting = "connecting"
    Error = "error"
    Close = "close"
    Cancel = "cancel"
