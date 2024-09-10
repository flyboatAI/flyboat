from enum import Enum


class PipeliningOperationType(str, Enum):
    Publish = "publish"
    CancelPublish = "cancel_publish"
    Call = "call"
