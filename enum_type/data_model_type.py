from enum import Enum


class DataModelType(str, Enum):
    Datatable = "datatable"
    Manual = "manual"
    AutoCreate = "autocreate"
    File = "file"
