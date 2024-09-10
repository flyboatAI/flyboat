from parameter_entity.data_model.manual_data_model import ManualDataModel


class FileDataModel(ManualDataModel):
    file_type: str | None = "xlsx"
