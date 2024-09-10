from pydantic import BaseModel


class ManualDataModel(BaseModel):
    data_model_name: str | None
    data_model_description: str | None = None
    data: list[dict] | None
    fields: list[dict] | None
