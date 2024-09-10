from pydantic import BaseModel


class RelationDataModel(BaseModel):
    table_name: str | None
    datasource_id: str | None
    data_model_name: str | None
    data_model_description: str | None = None
