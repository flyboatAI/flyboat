from pydantic import BaseModel


class GeneralDataModel(BaseModel):
    table_name: str | None = None
    datasource_id: str | None = None
    table_id: str | None = None
    search_key: str | None = None
    current: int = 1
    size: int = 10
