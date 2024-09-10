from pydantic import BaseModel


class AppendDataModel(BaseModel):
    table_id: str | None
    data: list[dict] | None
    fields: list[dict] | None
