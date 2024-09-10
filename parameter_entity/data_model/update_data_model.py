from pydantic import BaseModel


class UpdateDataModel(BaseModel):
    table_id: str | None
    data: dict | None
    rowid_list: list[str] | None = None
