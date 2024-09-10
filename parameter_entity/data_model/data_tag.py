from pydantic import BaseModel


class DataTag(BaseModel):
    table_id: str | None
    field: dict | None
