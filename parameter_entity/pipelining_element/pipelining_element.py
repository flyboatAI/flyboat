from pydantic import BaseModel


class PipeliningElement(BaseModel):
    id: str | None
