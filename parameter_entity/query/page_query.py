from pydantic import BaseModel


class PageQuery(BaseModel):
    current: int = 1
    size: int = 10
