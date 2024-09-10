from pydantic import BaseModel


class TestDataSourceLink(BaseModel):
    db_url: str
    db_user: str | None = None
    db_password: str | None = None
