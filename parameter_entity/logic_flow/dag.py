from pydantic import BaseModel


class Dag(BaseModel):
    version_id: str | None
    dag: str | None
