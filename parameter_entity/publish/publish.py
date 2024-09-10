from pydantic import BaseModel


class Publish(BaseModel):
    id: str | None = None
    version_id: str | None = None
    publish_id: str | None = None
