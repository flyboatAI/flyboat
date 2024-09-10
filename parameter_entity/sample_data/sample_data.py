from pydantic import BaseModel


class SampleDataModel(BaseModel):
    version_id: str | None
    element_id: str | None
    train: list[dict] | None = None
    test: list[dict] | None = None
    valid: list[dict] | None = None
    fields: list[dict] | None = None
