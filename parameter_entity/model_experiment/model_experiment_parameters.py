from pydantic import BaseModel


class ModelExperimentParameters(BaseModel):
    experiment_id: str | None
    publish_id: str | None = None
    params: list[dict] | None = None
