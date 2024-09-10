from pydantic import BaseModel


class ExecuteModelExperiment(BaseModel):
    experiment_id: str | None
    publish_id: str
    params: dict
    origin_params: list[dict]
