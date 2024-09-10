from pydantic import BaseModel


class NotebookExperimentDetail(BaseModel):
    notebook_name: str | None = None
    notebook_id: str | None = None
