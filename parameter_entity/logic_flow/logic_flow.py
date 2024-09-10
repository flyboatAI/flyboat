from pydantic import BaseModel


class LogicFlow(BaseModel):
    version_id: str | None
    logic_flow: str | None
