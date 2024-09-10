from pydantic import BaseModel


class LicenseData(BaseModel):
    serial_number: str


class IdentifierAndExp(BaseModel):
    key: str
    exp: int
