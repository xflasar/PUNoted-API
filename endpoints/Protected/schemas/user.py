from typing import List, Optional
from pydantic import BaseModel

class Company(BaseModel):
    CompanyId: str
    CompanyCode: str
    CompanyName: str
    CorporationId: Optional[str] = None
    CorporationCode: Optional[str] = None
    CorporationName: Optional[str] = None
    Timestamp: Optional[float] = None

    model_config = {
        "extra": "allow"
    }

class UserCompany(BaseModel):
    Username: str
    Company: Company
