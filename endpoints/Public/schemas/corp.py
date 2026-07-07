from typing import List, Optional
from pydantic import BaseModel, Field

class CorpPrice(BaseModel):
    ticker: str = Field(..., description="Material ticker")
    price: float = Field(..., description="Internal corporation price")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"ticker": "RAT", "price": 12.50},
                {"ticker": "DW", "price": 5.20}
            ]
        }
    }

class CorpMemberData(BaseModel):
    company_code: str = Field(..., description="Company code")
    company_name: str = Field(..., description="Company name")

class CorpMembersDetails(BaseModel):
    corporation_name: Optional[str] = Field(None, description="Corporation Name")
    corporation_code: Optional[str] = Field(None, description="Corporation Code")
    members: List[CorpMemberData] = Field(default_factory=list, description="List of corporation members")

class CorpMembersResponse(BaseModel):
    success: bool = Field(..., description="Request success status")
    data: CorpMembersDetails = Field(..., description="Corporation details")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "success": True,
                    "data": {
                        "corporation_name": "MegaCorp",
                        "corporation_code": "MEGA",
                        "members": [
                            {"company_code": "ABC", "company_name": "ABC Logistics"}
                        ]
                    }
                }
            ]
        }
    }
