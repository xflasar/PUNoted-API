from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

class PublicCompanyProfile(BaseModel):
    UserId: str = Field(..., description="Unique identifier for the user")
    Username: str = Field(..., description="Username of the player")
    CompanyId: str = Field(..., description="Unique identifier for the company")
    CompanyName: str = Field(..., description="Name of the company")
    CompanyCode: str = Field(..., description="1-4 letter ticker for the company")
    SubscriptionLevel: Optional[str] = Field(None, description="Current subscription tier")
    HighestTier: Optional[str] = Field(None, description="Highest subscription tier reached")
    Pioneer: bool = Field(..., description="Is the user a pioneer?")
    Moderator: bool = Field(..., description="Is the user a moderator?")
    Team: bool = Field(..., description="Is the user on the dev team?")
    Translator: bool = Field(..., description="Is the user a translator?")
    ActiveDaysPerWeek: Optional[int] = Field(None, description="Number of days active per week")
    CreatedTimestamp: str = Field(..., description="Account creation timestamp")
    Gifts: Dict[str, Any] = Field(..., description="Any gifts associated with the company")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "UserId": "12345",
                    "Username": "SpaceCEO",
                    "CompanyId": "abcd-efgh",
                    "CompanyName": "Space Corp",
                    "CompanyCode": "SPC",
                    "SubscriptionLevel": "PRO",
                    "HighestTier": "PRO",
                    "Pioneer": False,
                    "Moderator": False,
                    "Team": False,
                    "Translator": False,
                    "ActiveDaysPerWeek": 7,
                    "CreatedTimestamp": "2026-01-01T12:00:00Z",
                    "Gifts": {}
                }
            ]
        }
    }
