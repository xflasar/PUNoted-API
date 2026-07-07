from typing import List, Optional
from pydantic import BaseModel

class WorkforceNeed(BaseModel):
    Category: str
    Essential: bool
    MaterialId: str
    MaterialTicker: str
    Satisfaction: float
    UnitsPerInterval: float
    UnitsPerOneHundred: float

    model_config = {
        "extra": "allow"
    }

class WorkforcePopulation(BaseModel):
    WorkforceTypeName: str
    Population: int
    Reserve: int
    Capacity: int
    Required: int
    Satisfaction: float
    WorkforceNeeds: List[WorkforceNeed]

    model_config = {
        "extra": "allow"
    }

class Workforce(BaseModel):
    PlanetId: str
    PlanetNaturalId: str
    PlanetName: str
    SiteId: str
    UserNameSubmitted: str
    Timestamp: Optional[str] = None
    Workforces: List[WorkforcePopulation]

    model_config = {
        "extra": "allow"
    }

class UserWorkforce(BaseModel):
    Username: str
    Workforce: List[Workforce]
