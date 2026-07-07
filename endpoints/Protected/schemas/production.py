from typing import List, Optional, Dict
from pydantic import BaseModel

class ProductionMaterial(BaseModel):
    MaterialTicker: str
    MaterialAmount: float

    model_config = {
        "extra": "allow"
    }

class ProductionOrder(BaseModel):
    OrderId: str
    Created: Optional[str] = None
    Completion: Optional[str] = None
    DurationMs: Optional[float] = None
    Halted: bool
    Recurring: bool
    Completed: bool
    Started: Optional[str] = None
    Inputs: List[ProductionMaterial]
    Outputs: List[ProductionMaterial]
    CompletedPercentage: Optional[float] = None

    model_config = {
        "extra": "allow"
    }

class ProductionLine(BaseModel):
    ProductionLineId: str
    SiteId: str
    PlanetId: str
    PlanetNaturalId: str
    PlanetName: str
    Type: str
    Capacity: int
    Efficiency: float
    Condition: float
    UserNameSubmitted: str
    Orders: List[ProductionOrder]

    model_config = {
        "extra": "allow"
    }

class UserProduction(BaseModel):
    Username: str
    Production: List[ProductionLine]

class BurnRateMaterial(BaseModel):
    MaterialTicker: str
    Production: float
    Consumption: float
    Net: float

class UserBurnRates(BaseModel):
    Username: str
    BurnRates: Dict[str, List[BurnRateMaterial]]
