from typing import List, Optional
from pydantic import BaseModel

class Ship(BaseModel):
    ShipId: str
    Registration: str
    Name: Optional[str] = None
    FlightId: Optional[str] = None
    CommissioningTimeEpochMs: Optional[float] = None
    Condition: Optional[float] = None
    StlFuelFlowRate: Optional[float] = None
    Type: str
    Location: Optional[str] = None
    SystemId: Optional[str] = None
    PlanetId: Optional[str] = None
    StationId: Optional[str] = None

    model_config = {
        "extra": "allow"
    }

class UserShips(BaseModel):
    Username: str
    Ships: List[Ship]
