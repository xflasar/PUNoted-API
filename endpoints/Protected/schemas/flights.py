from typing import List, Optional
from pydantic import BaseModel

class FlightSegment(BaseModel):
    Type: str
    DepartureTimeEpochMs: Optional[float] = None
    ArrivalTimeEpochMs: Optional[float] = None
    StlDistance: Optional[float] = None
    StlFuelConsumption: Optional[float] = None
    FtlDistance: Optional[float] = None
    FtlFuelConsumption: Optional[float] = None
    Origin: Optional[str] = None
    Destination: Optional[str] = None

    model_config = {
        "extra": "allow"
    }

class Flight(BaseModel):
    FlightId: str
    ShipId: str
    ShipName: Optional[str] = None
    ShipRegistration: str
    DepartureTimeEpochMs: Optional[float] = None
    ArrivalTimeEpochMs: Optional[float] = None
    CurrentSegmentIndex: Optional[int] = None
    IsAborted: bool
    StlDistance: Optional[float] = None
    FtlDistance: Optional[float] = None
    UserNameSubmitted: str
    Origin: Optional[str] = None
    Destination: Optional[str] = None
    Segments: List[FlightSegment]

    model_config = {
        "extra": "allow"
    }

class UserFlights(BaseModel):
    Username: str
    Flights: List[Flight]
