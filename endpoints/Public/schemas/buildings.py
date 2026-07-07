from typing import List, Optional
from pydantic import BaseModel, Field

class BuildingCost(BaseModel):
    CommodityName: str
    CommodityTicker: str
    Weight: float
    Volume: float
    Amount: int

class Building(BaseModel):
    BuildingCosts: List[BuildingCost] = Field(..., description="List of materials required to build")
    BuildingId: str = Field(..., description="Unique identifier of the building")
    Name: str = Field(..., description="Name of the building")
    Ticker: str = Field(..., description="Ticker code for the building")
    Expertise: Optional[str] = Field(None, description="Category of expertise")
    Pioneers: int = Field(0, description="Pioneer capacity")
    Settlers: int = Field(0, description="Settler capacity")
    Technicians: int = Field(0, description="Technician capacity")
    Engineers: int = Field(0, description="Engineer capacity")
    Scientists: int = Field(0, description="Scientist capacity")
    AreaCost: int = Field(..., description="Area required on a planet")
    UserNameSubmitted: str = Field(..., description="Who submitted this data")
    Timestamp: str = Field(..., description="Last update timestamp")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "BuildingCosts": [
                        {
                            "CommodityName": "Basic Structural Elements",
                            "CommodityTicker": "BSE",
                            "Weight": 10.0,
                            "Volume": 10.0,
                            "Amount": 5
                        }
                    ],
                    "BuildingId": "xyz-123",
                    "Name": "Rig",
                    "Ticker": "RIG",
                    "Expertise": "Resource Extraction",
                    "Pioneers": 10,
                    "Settlers": 0,
                    "Technicians": 0,
                    "Engineers": 0,
                    "Scientists": 0,
                    "AreaCost": 20,
                    "UserNameSubmitted": "System",
                    "Timestamp": "2026-01-01T12:00:00Z"
                }
            ]
        }
    }
