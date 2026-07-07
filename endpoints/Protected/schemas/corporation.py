from typing import List, Optional
from pydantic import BaseModel, Field

class CorpProductionRow(BaseModel):
    CorpCode: str
    CompanyName: str
    PlanetName: str
    MaterialTicker: str
    Production: float
    Consumption: float

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "CorpCode": "MEGA",
                    "CompanyName": "Alpha Logistics",
                    "PlanetName": "Promitor",
                    "MaterialTicker": "RAT",
                    "Production": 100.5,
                    "Consumption": 20.0
                }
            ]
        }
    }

class CorpProductionOverviewResponse(BaseModel):
    success: bool
    data: List[CorpProductionRow]
