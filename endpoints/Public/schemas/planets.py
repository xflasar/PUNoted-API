from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

class PlanetMinimal(BaseModel):
    PlanetNaturalId: str
    PlanetName: str

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"PlanetNaturalId": "VH-331a", "PlanetName": "Promitor"}
            ]
        }
    }

class PlanetFull(BaseModel):
    PlanetNaturalId: str
    PlanetName: str
    PlanetId: str
    Resources: List[Dict[str, Any]]
    BuildRequirements: List[Dict[str, Any]]
    ProductionFees: List[Dict[str, Any]]
    Gravity: Optional[float] = None
    Temperature: Optional[float] = None
    Fertility: Optional[float] = None
    HasLocalMarket: bool
    HasChamberOfCommerce: bool
    HasWarehouse: bool
    HasAdministrationCenter: bool
    HasShipyard: bool

    model_config = {
        "extra": "allow"
    }
