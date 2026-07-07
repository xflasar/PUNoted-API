from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

class Material(BaseModel):
    Ticker: str
    Name: str
    Category: str
    Weight: float
    Volume: float

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "Ticker": "RAT",
                    "Name": "Rations",
                    "Category": "Consumables",
                    "Weight": 1.0,
                    "Volume": 1.0
                }
            ]
        }
    }

class Recipe(BaseModel):
    # Depending on DB payload, flexible definition
    BuildingTicker: Optional[str] = None
    RecipeName: Optional[str] = None
    Inputs: Optional[List[Dict[str, Any]]] = None
    Outputs: Optional[List[Dict[str, Any]]] = None
    
    model_config = {
        "extra": "allow"
    }
