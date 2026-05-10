# app/schemas/internal_planner.py
from typing import Dict, List, Optional

from pydantic import BaseModel


class BuildingRequirementDTO(BaseModel):
    ticker: str
    amount: int

class InternalBuildingDTO(BaseModel):
    id: str
    ticker: str
    name: str
    type: str
    area: int
    category: Optional[str] = None
    buildReq: List[BuildingRequirementDTO]
    workers: Optional[Dict[str, int]] = None
    supply: Optional[Dict[str, int]] = None
    storageWeight: Optional[int] = None
    storageVolume: Optional[int] = None

class RecipeIODTO(BaseModel):
    ticker: str
    amount: float

class InputRecipeDTO(BaseModel):
    processid: str
    name: str
    durationmillis: int
    madeIn: str
    inputs: List[RecipeIODTO]
    outputs: List[RecipeIODTO]

class InternalMaterialDTO(BaseModel):
    ticker: str
    name: str
    resource: bool
    inputRecipes: List[InputRecipeDTO]
    requiredFor: List[str] # Used by frontend to determine if it's an end-product
    production_building: Optional[str] = None
