from typing import List, Optional
from pydantic import BaseModel

class SiteMaterial(BaseModel):
    MaterialId: str
    MaterialName: str
    MaterialTicker: str
    MaterialAmount: float

    model_config = {
        "extra": "allow"
    }

class SiteBuilding(BaseModel):
    SiteBuildingId: str
    BuildingId: str
    BuildingCreated: Optional[float] = None
    BuildingName: str
    BuildingTicker: str
    BuildingLastRepair: Optional[float] = None
    Condition: float
    ReclaimableMaterials: Optional[List[SiteMaterial]] = None
    RepairMaterials: Optional[List[SiteMaterial]] = None

    model_config = {
        "extra": "allow"
    }

class Site(BaseModel):
    SiteId: str
    PlanetId: str
    PlanetIdentifier: str
    PlanetName: str
    PlanetFoundedEpochMs: Optional[float] = None
    InvestedPermits: int
    MaximumPermits: int
    UserNameSubmitted: str
    Timestamp: Optional[str] = None
    Buildings: Optional[List[SiteBuilding]] = None

    model_config = {
        "extra": "allow"
    }

class UserSites(BaseModel):
    Username: str
    Sites: List[Site]
