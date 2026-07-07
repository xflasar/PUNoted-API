from typing import List, Optional, Any, Dict
from pydantic import BaseModel

class StorageItem(BaseModel):
    MaterialId: Optional[str] = None
    MaterialName: Optional[str] = None
    MaterialTicker: Optional[str] = None
    MaterialCategory: Optional[str] = None
    MaterialAmount: float
    MaterialWeight: float
    MaterialVolume: float
    Type: str
    
    model_config = {
        "extra": "allow"
    }

class Storage(BaseModel):
    StorageId: str
    AddressableId: str
    Name: str
    WeightCapacity: float
    VolumeCapacity: float
    WeightLoad: float
    VolumeLoad: float
    Type: str
    Timestamp: Optional[str] = None
    StorageItems: List[StorageItem]

    model_config = {
        "extra": "allow"
    }

class UserStorages(BaseModel):
    Username: str
    Storages: List[Storage]
