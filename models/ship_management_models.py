from typing import List, Optional, Any, Dict
from pydantic import BaseModel
from datetime import datetime

class Part(BaseModel):
    isAvailable: bool = False
    name: str
    quantity: int

class ShipTypePreset(BaseModel):
    id: Optional[int] = None
    name: str
    price: float
    priceCorp: float
    parts: List[Part]
    is_admin_preset: bool = False
    created_by: Optional[str] = None
    created_at: Optional[datetime] = None

    model_config = {
        "extra": "allow"
    }

class ShipOrder(BaseModel):
    id: Optional[int] = None
    corporation_id: str
    customer: str
    customer_company_code: Optional[str] = None
    shipType: ShipTypePreset  # snapshot
    price: float
    waitTimeDays: int
    status: str = "QUEUED"
    notes: Optional[str] = None
    completionDate: Optional[datetime] = None
    created_at: Optional[datetime] = None
    isOwner: bool = False
    isAdmin: bool = False

    model_config = {
        "extra": "allow"
    }

class ShipOrderCreate(BaseModel):
    corporation_id: str
    customer: str
    customer_company_code: Optional[str] = None
    ownerType: str
    ownerId: Optional[str] = None
    guestPin: Optional[str] = None
    shipType: ShipTypePreset
    price: float
    waitTimeDays: int
    notes: Optional[str] = None

class ShipOrderUpdate(BaseModel):
    guestPin: Optional[str] = None  # to verify guest
    status: Optional[str] = None
    notes: Optional[str] = None
    price: Optional[float] = None

class ShipTypePresetCreate(BaseModel):
    corporation_id: Optional[str] = None
    name: str
    price: float
    priceCorp: float
    parts: List[Part]
    is_admin_preset: bool = False

class ShipTypePresetUpdate(BaseModel):
    name: Optional[str] = None
    price: Optional[float] = None
    priceCorp: Optional[float] = None
    parts: Optional[List[Part]] = None
