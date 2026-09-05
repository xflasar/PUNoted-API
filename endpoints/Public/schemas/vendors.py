from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

class VendorDetails(BaseModel):
    vendorid: str
    companycode: str
    companyname: str
    corpname: Optional[str] = None
    gamename: str
    isactive: bool
    activity: str
    cx: str

class VendorLocation(BaseModel):
    id: str
    location_name: str
    location_code: str
    available: float

class VendorOrder(BaseModel):
    orderid: Optional[str] = None
    materialticker: str
    ordertype: str
    fixedprice: float = 0.0
    location: List[VendorLocation] = Field(default_factory=list)
    price: Dict[str, float] = Field(default_factory=dict)
    available: float = 0.0

class VendorEntry(BaseModel):
    vendor: VendorDetails
    orders: List[VendorOrder]

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "vendor": {
                        "vendorid": "123",
                        "companycode": "ABC",
                        "companyname": "Alpha Corp",
                        "corpname": "MegaCorp",
                        "gamename": "AlphaOne",
                        "isactive": True,
                        "activity": "1d",
                        "cx": "AI1"
                    },
                    "orders": []
                }
            ]
        }
    }
