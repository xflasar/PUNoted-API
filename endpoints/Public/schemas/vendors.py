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

class VendorOrder(BaseModel):
    orderid: str
    materialticker: str
    price: Dict[str, Any]
    ordertype: str
    locations: List[Dict[str, Any]]

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
