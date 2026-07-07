from typing import List, Optional
from pydantic import BaseModel

class CXOrder(BaseModel):
    OrderId: str
    DateEpochMs: float
    Ticker: str
    Type: str
    Status: str
    Price: float
    Currency: str
    Amount: float
    FilledAmount: float
    TotalValue: float

    model_config = {
        "extra": "allow"
    }

class UserCXOrders(BaseModel):
    Username: str
    Orders: List[CXOrder]
