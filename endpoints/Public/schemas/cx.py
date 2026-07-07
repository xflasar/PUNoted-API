from typing import List, Optional
from pydantic import BaseModel, Field

class CXPrice(BaseModel):
    Ticker: str
    MMBuy: Optional[float] = 0
    MMSell: Optional[float] = 0
    AI1_Average: Optional[float] = Field(0, alias="AI1-Average")
    AI1_AskAmt: Optional[float] = Field(0, alias="AI1-AskAmt")
    AI1_AskPrice: Optional[float] = Field(0, alias="AI1-AskPrice")
    AI1_AskAvail: Optional[float] = Field(0, alias="AI1-AskAvail")
    AI1_BidAmt: Optional[float] = Field(0, alias="AI1-BidAmt")
    AI1_BidPrice: Optional[float] = Field(0, alias="AI1-BidPrice")
    AI1_BidAvail: Optional[float] = Field(0, alias="AI1-BidAvail")
    # Using Extra kwargs or just defining a few to represent the shape
    # Since there are many exchanges, we allow extra fields
    model_config = {
        "extra": "allow",
        "populate_by_name": True,
        "json_schema_extra": {
            "examples": [
                {
                    "Ticker": "RAT",
                    "MMBuy": 10.5,
                    "MMSell": 12.0,
                    "AI1-Average": 11.2,
                    "AI1-AskPrice": 11.5
                }
            ]
        }
    }
