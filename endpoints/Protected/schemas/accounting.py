from typing import List, Optional
from pydantic import BaseModel, Field

class CurrencyAccount(BaseModel):
    Currency: str
    Balance: float
    BookBalance: float
    Category: str
    Type: str
    AccountNumber: str
    LastUpdatedEpochMs: float

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "Currency": "ICA",
                    "Balance": 1000.50,
                    "BookBalance": 1000.50,
                    "Category": "CASH",
                    "Type": "asset",
                    "AccountNumber": "ICA-12345",
                    "LastUpdatedEpochMs": 1700000000000
                }
            ]
        }
    }

class UserAccounting(BaseModel):
    Username: str
    Accounts: List[CurrencyAccount]
