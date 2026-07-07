from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

class ContractCondition(BaseModel):
    ConditionId: str
    Type: str
    Party: str
    ConditionIndex: int
    Status: str
    
    model_config = {
        "extra": "allow"
    }

class ContractStats(BaseModel):
    LoanStrategy: Optional[str] = None
    InstallmentInterval: Optional[int] = 0
    InstallmentCount: Optional[int] = 0
    InstallmentDone: Optional[int] = 0
    InterestRate: Optional[float] = 0.0
    TotalInterestRate: Optional[float] = 0.0

class ContractDetails(BaseModel):
    ContractId: str
    ContractLocalId: str
    UserNameSubmitted: str
    Timestamp: Optional[str] = None
    DateEpochMs: Optional[float] = None
    Status: str
    Party: str
    PartnerCode: str
    PartnerName: str
    Conditions: List[ContractCondition]

    # Include stats directly based on the Python JSON structure
    LoanStrategy: Optional[str] = None
    InstallmentInterval: Optional[int] = 0
    InstallmentCount: Optional[int] = 0
    InstallmentDone: Optional[int] = 0
    InterestRate: Optional[float] = 0.0
    TotalInterestRate: Optional[float] = 0.0

    model_config = {
        "extra": "allow",
        "json_schema_extra": {
            "examples": [
                {
                    "ContractId": "abc-123",
                    "ContractLocalId": "LOAN-001",
                    "UserNameSubmitted": "SpaceCEO",
                    "Status": "FULFILLED",
                    "Party": "CUSTOMER",
                    "PartnerCode": "BANK",
                    "PartnerName": "Galactic Bank",
                    "Conditions": []
                }
            ]
        }
    }

class UserContracts(BaseModel):
    Username: str
    Contracts: List[ContractDetails]
