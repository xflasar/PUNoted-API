from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class ContractCondition(BaseModel):
    id: str
    type: str
    status: str
    index: int
    party: Optional[str] = None
    deadline: Optional[datetime] = None
    amountmoney: Optional[float] = None
    currencymoney: Optional[str] = None
    interestamount: Optional[float] = None
    currency: Optional[str] = None
    repaymentamount: Optional[float] = None
    totalamount: Optional[float] = None
    implied_interest_rate: Optional[float] = None
    material_summary: Optional[str] = None
    addresssystemid: Optional[str] = None
    addressplanetid: Optional[str] = None
    addressstationid: Optional[str] = None
    destinationsystemid: Optional[str] = None
    destinationplanetid: Optional[str] = None
    destinationstationid: Optional[str] = None
    reputationchange: Optional[float] = None
    addresssystemname: Optional[str] = None
    addressplanetname: Optional[str] = None
    addressstationname: Optional[str] = None
    destinationsystemname: Optional[str] = None
    destinationplanetname: Optional[str] = None
    destinationstationname: Optional[str] = None

class ContractListItem(BaseModel):
    id: str
    localid: Optional[str] = None
    name: Optional[str] = None
    date: datetime
    status: str
    extensiondeadline: Optional[int] = None
    contracttype: Optional[str] = None
    partnername: Optional[str] = None
    partnercode: Optional[str] = None
    duedate: Optional[datetime] = None
    implied_interest_rate: Optional[float] = None
    installment_count: Optional[int] = None
    installment_done: Optional[int] = None
    installment_interval: Optional[int] = None
    loan_strategy: Optional[str] = None
    party: str = "UNKNOWN" 
    is_income: bool = False 
    total_amount: float = 0.0
    currency: str = "ICA"
    operation_type: str = "UNKNOWN"

class ContractDetail(ContractListItem):
    preamble: Optional[str] = None
    conditions: List[ContractCondition] = []

class ContractFilter(BaseModel):
    category: str = "ALL" # ALL, TRADE, SHIPMENT, LOAN
    status: Optional[str] = None
    search: Optional[str] = None
    page: int = 1
    limit: int = 20

class ContractStats(BaseModel):
    total_count: int
    status_counts: dict
    total_revenue: float
    total_expenses: float
    net_value: float
    history: List[dict] # {date: str, count: int}

class WeeklyFinancials(BaseModel):
    revenue: float
    expenses: float
    net: float
    count: int

class ContractDashboardStats(BaseModel):
    current_week: WeeklyFinancials
    last_week: WeeklyFinancials
    total_active: int
    active_breached: int # Currently breached but active

class DashboardWidgetLists(BaseModel):
    immediate: List[ContractListItem]
    active: List[ContractListItem]
    breached: List[ContractListItem]

class PaginatedContractList(BaseModel):
    items: List[ContractListItem]
    total: int

class LoansList(BaseModel):
    items: List[ContractListItem]
    total: int


class BankCreate(BaseModel):
    name: str
    description: Optional[str] = None
    liquidity: float = 0.0
    default_interest_rate: float = 5.0


class BankResponse(BaseModel):
    id: int
    name: str
    owner_username: str
    liquidity: float
    default_interest_rate: float
    description: Optional[str] = None
    created_at: datetime
    active_loans_count: Optional[int] = 0


class LoanRequestCreate(BaseModel):
    bank_id: int
    amount: float
    interest_rate: float
    term_days: int


class LoanRequestResponse(BaseModel):
    id: int
    bank_id: int
    requester_username: str
    amount: float
    interest_rate: float
    term_days: int
    status: str
    contract_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    bank_name: Optional[str] = None


class LoanRequestAction(BaseModel):
    loan_id: int
    status: str
    contract_id: Optional[str] = None

