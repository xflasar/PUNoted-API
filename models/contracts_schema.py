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
    # Material info flattened for summary
    material_summary: Optional[str] = None

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

    # Computed fields
    total_amount: float = 0.0
    currency: str = "ICA"
    operation_type: str = "UNKNOWN" # BUY vs SELL based on party

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
