from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel


class BankCreate(BaseModel):
    name: str
    headline: Optional[str] = None
    description: Optional[str] = None
    liquidity: float = 0.0
    default_interest_rate: float = 5.0
    currency: Optional[str] = "ICA"
    default_repay_days: Optional[int] = 14
    default_interval: Optional[str] = "At Maturity"
    min_loan_amount: Optional[float] = 0.0
    max_loan_amount: Optional[float] = 0.0
    public_show_loans: Optional[bool] = True
    public_show_deposits: Optional[bool] = True
    public_show_withdrawals: Optional[bool] = True
    public_show_names: Optional[bool] = False
    is_public: Optional[bool] = True
    max_interest_rate: Optional[float] = None
    weekly_yield_rate: Optional[float] = 1.5
    max_weekly_yield_rate: Optional[float] = None
    deposit_status: Optional[str] = "OPEN"
    deposit_strategy: Optional[str] = "BALANCED"


class BankUpdate(BaseModel):
    headline: Optional[str] = None
    description: Optional[str] = None
    default_interest_rate: Optional[float] = None
    max_interest_rate: Optional[float] = None
    weekly_yield_rate: Optional[float] = None
    max_weekly_yield_rate: Optional[float] = None
    deposit_status: Optional[str] = None
    deposit_strategy: Optional[str] = None
    public_show_loans: Optional[bool] = None
    public_show_deposits: Optional[bool] = None
    public_show_withdrawals: Optional[bool] = None
    public_show_names: Optional[bool] = None
    is_public: Optional[bool] = None


class BankResponse(BaseModel):
    id: int
    name: str
    headline: Optional[str] = None
    owner_username: str
    liquidity: float
    default_interest_rate: float
    max_interest_rate: Optional[float] = None
    weekly_yield_rate: Optional[float] = 1.5
    max_weekly_yield_rate: Optional[float] = None
    deposit_status: Optional[str] = "OPEN"
    deposit_strategy: Optional[str] = "BALANCED"
    description: Optional[str] = None
    created_at: datetime
    active_loans_count: Optional[int] = 0
    total_loans_count: Optional[int] = 0
    total_lent_amount: Optional[float] = 0.0
    active_lent_amount: Optional[float] = 0.0
    currency: Optional[str] = "ICA"
    default_repay_days: Optional[int] = 14
    default_interval: Optional[str] = "At Maturity"
    min_loan_amount: Optional[float] = 0.0
    max_loan_amount: Optional[float] = 0.0
    total_deposit_amount: Optional[float] = 0.0
    total_withdrawal_amount: Optional[float] = 0.0
    depositors_count: Optional[int] = 0
    public_show_loans: Optional[bool] = True
    public_show_deposits: Optional[bool] = True
    public_show_withdrawals: Optional[bool] = True
    public_show_names: Optional[bool] = False
    is_public: Optional[bool] = True
    owner_is_synchronized: Optional[bool] = False
    owner_last_active: Optional[datetime] = None


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
    partner_code: Optional[str] = None
    partner_name: Optional[str] = None
    contract_status: Optional[str] = None
    contract_duedate: Optional[datetime] = None
    currency: Optional[str] = None
    transaction_type: Optional[str] = "LOAN"
    is_verified: Optional[bool] = False


class LoanRequestAction(BaseModel):
    loan_id: int
    status: str
    contract_id: Optional[str] = None


class LoanLinkContract(BaseModel):
    bank_id: int
    contract_localid: str
    interest_rate: Optional[float] = None
    term_days: Optional[int] = None
    transaction_type: Optional[str] = "LOAN"


class ManualDepositPayload(BaseModel):
    bank_id: int
    amount: float
    username: Optional[str] = None
    transaction_type: str = "DEPOSIT"
    note: Optional[str] = None
    update_liquidity: bool = True


class BankCsvImportPayload(BaseModel):
    bank_id: int
    csv_content: str


class BankCsvImportResponse(BaseModel):
    imported: int
    verified: int
    unverified: int
    errors: List[str] = []


class DepositorHistoryPoint(BaseModel):
    date: str
    type: str
    amount: float
    balance: float


class DepositorWeeklySnapshot(BaseModel):
    week_date: str
    starting_balance: float
    net_deposits: float
    effective_capital: float
    yield_earned: float
    closing_balance: float
    projected_closing_balance: Optional[float] = None
    is_current: Optional[bool] = False


class DepositorSummary(BaseModel):
    username: str
    total_deposited: float
    total_withdrawn: float
    current_balance: float
    first_deposit_date: Optional[str] = None
    last_activity_date: Optional[str] = None
    transactions_count: int
    history: List[DepositorHistoryPoint] = []
    weekly_snapshots: List[DepositorWeeklySnapshot] = []
    is_owner: Optional[bool] = False
    equity_share_percent: Optional[float] = 0.0
    days_active_in_cycle: Optional[float] = 7.0
    cycle_proration_factor: Optional[float] = 1.0
    weekly_yield_rate: Optional[float] = 1.5
    effective_capital: Optional[float] = 0.0
    projected_weekly_payout: Optional[float] = 0.0


class BankWeeklyHistoryPoint(BaseModel):
    week_date: str
    total_pool: float
    owner_equity: float
    client_deposits: float
    total_yield_distributed: float
    depositors_count: int
    is_current: Optional[bool] = False


class BankYieldMetrics(BaseModel):
    total_pool: float = 0.0
    owner_equity: float = 0.0
    owner_equity_percent: float = 0.0
    client_deposits: float = 0.0
    client_deposits_percent: float = 0.0
    weekly_yield_rate: float = 1.5
    cycle_start_date: str = ""
    next_distribution_date: str = ""
    days_elapsed_in_cycle: float = 0.0
    cycle_progress_percent: float = 0.0
    est_weekly_payout_pool: float = 0.0
    weekly_history: List[BankWeeklyHistoryPoint] = []


class BankDepositorsResponse(BaseModel):
    metrics: BankYieldMetrics
    depositors: List[DepositorSummary]


class YieldDistributionPayload(BaseModel):
    bank_id: int
    min_weekly_apy: Optional[float] = 1.0
    max_weekly_apy: Optional[float] = 2.5


class YieldDistributionDetail(BaseModel):
    username: str
    balance: float
    days_active: int
    dynamic_apy: float
    payout: float


class YieldDistributionResponse(BaseModel):
    success: bool
    total_distributed: float
    depositors_paid_count: int
    owner_retained_profit: float
    details: List[YieldDistributionDetail] = []
