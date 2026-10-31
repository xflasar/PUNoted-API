from typing import Optional, List, Dict, Any
from repositories.banking_repo import BankingRepository


class BankingService:
    def __init__(self, repo: BankingRepository):
        self.repo = repo

    async def create_bank(
        self,
        owner_username: str,
        name: str,
        description: str,
        liquidity: float,
        rate: float,
        currency: str = "ICA",
        default_repay_days: int = 14,
        default_interval: str = "At Maturity",
        min_loan_amount: float = 0.0,
        max_loan_amount: float = 0.0,
        headline: Optional[str] = None,
        is_public: bool = True,
    ) -> Dict[str, Any]:
        return await self.repo.create_bank(
            owner_username, name, description, liquidity, rate,
            currency, default_repay_days, default_interval, min_loan_amount, max_loan_amount, headline, is_public
        )

    async def update_bank(
        self,
        bank_id: int,
        headline: Optional[str] = None,
        description: Optional[str] = None,
        default_interest_rate: Optional[float] = None,
        public_show_loans: Optional[bool] = None,
        public_show_deposits: Optional[bool] = None,
        public_show_withdrawals: Optional[bool] = None,
        public_show_names: Optional[bool] = None,
        is_public: Optional[bool] = None,
        max_interest_rate: Optional[float] = None,
        weekly_yield_rate: Optional[float] = None,
        max_weekly_yield_rate: Optional[float] = None,
        deposit_status: Optional[str] = None,
        deposit_strategy: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self.repo.update_bank(
            bank_id, headline, description, default_interest_rate,
            public_show_loans, public_show_deposits, public_show_withdrawals, public_show_names, is_public,
            max_interest_rate, weekly_yield_rate, max_weekly_yield_rate, deposit_status, deposit_strategy
        )

    async def get_banks(self, public_only: bool = False) -> List[Dict[str, Any]]:
        return await self.repo.get_banks(public_only=public_only)

    async def get_bank_by_owner(self, owner_username: str) -> Optional[Dict[str, Any]]:
        return await self.repo.get_bank_by_owner(owner_username)

    async def get_bank_by_id(self, bank_id: int) -> Optional[Dict[str, Any]]:
        return await self.repo.get_bank_by_id(bank_id)

    async def create_loan_request(self, requester_username: str, bank_id: int, amount: float, interest_rate: float, term_days: int):
        return await self.repo.create_loan_request(bank_id, requester_username, amount, interest_rate, term_days)

    async def get_requested_loans(self, requester_username: str) -> List[Dict[str, Any]]:
        return await self.repo.get_requested_loans(requester_username)

    async def get_bank_loan_requests(self, bank_id: int) -> List[Dict[str, Any]]:
        return await self.repo.get_bank_loan_requests(bank_id)

    async def action_loan_request(self, loan_id: int, status: str, contract_id: Optional[str] = None) -> bool:
        return await self.repo.action_loan_request(loan_id, status, contract_id)

    async def link_contract_to_bank(self, bank_id: int, contract_localid: str, interest_rate: Optional[float] = None, term_days: Optional[int] = None, transaction_type: str = "LOAN"):
        return await self.repo.link_contract_to_bank(bank_id, contract_localid, interest_rate, term_days, transaction_type)

    async def import_bank_ledger_csv(self, bank_id: int, csv_content: str):
        return await self.repo.import_bank_ledger_csv(bank_id, csv_content)

    async def record_manual_deposit(self, bank_id: int, amount: float, username: Optional[str] = None, transaction_type: str = "DEPOSIT", note: Optional[str] = None, update_liquidity: bool = True):
        return await self.repo.record_manual_deposit(bank_id, amount, username, transaction_type, note, update_liquidity)

    async def get_bank_depositors(self, bank_id: int) -> Dict[str, Any]:
        return await self.repo.get_bank_depositors(bank_id)

    async def process_automatic_weekly_yields(self) -> Dict[str, Any]:
        return await self.repo.process_automatic_weekly_yields()
