from typing import List

from models.contracts_schema import ContractListItem
from repositories.contracts_repo import ContractsRepository


class ContractsService:
    def __init__(self, repo: ContractsRepository):
        self.repo = repo

    async def get_contracts_list(self, user_id: str, filter_params) -> List[ContractListItem]:
        offset = (filter_params.page - 1) * filter_params.limit
        raw_data = await self.repo.get_contracts(
            user_id,
            filter_params.category,
            filter_params.status,
            filter_params.search,
            filter_params.limit,
            offset
        )

        results = []
        results = []
        for row in raw_data['items']:
            op_type = row['contracttype']

            results.append(ContractListItem(
                id=row['id'],
                localid=row['localid'],
                name=row['name'],
                date=row['date'],
                status=row['status'],
                contracttype=row['contracttype'],
                partnername=row['partnername'],
                partnercode=row['partnercode'],
                duedate=row['duedate'],
                total_amount=float(row['total_amount']),
                currency=row['currency'] or "ICA",
                operation_type=op_type,
                party=row['party'] or "UNKNOWN",
                is_income=row['is_income'] 
            ))

        return {
            "items": results,
            "total": raw_data['total']
        }

    async def create_bank(self, owner_username: str, name: str, description: str, liquidity: float, rate: float):
        return await self.repo.create_bank(owner_username, name, description, liquidity, rate)

    async def get_banks(self):
        return await self.repo.get_banks()

    async def get_bank_by_owner(self, owner_username: str):
        return await self.repo.get_bank_by_owner(owner_username)

    async def create_loan_request(self, requester_username: str, bank_id: int, amount: float, interest_rate: float, term_days: int):
        return await self.repo.create_loan_request(bank_id, requester_username, amount, interest_rate, term_days)

    async def get_requested_loans(self, requester_username: str):
        return await self.repo.get_requested_loans(requester_username)

    async def get_bank_loan_requests(self, bank_id: int):
        return await self.repo.get_bank_loan_requests(bank_id)

    async def action_loan_request(self, loan_id: int, status: str, contract_id: str = None):
        return await self.repo.action_loan_request(loan_id, status, contract_id)

