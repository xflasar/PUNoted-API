from typing import List
from repositories.contracts_repo import ContractsRepository
from models.contracts_schema import ContractListItem, ContractDetail, ContractCondition

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
                operation_type=op_type
            ))
        return {
            "items": results,
            "total": raw_data['total']
        }