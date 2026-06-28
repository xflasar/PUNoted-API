
from typing import List, Optional
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request

from app.core.security import require_internal_origin
from auth import get_current_user_id
from models.contracts_schema import (
    ContractDashboardStats,
    ContractDetail,
    ContractFilter,
    ContractStats,
    DashboardWidgetLists,
    LoansList,
    PaginatedContractList,
    BankCreate,
    BankResponse,
    LoanRequestCreate,
    LoanRequestResponse,
    LoanRequestAction,
)
from repositories.contracts_repo import ContractsRepository
from services.internal.contracts_service import ContractsService

contracts_router = APIRouter(dependencies=[Depends(require_internal_origin)])

def get_service(request: Request):
    return ContractsService(ContractsRepository(request.app.state.db))

@contracts_router.post("/list", response_model=PaginatedContractList)
async def list_contracts(
    filters: ContractFilter = Body(default_factory=ContractFilter),
    user_id: str = Depends(get_current_user_id),
    service: ContractsService = Depends(get_service)
):
    return await service.get_contracts_list(user_id, filters)

@contracts_router.post("/loans", response_model=LoansList)
async def loans_contracts(
    status: str = Query(None, description="Filter by loan status"),
    search: str = Query(None, description="Search for loans by partnername or partnercode or loan localid."),
    user_id: str = Depends(get_current_user_id),
    service: ContractsService = Depends(get_service)
):
    return await service.repo.get_loans(user_id, status, search)

@contracts_router.get("/detail", response_model=ContractDetail, response_model_exclude_none=True)
async def get_contract_detail(
    contract_id: str = Query(..., description="The ID of the contract"),
    user_id: str = Depends(get_current_user_id),
    service: ContractsService = Depends(get_service)
):
    result = await service.repo.get_contract_detail(contract_id, user_id)

    # Check if result is None
    if not result:
        raise HTTPException(status_code=404, detail="Contract not found")

    return result

@contracts_router.get("/stats", response_model=ContractStats)
async def get_contract_stats(
    user_id: str = Depends(get_current_user_id),
    service: ContractsService = Depends(get_service)
):
    return await service.repo.get_stats(user_id)

@contracts_router.get("/dashboard-stats", response_model=ContractDashboardStats)
async def get_dashboard_stats(
    user_id: str = Depends(get_current_user_id),
    service: ContractsService = Depends(get_service)
):
    return await service.repo.get_dashboard_stats(user_id)

@contracts_router.get("/dashboard-widgets", response_model=DashboardWidgetLists)
async def get_dashboard_widgets(
    user_id: str = Depends(get_current_user_id),
    service: ContractsService = Depends(get_service)
):
    return await service.repo.get_dashboard_widgets(user_id)


@contracts_router.post("/banks", response_model=BankResponse)
async def create_player_bank(
    data: BankCreate,
    user_id: str = Depends(get_current_user_id),
    service: ContractsService = Depends(get_service)
):
    async with service.repo.db.pool.acquire() as conn:
        owner_username = await conn.fetchval("SELECT username FROM users WHERE accountid = $1", user_id)
        if not owner_username:
            raise HTTPException(status_code=404, detail="User username not found")
    
    existing = await service.get_bank_by_owner(owner_username)
    if existing:
        raise HTTPException(status_code=400, detail="You already own a bank")
        
    return await service.create_bank(owner_username, data.name, data.description, data.liquidity, data.default_interest_rate)


@contracts_router.get("/banks", response_model=List[BankResponse])
async def list_player_banks(
    service: ContractsService = Depends(get_service)
):
    return await service.get_banks()


@contracts_router.get("/banks/my-bank", response_model=Optional[BankResponse])
async def get_my_bank(
    user_id: str = Depends(get_current_user_id),
    service: ContractsService = Depends(get_service)
):
    async with service.repo.db.pool.acquire() as conn:
        owner_username = await conn.fetchval("SELECT username FROM users WHERE accountid = $1", user_id)
        if not owner_username:
            return None
    return await service.get_bank_by_owner(owner_username)


@contracts_router.post("/banks/request", response_model=LoanRequestResponse)
async def request_bank_loan(
    data: LoanRequestCreate,
    user_id: str = Depends(get_current_user_id),
    service: ContractsService = Depends(get_service)
):
    async with service.repo.db.pool.acquire() as conn:
        requester_username = await conn.fetchval("SELECT username FROM users WHERE accountid = $1", user_id)
        if not requester_username:
            raise HTTPException(status_code=404, detail="User username not found")
            
    return await service.create_loan_request(requester_username, data.bank_id, data.amount, data.interest_rate, data.term_days)


@contracts_router.get("/banks/loans/requested", response_model=List[LoanRequestResponse])
async def get_my_requested_loans(
    user_id: str = Depends(get_current_user_id),
    service: ContractsService = Depends(get_service)
):
    async with service.repo.db.pool.acquire() as conn:
        username = await conn.fetchval("SELECT username FROM users WHERE accountid = $1", user_id)
        if not username:
            return []
    return await service.get_requested_loans(username)


@contracts_router.get("/banks/loans/received", response_model=List[LoanRequestResponse])
async def get_bank_received_loans(
    bank_id: int = Query(..., description="The ID of the bank"),
    user_id: str = Depends(get_current_user_id),
    service: ContractsService = Depends(get_service)
):
    async with service.repo.db.pool.acquire() as conn:
        username = await conn.fetchval("SELECT username FROM users WHERE accountid = $1", user_id)
        owner_username = await conn.fetchval("SELECT owner_username FROM player_banks WHERE id = $1", bank_id)
        if not username or username != owner_username:
            raise HTTPException(status_code=403, detail="You do not own this bank")
            
    return await service.get_bank_loan_requests(bank_id)


@contracts_router.post("/banks/loans/action")
async def action_bank_loan_request(
    data: LoanRequestAction,
    user_id: str = Depends(get_current_user_id),
    service: ContractsService = Depends(get_service)
):
    async with service.repo.db.pool.acquire() as conn:
        username = await conn.fetchval("SELECT username FROM users WHERE accountid = $1", user_id)
        bank_id = await conn.fetchval("SELECT bank_id FROM bank_loan_requests WHERE id = $1", data.loan_id)
        if not bank_id:
            raise HTTPException(status_code=404, detail="Loan request not found")
        owner_username = await conn.fetchval("SELECT owner_username FROM player_banks WHERE id = $1", bank_id)
        if not username or username != owner_username:
            raise HTTPException(status_code=403, detail="You do not own this bank")
            
    success = await service.action_loan_request(data.loan_id, data.status, data.contract_id)
    return {"success": success}
