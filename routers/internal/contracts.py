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
async def list_loan_contracts(
    status: Optional[str] = Query("ALL"),
    search: Optional[str] = Query(""),
    filters: ContractFilter = Body(default_factory=ContractFilter),
    user_id: str = Depends(get_current_user_id),
    service: ContractsService = Depends(get_service)
):
    search_term = (filters.search if filters and filters.search else search) or ""
    status_filter = status or "ALL"
    return await service.repo.get_loans(user_id, status_filter, search_term)


@contracts_router.get("/detail", response_model=ContractDetail, response_model_exclude_none=True)
async def get_contract_detail(
    contract_id: Optional[str] = Query(None, description="The internal ID of the contract"),
    local_id: Optional[str] = Query(None, description="The local/in-game ID of the contract"),
    user_id: str = Depends(get_current_user_id),
    service: ContractsService = Depends(get_service)
):
    target_id = contract_id or local_id
    if not target_id:
        raise HTTPException(status_code=400, detail="Must provide either contract_id or local_id")
    detail = await service.repo.get_contract_detail(contract_id=target_id, user_id=user_id, local_id=local_id)
    if not detail:
        raise HTTPException(status_code=404, detail="Contract not found")
    return detail


@contracts_router.get("/stats", response_model=ContractStats)
async def get_contracts_stats(
    user_id: str = Depends(get_current_user_id),
    service: ContractsService = Depends(get_service)
):
    return await service.repo.get_contracts_stats(user_id)


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
