
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
