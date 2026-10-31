from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.core.limiter import get_auth_key, limiter
from auth import RequireAuth
from models.banking_schema import (
    BankDepositorsResponse,
    BankResponse,
    LoanRequestResponse,
)
from repositories.banking_repo import BankingRepository
from services.internal.banking_service import BankingService

protected_banking_router = APIRouter()


def get_banking_service(request: Request) -> BankingService:
    return BankingService(BankingRepository(request.app.state.db))


@protected_banking_router.get(
    "/my-bank",
    summary="Get User Bank",
    description="Retrieve the authenticated user's registered bank, liquidity, lending terms, and operational stats.",
    response_model=Optional[BankResponse],
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_my_bank(
    request: Request,
    user_id: str = Depends(RequireAuth(["banking:read"], is_single_user_endpoint=True)),
    service: BankingService = Depends(get_banking_service)
):
    valid_targets = getattr(request.state, "valid_target_users", [])
    if not valid_targets:
        raise HTTPException(status_code=403, detail="No authorized user targets found")
    username = valid_targets[0]
    return await service.get_bank_by_owner(username)


@protected_banking_router.get(
    "/loans/requested",
    summary="Get User Requested Loans",
    description="Retrieve all loan requests submitted by the authenticated user across player banks.",
    response_model=List[LoanRequestResponse],
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_requested_loans(
    request: Request,
    user_id: str = Depends(RequireAuth(["banking:read"], is_single_user_endpoint=True)),
    service: BankingService = Depends(get_banking_service)
):
    valid_targets = getattr(request.state, "valid_target_users", [])
    if not valid_targets:
        return []
    username = valid_targets[0]
    return await service.get_requested_loans(username)


@protected_banking_router.get(
    "/loans/received",
    summary="Get Bank Received Loan Requests",
    description="Retrieve incoming loan requests submitted to the authenticated user's bank.",
    response_model=List[LoanRequestResponse],
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_received_loans(
    bank_id: int = Query(..., description="The ID of the bank"),
    request: Request = None,
    user_id: str = Depends(RequireAuth(["banking:read"], is_single_user_endpoint=True)),
    service: BankingService = Depends(get_banking_service)
):
    valid_targets = getattr(request.state, "valid_target_users", [])
    bank = await service.get_bank_by_id(bank_id)
    if not bank:
        raise HTTPException(status_code=404, detail="Bank not found")
    if bank["owner_username"] not in valid_targets:
        raise HTTPException(status_code=403, detail="Not authorized to view loan requests for this bank")
    return await service.get_bank_loan_requests(bank_id)


@protected_banking_router.get(
    "/depositors",
    summary="Get Bank Depositors and Cycle Metrics",
    description="Retrieve the depositor roster, prorated dollar-days, and weekly distribution metrics for a bank.",
    response_model=BankDepositorsResponse,
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_bank_depositors(
    bank_id: int = Query(..., description="The ID of the bank"),
    request: Request = None,
    user_id: str = Depends(RequireAuth(["banking:read"], is_single_user_endpoint=True)),
    service: BankingService = Depends(get_banking_service)
):
    valid_targets = getattr(request.state, "valid_target_users", [])
    bank = await service.get_bank_by_id(bank_id)
    if not bank:
        raise HTTPException(status_code=404, detail="Bank not found")
    if bank["owner_username"] not in valid_targets:
        raise HTTPException(status_code=403, detail="Not authorized to view depositors for this bank")
    return await service.get_bank_depositors(bank_id)
