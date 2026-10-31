from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from app.core.limiter import get_auth_key, get_public_key, limiter
from auth import OptionalAuth
from models.banking_schema import BankResponse
from repositories.banking_repo import BankingRepository
from services.internal.banking_service import BankingService

public_banking_router = APIRouter()


def get_banking_service(request: Request) -> BankingService:
    return BankingService(BankingRepository(request.app.state.db))


@public_banking_router.get(
    "",
    summary="Public Bank Directory",
    description="Retrieve all publicly listed player banks and their current lending parameters, yield rates, and active pool stats.",
    response_model=List[BankResponse],
)
@public_banking_router.get(
    "/",
    include_in_schema=False,
    response_model=List[BankResponse],
)
@limiter.limit("120/minute", key_func=get_auth_key)
@limiter.limit("60/minute", key_func=get_public_key)
async def get_public_banks(
    request: Request,
    user_id: Optional[str] = Depends(OptionalAuth()),
    service: BankingService = Depends(get_banking_service)
):
    return await service.get_banks(public_only=True)


@public_banking_router.get(
    "/{bank_id}",
    summary="Public Bank Details",
    description="Retrieve detailed public statistics and loan terms for a specific player bank.",
    response_model=BankResponse,
)
@limiter.limit("120/minute", key_func=get_auth_key)
@limiter.limit("60/minute", key_func=get_public_key)
async def get_public_bank_detail(
    bank_id: int,
    request: Request,
    user_id: Optional[str] = Depends(OptionalAuth()),
    service: BankingService = Depends(get_banking_service)
):
    bank = await service.get_bank_by_id(bank_id)
    if not bank or bank.get("is_public") is False:
        raise HTTPException(status_code=404, detail="Bank not found or not publicly listed")
    return bank
