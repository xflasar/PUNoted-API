from typing import Any, Optional

import orjson
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse as DefaultJSONResponse

from app.core.limiter import get_auth_key, limiter
from auth import RequireAuth
from endpoints.Protected.services.accounting_service import get_accounting_data
from endpoints.Protected.schemas.accounting import UserAccounting, CurrencyAccount
from typing import List

accounting_router = APIRouter()

class ORJSONResponse(DefaultJSONResponse):
    media_type = "application/json"
    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)

# ==============================================================================
# 1. LIST Endpoint (Multi-User) -> [{ "Username": "x", "Accounts": [...] }]
# ==============================================================================
@accounting_router.get(
    "/",
    summary="Get Currency Accounts",
    description="Returns accounts list. If no usernames provided, returns group data.",
    responses={200: {"model": List[UserAccounting]}}
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_accounting(
    request: Request,
    usernames: Optional[str] = Query(None, description="Comma-separated list of usernames"),
    currency: Optional[str] = Query(None, description="Filter by currency code (e.g. ICA, CIS)"),
    user_id: str = Depends(RequireAuth(["accounting:read"], is_single_user_endpoint=False)),
):
    db = request.app.state.db
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        return []

    accounting_data = await get_accounting_data(db, valid_targets, currency)
    return accounting_data


# ==============================================================================
# 2. SINGLE USER Endpoint (Unwrapped) -> [...] (Just the accounts list)
# ==============================================================================
@accounting_router.get(
    "/user",
    summary="Get Single User Accounts",
    description="Returns a flat list of accounts for a specific user, if no username provided, returns your own data.",
    response_class=ORJSONResponse,
    responses={200: {"model": List[CurrencyAccount]}}
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_accounting_user(
    request: Request,
    username: Optional[str] = Query(None, description="Specific username"),
    currency: Optional[str] = Query(None, description="Filter by currency code (e.g. ICA, CIS)"),
    user_id: str = Depends(RequireAuth(["accounting:read"], is_single_user_endpoint=True)),
):
    db = request.app.state.db
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        raise HTTPException(status_code=404, detail="User not found or access denied")

    accounting_data = await get_accounting_data(db, valid_targets, currency)

    if not accounting_data:
        return []
    
    if accounting_data and "Accounts" in accounting_data[0]:
        return accounting_data[0]["Accounts"]
    return []
