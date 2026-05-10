import logging
from typing import Any, Optional

import orjson
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse as DefaultJSONResponse

from app.core.limiter import get_auth_key, limiter
from auth import RequireAuth
from endpoints.Protected.repositories.user_repo import fetch_company_data

user_router = APIRouter()
logger = logging.getLogger("user_router")

class ORJSONResponse(DefaultJSONResponse):
    media_type = "application/json"
    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)

# ==============================================================================
# 1. COMPANY DATA - LIST
# ==============================================================================
@user_router.get(
    "/companydata",
    summary="Get Company Data",
    description="Search by Usernames, Company Codes, or Names. Scopes automatically to Group or Self."
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_company_data(
    request: Request,
    usernames: Optional[str] = Query(None, description="Comma-separated usernames"),
    codes: Optional[str] = Query(None, description="Comma-separated Company Codes"),
    names: Optional[str] = Query(None, description="Comma-separated Company Names"),
    user_id: str = Depends(RequireAuth(["profile:read"], is_single_user_endpoint=False)),
):
    pool = request.app.state.db.pool

    # 1. Get Validated Targets (Provided by Auth)
    valid_targets = getattr(request.state, "valid_target_users", [])
    if not valid_targets:
        return Response(content='[]', media_type="application/json")

    # 2. Parse Filters
    target_codes = [c.strip() for c in codes.split(",") if c.strip()] if codes else None
    target_names = [n.strip() for n in names.split(",") if n.strip()] if names else None

    # 3. Fetch
    async with pool.acquire() as conn:
        json_data = await fetch_company_data(
            conn,
            usernames=valid_targets,
            codes=target_codes,
            names=target_names
        )

    return Response(content=json_data, media_type="application/json")


# ==============================================================================
# 2. COMPANY DATA - SINGLE USER
# ==============================================================================
@user_router.get(
    "/companydata/user",
    summary="Get Single Company Data",
    description="Returns a single company object.",
    response_class=ORJSONResponse
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_company_data_user(
    request: Request,
    username: Optional[str] = Query(None, description="Specific username"),
    code: Optional[str] = Query(None, description="Specific Company Code"),
    name: Optional[str] = Query(None, description="Specific Company Name"),
    user_id: str = Depends(RequireAuth(["profile:read"], is_single_user_endpoint=True)),
):
    pool = request.app.state.db.pool

    # 1. Get Validated Targets
    valid_targets = getattr(request.state, "valid_target_users", [])
    if not valid_targets:
        raise HTTPException(status_code=404, detail="Company not found or access denied")

    target_codes = [code.strip()] if code else None
    target_names = [name.strip()] if name else None

    async with pool.acquire() as conn:
        json_str = await fetch_company_data(
            conn,
            usernames=valid_targets,
            codes=target_codes,
            names=target_names
        )

        try:
            data_list = orjson.loads(json_str)
            if data_list and "Company" in data_list[0]:
                return data_list[0]["Company"]
            return {}
        except Exception:
            return {}
