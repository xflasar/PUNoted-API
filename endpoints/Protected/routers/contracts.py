import logging
from typing import Any, Dict, List, Optional

import orjson
from asyncpg import PostgresError
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse as DefaultJSONResponse, StreamingResponse

from auth import RequireAuth
from app.core.limiter import get_auth_key, limiter
from endpoints.Protected.repositories.contracts_repo import get_filtered_contracts, stream_contracts_csv

logger = logging.getLogger(__name__)

contracts_router = APIRouter()

class ORJSONResponse(DefaultJSONResponse):
    media_type = "application/json"
    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)

# ==============================================================================
# 1. MAIN CONTRACTS - LIST (Multi-User, Paginated JSON)
# ==============================================================================
@contracts_router.get("/")
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_contracts(
    request: Request,
    contracttype: Optional[str] = Query(None, description="Filter by LOAN, SHIPMENT, BUY, SELL"),
    status: Optional[str] = Query(None, description="Filter by FULFILLED, etc."),
    partnercode: Optional[str] = Query(None, description="Filter by company code"),
    party: Optional[str] = Query(None, description="CUSTOMER or PROVIDER"),
    localid: Optional[str] = Query(None, description="Specific Contract Local ID"),
    limit: int = Query(50, ge=1, le=200),
    page: int = Query(1, ge=1),
    usernames: Optional[str] = Query(None, description="Target users or leave blank to get token owner data."),
    user_id: str = Depends(RequireAuth(["contracts:read"]))
):
    pool = request.app.state.db.pool
    valid_targets = getattr(request.state, "valid_target_users", [])
    
    if not valid_targets:
        return Response(content='[]', media_type="application/json")

    async with pool.acquire() as conn:
        json_data = await get_filtered_contracts(
            conn,
            valid_targets,
            c_type=contracttype,
            status=status,
            partner_code=partnercode,
            party=party,
            local_id=localid,
            limit=limit,
            page=page
        )

    return Response(content=json_data, media_type="application/json")


# ==============================================================================
# 2. MAIN CONTRACTS - SINGLE USER (Unwrapped List, Paginated JSON)
# ==============================================================================
@contracts_router.get("/user", response_class=ORJSONResponse)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_contract_user(
    request: Request,
    username: Optional[str] = Query(None, description="Target user or leave blank to get token owner data."),
    contracttype: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    partnercode: Optional[str] = Query(None),
    party: Optional[str] = Query(None),
    localid: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
    page: int = Query(1, ge=1),
    user_id: str = Depends(RequireAuth(["contracts:read"]))
):
    pool = request.app.state.db.pool
    
    target_user = getattr(request.state, "valid_target_users", [])

    async with pool.acquire() as conn:
        json_str = await get_filtered_contracts(
            conn,
            [target_user],
            c_type=contracttype,
            status=status,
            partner_code=partnercode,
            party=party,
            local_id=localid,
            limit=limit,
            page=page
        )
        
        try:
            data_list = orjson.loads(json_str)
            if data_list and "Contracts" in data_list[0]:
                return data_list[0]["Contracts"]
            return []
        except Exception:
            return []

# ==============================================================================
# 3. CSV EXPORT - MULTI USER (Streamed)
# ==============================================================================
@contracts_router.get("/csv")
async def export_contracts_csv(
    request: Request,
    contracttype: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    partnercode: Optional[str] = Query(None),
    party: Optional[str] = Query(None),
    user_id: str = Depends(RequireAuth(["contracts:read"]))
):
    pool = request.app.state.db.pool
    valid_targets = getattr(request.state, "valid_target_users", [])
    if not valid_targets: return Response(status_code=403)

    async def iter_csv():
        async with pool.acquire() as conn:
            generator = stream_contracts_csv(
                conn,
                valid_targets,
                c_type=contracttype,
                status=status,
                partner_code=partnercode,
                party=party
            )
            async for line in generator:
                yield line

    filename = f"contracts_export_{contracttype or 'all'}.csv"
    return StreamingResponse(
        iter_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

# ==============================================================================
# 4. CSV EXPORT - SINGLE USER (Streamed)
# ==============================================================================
@contracts_router.get("/user/csv")
async def export_user_csv(
    request: Request,
    username: Optional[str] = Query(None),
    contracttype: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    partnercode: Optional[str] = Query(None),
    party: Optional[str] = Query(None),
    user_id: str = Depends(RequireAuth(["contracts:read"]))
):
    pool = request.app.state.db.pool
    target_user = username or getattr(request.state, "valid_target_users", [])

    async def iter_csv():
        async with pool.acquire() as conn:
            generator = stream_contracts_csv(
                conn,
                [target_user],
                c_type=contracttype,
                status=status,
                partner_code=partnercode,
                party=party
            )
            async for line in generator:
                yield line

    filename = f"contracts_{target_user}_{contracttype or 'all'}.csv"
    return StreamingResponse(
        iter_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )