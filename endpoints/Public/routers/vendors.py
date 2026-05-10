# endpoints/Protected/routers/vendors.py

from typing import Optional

from fastapi import APIRouter, Depends, Query, Request, Response

from app.core.limiter import get_auth_key, get_public_key, limiter

# Import OptionalAuth
from auth import OptionalAuth
from endpoints.Public.repositories.vendors_repo import fetch_public_vendors

vendors_router = APIRouter()

@vendors_router.get(
    "/",
    summary="Public Vendor Directory",
    description="Search for active vendors. Public access allowed.",
)
@limiter.limit("30/minute", key_func=get_auth_key)
@limiter.limit("10/minute", key_func=get_public_key)
async def get_vendors(
    request: Request,
    search: Optional[str] = Query(None, description="Search by Company Name or Code"),
    corp: Optional[str] = Query(None, description="Filter by Corporation Name"),
    operator: Optional[str] = Query(None, description="Filter by In-Game Username"),
    user_id: Optional[str] = Depends(OptionalAuth())
):
    pool = request.app.state.db.pool

    async with pool.acquire() as conn:
        json_data = await fetch_public_vendors(conn, search=search, corp=corp, operator=operator)

    return Response(content=json_data, media_type="application/json")
