from typing import Optional

from fastapi import APIRouter, Depends, Query, Request, Response

from app.core.limiter import get_auth_key, get_public_key, limiter

from auth import OptionalAuth
from endpoints.Public.services.vendors_service import get_vendors_data
from endpoints.Public.schemas.vendors import VendorEntry
from typing import List

vendors_router = APIRouter()

@vendors_router.get(
    "/",
    summary="Public Vendor Directory",
    description="Search for active vendors. Public access allowed.",
    responses={200: {"model": List[VendorEntry]}}
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
    db = request.app.state.db
    vendors_data = await get_vendors_data(db, search=search, corp=corp, operator=operator)
    return vendors_data

