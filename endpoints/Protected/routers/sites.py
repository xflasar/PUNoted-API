from typing import Any, Optional

import orjson
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse as DefaultJSONResponse

from app.core.limiter import get_auth_key, limiter
from auth import RequireAuth
from endpoints.Protected.services.sites_service import get_sites_data
from endpoints.Protected.schemas.sites import UserSites, Site
from typing import List

sites_router = APIRouter()

class ORJSONResponse(DefaultJSONResponse):
    media_type = "application/json"
    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)

# ==============================================================================
# 1. LIST Endpoint (Multi-User) -> [{ "Username": "x", "Sites": [...] }]
# ==============================================================================
@sites_router.get(
    "/",
    summary="Search Sites",
    description="Search for sites list. If no usernames provided, returns your own data.",
    responses={200: {"model": List[UserSites]}}
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def search_sites(
    request: Request,
    usernames: Optional[str] = Query(None, description="Comma-separated list of usernames"),
    location: Optional[str] = Query(None, description="Partial match for Planet Name/ID"),
    include_buildings: bool = Query(False, description="Include buildings list"),
    include_reclaimable: bool = Query(False, description="Include Reclaimable Materials"),
    include_repair: bool = Query(False, description="Include Repair Materials"),
    user_id: str = Depends(RequireAuth(["sites:read"])),
):
    if (include_reclaimable or include_repair) and not include_buildings:
        raise HTTPException(status_code=400, detail="Cannot request materials without include_buildings=true")

    db = request.app.state.db
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        return []

    sites_data = await get_sites_data(
        db,
        valid_targets,
        location=location,
        include_buildings=include_buildings,
        include_reclaimable=include_reclaimable,
        include_repair=include_repair,
    )
    return sites_data


# ==============================================================================
# 2. SINGLE USER Endpoint (Unwrapped) -> [...] (Just the sites list)
# ==============================================================================
@sites_router.get(
    "/user",
    summary="Get Single User Sites",
    description="Returns a flat list of sites for a specific user.",
    response_class=ORJSONResponse,
    responses={200: {"model": List[Site]}}
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def search_sites_user(
    request: Request,
    username: Optional[str] = Query(None, description="Specific username"),
    location: Optional[str] = Query(None, description="Partial match for Planet Name/ID"),
    include_buildings: bool = Query(False, description="Include buildings list"),
    include_reclaimable: bool = Query(False, description="Include Reclaimable Materials"),
    include_repair: bool = Query(False, description="Include Repair Materials"),
    user_id: str = Depends(RequireAuth(["sites:read"], is_single_user_endpoint=True)),
):
    if (include_reclaimable or include_repair) and not include_buildings:
        raise HTTPException(status_code=400, detail="Cannot request materials without include_buildings=true")

    db = request.app.state.db
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        raise HTTPException(status_code=404, detail="User not found or access denied")

    # 1. Fetch standard multi-user structure
    sites_data = await get_sites_data(
        db,
        valid_targets,
        location=location,
        include_buildings=include_buildings,
        include_reclaimable=include_reclaimable,
        include_repair=include_repair,
    )

    if not sites_data:
        return []

    if sites_data and "Sites" in sites_data[0]:
        return sites_data[0]["Sites"]
    return []
