from typing import Optional, Any
import orjson
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse as DefaultJSONResponse

from auth import RequireAuth
from app.core.limiter import get_auth_key, limiter
from endpoints.Protected.repositories.sites_repo import fetch_sites

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

    pool = request.app.state.db.pool
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        return Response(content='[]', media_type="application/json")

    async with pool.acquire() as conn:
        json_data = await fetch_sites(
            conn,
            valid_targets,
            location=location,
            include_buildings=include_buildings,
            include_reclaimable=include_reclaimable,
            include_repair=include_repair,
        )

    return Response(content=json_data, media_type="application/json")


# ==============================================================================
# 2. SINGLE USER Endpoint (Unwrapped) -> [...] (Just the sites list)
# ==============================================================================
@sites_router.get(
    "/user",
    summary="Get Single User Sites",
    description="Returns a flat list of sites for a specific user.",
    response_class=ORJSONResponse
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def search_sites_user(
    request: Request,
    username: Optional[str] = Query(None, description="Specific username"),
    location: Optional[str] = Query(None, description="Partial match for Planet Name/ID"),
    include_buildings: bool = Query(False, description="Include buildings list"),
    include_reclaimable: bool = Query(False, description="Include Reclaimable Materials"),
    include_repair: bool = Query(False, description="Include Repair Materials"),
    user_id: str = Depends(RequireAuth(["sites:read"])),
):
    if (include_reclaimable or include_repair) and not include_buildings:
        raise HTTPException(status_code=400, detail="Cannot request materials without include_buildings=true")

    pool = request.app.state.db.pool
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        raise HTTPException(status_code=404, detail="User not found or access denied")

    async with pool.acquire() as conn:
        # 1. Fetch standard multi-user structure
        json_str = await fetch_sites(
            conn,
            valid_targets,
            location=location,
            include_buildings=include_buildings,
            include_reclaimable=include_reclaimable,
            include_repair=include_repair,
        )

        # 2. Unwrap to return ONLY the Sites list
        try:
            data_list = orjson.loads(json_str)
            if data_list and "Sites" in data_list[0]:
                return data_list[0]["Sites"] # Returns just the list of sites
            return []
        except Exception:
            return []