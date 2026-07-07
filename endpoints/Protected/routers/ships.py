from typing import Any, Literal, Optional

import orjson
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse as DefaultJSONResponse

from app.core.limiter import get_auth_key, limiter
from auth import RequireAuth
from endpoints.Protected.repositories.ships_repo import search_ships
from endpoints.Protected.schemas.ships import UserShips, Ship
from typing import List

ships_router = APIRouter()

class ORJSONResponse(DefaultJSONResponse):
    media_type = "application/json"
    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)

# ==============================================================================
# 1. LIST Endpoint (Multi-User) -> [{ "Username": "x", "Ships": [...] }]
# ==============================================================================
@ships_router.get(
    "/",
    summary="Search Ships",
    description="Search for ships list. If no usernames provided, returns your own data.",
    responses={200: {"model": List[UserShips]}}
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def search_user_ships(
    request: Request,
    usernames: Optional[str] = Query(None, description="Comma-separated list of usernames"),
    shipname: Optional[str] = Query(None, description="Partial match for Ship Name"),
    inflight: Optional[bool] = Query(None, description="Filter by flight status"),
    location: Optional[str] = Query(None, description="Partial match for Location"),
    type: Optional[Literal["HCB", "WCB", "VCB", "LCB", "TINY"]] = Query(None, description="Filter by hull type"),
    user_id: str = Depends(RequireAuth(["ships:read"])),
):
    pool = request.app.state.db.pool
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        return Response(content='[]', media_type="application/json")

    async with pool.acquire() as conn:
        ships_data = await search_ships(
            conn,
            valid_targets,
            shipname=shipname,
            inflight=inflight,
            location=location,
            ship_type=type,
        )
    
    if not ships_data or ships_data == "[]":
        return Response(content="[]", media_type="application/json")

    return Response(content=ships_data, media_type="application/json")



# ==============================================================================
# 2. SINGLE USER Endpoint (Unwrapped) -> [...] (Just the ships list)
# ==============================================================================
@ships_router.get(
    "/user",
    summary="Get Single User Ships",
    description="Returns a flat list of ships for a specific user.",
    response_class=ORJSONResponse,
    responses={200: {"model": List[Ship]}}
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def search_user_ships_single(
    request: Request,
    username: Optional[str] = Query(None, description="Specific username"),
    shipname: Optional[str] = Query(None, description="Partial match for Ship Name"),
    inflight: Optional[bool] = Query(None, description="Filter by flight status"),
    location: Optional[str] = Query(None, description="Partial match for Location"),
    type: Optional[Literal["HCB", "WCB", "VCB", "LCB", "TINY"]] = Query(None, description="Filter by hull type"),
    user_id: str = Depends(RequireAuth(["ships:read"])),
):
    pool = request.app.state.db.pool
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        raise HTTPException(status_code=404, detail="User not found or access denied")

    async with pool.acquire() as conn:
        # 1. Fetch standard multi-user structure
        ships_data = await search_ships(
            conn,
            valid_targets,
            shipname=shipname,
            inflight=inflight,
            location=location,
            ship_type=type,
        )

    if not ships_data or ships_data == "[]":
        return []

    try:
        data_list = orjson.loads(ships_data)
        if data_list and "Ships" in data_list[0]:
            return data_list[0]["Ships"]
        return []
    except Exception:
        return []

