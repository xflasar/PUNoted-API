from typing import Any, Literal, Optional

import orjson
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse as DefaultJSONResponse

from app.core.limiter import get_auth_key, limiter
from auth import RequireAuth
from endpoints.Protected.services.ships_service import get_ships_data
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
    db = request.app.state.db
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        return []

    ships_data = await get_ships_data(
        db,
        valid_targets,
        shipname=shipname,
        inflight=inflight,
        location=location,
        ship_type=type,
    )
    return ships_data



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
    user_id: str = Depends(RequireAuth(["ships:read"], is_single_user_endpoint=True)),
):
    db = request.app.state.db
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        raise HTTPException(status_code=404, detail="User not found or access denied")

    # 1. Fetch standard multi-user structure
    ships_data = await get_ships_data(
        db,
        valid_targets,
        shipname=shipname,
        inflight=inflight,
        location=location,
        ship_type=type,
    )

    if not ships_data:
        return []

    if ships_data and "Ships" in ships_data[0]:
        return ships_data[0]["Ships"]
    return []

