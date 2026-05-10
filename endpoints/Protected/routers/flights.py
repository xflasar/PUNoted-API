from typing import Any, Optional

import orjson
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse as DefaultJSONResponse

from app.core.limiter import get_auth_key, limiter
from auth import RequireAuth
from endpoints.Protected.repositories.flights_repo import search_flights

flights_router = APIRouter()

# Helper for fast JSON serialization
class ORJSONResponse(DefaultJSONResponse):
    media_type = "application/json"
    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)

# --------------------------------------------------------
# 1. LIST Endpoint (Standard)
# Returns: [ { "Username": "x3m", "Flights": [...] }, ... ]
# --------------------------------------------------------
@flights_router.get("/")
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_flights(
    request: Request,
    user_id: str = Depends(RequireAuth(["flights:read"])),
    usernames: Optional[str] = Query(None, description="Comma-separated list of usernames"),
    ship: Optional[str] = Query(None),
    current: Optional[bool] = Query(None),
    limit: int = Query(20, ge=1, le=100),
):
    pool = request.app.state.db.pool
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        return Response(content='[]', media_type="application/json")

    async with pool.acquire() as conn:
        flights_data = await search_flights(conn, valid_targets, ship, current, limit)

    return Response(content=flights_data, media_type="application/json")


# --------------------------------------------------------
# 2. SINGLE USER Endpoint (Flattened)
# Returns: [ { "FlightId": "...", "Origin": "..." }, ... ]
# --------------------------------------------------------
@flights_router.get("/user", response_class=ORJSONResponse)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_flight_user(
    request: Request,
    user_id: str = Depends(RequireAuth(["flights:read"])),
    username: Optional[str] = Query(None, description="Specific username to fetch"),
    ship: Optional[str] = Query(None),
    current: Optional[bool] = Query(None),
    limit: int = Query(20, ge=1, le=100),
):
    pool = request.app.state.db.pool

    # Auth logic has already processed 'username' into this list
    valid_targets = getattr(request.state, "valid_target_users", [])

    # Security/Edge Case: If auth filtered the user out
    if not valid_targets:
        raise HTTPException(status_code=404, detail="User not found or access denied")

    async with pool.acquire() as conn:
        # 1. Fetch standard structure: '[{"Username": "...", "Flights": [...]}]'
        json_str = await search_flights(conn, valid_targets, ship, current, limit)

        # 2. Unwrap to return ONLY the Flights list
        try:
            data_list = orjson.loads(json_str)
            if data_list and "Flights" in data_list[0]:
                return data_list[0]["Flights"]
            else:
                return []
        except Exception:
            return []
