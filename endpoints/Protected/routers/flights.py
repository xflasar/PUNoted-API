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

    if usernames:
        requested_users = [u.strip() for u in usernames.split(",") if u.strip()]
        valid_targets = [u for u in valid_targets if u in requested_users]

    if not valid_targets:
        return []

    async with pool.acquire() as conn:
        flights_data = await search_flights(conn, valid_targets, ship, current, limit)

    if flights_data is None:
        return []

    return flights_data


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
        flights_data = await search_flights(conn, valid_targets, ship, current, limit)

        if not flights_data:
            return []

        return flights_data
