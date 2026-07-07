import logging
from typing import Any, Optional

import orjson
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse as DefaultJSONResponse

from app.core.limiter import get_auth_key, limiter
from auth import RequireAuth
from endpoints.Protected.repositories.production_repo import search_production_lines
from endpoints.Protected.schemas.production import UserProduction, ProductionLine, BurnRateMaterial
from typing import List, Dict

logger = logging.getLogger(__name__)

production_router = APIRouter()

class ORJSONResponse(DefaultJSONResponse):
    media_type = "application/json"
    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)

# ==============================================================================
# 1. LIST Endpoint (Multi-User) -> [{ "Username": "x", "Production": [...] }]
# ==============================================================================
@production_router.get(
    "/",
    summary="Search Production Lines",
    description="Get production lines list. If no usernames provided, returns your own data.",
    responses={200: {"model": List[UserProduction]}}
)
@limiter.limit("30/minute", key_func=get_auth_key)
async def search_production(
    request: Request,
    location: Optional[str] = Query(None, description="Filter by Planet/Natural ID"),
    usernames: Optional[str] = Query(None, description="Comma-separated list of usernames"),
    user_id: str = Depends(RequireAuth(["production:read"])),
):
    pool = request.app.state.db.pool
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        return Response(content='[]', media_type="application/json")

    async with pool.acquire() as conn:
        json_data = await search_production_lines(conn, valid_targets, location)

    return Response(content=json_data, media_type="application/json")


# ==============================================================================
# 2. SINGLE USER Endpoint (Unwrapped) -> [...] (Just the production lines list)
# ==============================================================================
@production_router.get(
    "/user",
    summary="Get Single User Production",
    description="Returns a flat list of production lines for a specific user.",
    response_class=ORJSONResponse,
    responses={200: {"model": List[ProductionLine]}}
)
@limiter.limit("30/minute", key_func=get_auth_key)
async def search_production_user(
    request: Request,
    username: Optional[str] = Query(None, description="Specific username"),
    location: Optional[str] = Query(None, description="Filter by Planet/Natural ID"),
    user_id: str = Depends(RequireAuth(["production:read"])),
):
    pool = request.app.state.db.pool
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        raise HTTPException(status_code=404, detail="User not found or access denied")

    async with pool.acquire() as conn:
        # 1. Fetch standard multi-user structure
        json_str = await search_production_lines(conn, valid_targets, location)

        # 2. Unwrap to return ONLY the Production list
        try:
            data_list = orjson.loads(json_str)
            if data_list and "Production" in data_list[0]:
                return data_list[0]["Production"]
            return []
        except Exception:
            return []

@production_router.get(
    "/user/burn",
    summary="Get Single User Burn Production",
    description="Returns a dictionary of burn rates grouped by planet for a specific user.",
    response_class=ORJSONResponse,
    responses={200: {"model": Dict[str, List[BurnRateMaterial]]}}
)
@limiter.limit("30/minute", key_func=get_auth_key)
async def search_burn_production_user(
    request: Request,
    username: Optional[str] = Query(None, description="Specific username"),
    location: Optional[str] = Query(None, description="Filter by Planet/Natural ID"),
    user_id: str = Depends(RequireAuth(["production:read"])),
):
    pool = request.app.state.db.pool
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        raise HTTPException(status_code=404, detail="User not found or access denied")

    async with pool.acquire() as conn:
        # 1. Fetch standard multi-user structure
        json_str = await search_production_lines(conn, valid_targets, location, burn=True)

        # 2. Unwrap to return ONLY the BurnRates object
        try:
            data_list = orjson.loads(json_str)
            if data_list and "BurnRates" in data_list[0]:
                return data_list[0]["BurnRates"]

            return {}
        except Exception as e:
            request.app.state.logger.error(f"Error unwrapping burn data: {e}")
            return {}

@production_router.get(
    "/user/production/simple",
    summary="Get Single User Simple Production",
    description="Returns a simplified view of production data for a specific user.",
    response_class=ORJSONResponse)
@limiter.limit("30/minute", key_func=get_auth_key)
async def search_simple_production_user(
    request: Request,
    user_id: str = Depends(RequireAuth(["production:read"])),
):
    pool = request.app.state.db.pool
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        raise HTTPException(status_code=404, detail="User not found or access denied")

    async with pool.acquire() as conn:
        json_str = await search_production_lines(conn, valid_targets, burn=True, simple=True)

        try:
            parsed_data = orjson.loads(json_str)

            return parsed_data if parsed_data else {}
        except Exception as e:
            logger.error(f"Error unwrapping simple production data: {e}", exc_info=True)
            return {}
