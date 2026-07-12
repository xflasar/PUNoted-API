import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request, Response

from app.core.limiter import get_auth_key, get_public_key, limiter

# Import OptionalAuth
from auth import OptionalAuth, RequireAuth
from endpoints.Public.repositories.corp_repo import fetch_corp_members
from endpoints.Public.services.corp_service import (
    generate_json_data,
    get_ship_presets,
    get_ship_orders,
    get_ship_order_by_pin,
    get_user_role,
)
from endpoints.Public.schemas.corp import CorpPrice, CorpMembersResponse
from typing import List, Any
from models.ship_management_models import ShipTypePreset, ShipOrder

logger = logging.getLogger(__name__)

corporation_router = APIRouter()

@corporation_router.get(
    "/prices",
    description="Get corporation market data in JSON format. Public access allowed.",
    response_class=Response,
    responses={
        200: {
            "model": List[CorpPrice],
            "description": "Returns corporation market data in JSON format."
        }
    }
)
@limiter.limit("120/minute", key_func=get_auth_key)
@limiter.limit("60/minute", key_func=get_public_key)
async def get_corporation_prices_json(
    request: Request,
    #cx: Optional[str] = Query(None, description="Search by CX CODE."),
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db

    json_data = await generate_json_data(db)

    return Response(
        content=json.dumps(json_data),
        media_type="application/json",
        headers={
            "Cache-Control": "public, max-age=1800"
        }
    )

@corporation_router.get(
    "/members",
    summary="Get Corporation Members",
    description="Returns all company members belonging to the requesting user's corporation.",
    responses={200: {"model": CorpMembersResponse}}
)
async def get_corporation_members_endpoint(
    request: Request,
    user_id: str = Depends(RequireAuth(["corp:read"]))
):
    db = request.app.state.db

    try:
        records = await fetch_corp_members(db, user_id)

        # Handle users who are not in a corporation
        if not records:
            return {
                "success": True,
                "data": {
                    "corporation_name": None,
                    "corporation_code": None,
                    "members": []
                }
            }

        # 4. Use the exact column names returned by the SQL query: `c.name` and `c.code`
        corporation_name = records[0]["name"]
        corporation_code = records[0]["code"]

        # Build the members array
        members = [
            {
                "company_code": record["companycode"],
                "company_name": record["companyname"]
            }
            for record in records
        ]

        return {
            "success": True,
            "data": {
                "corporation_name": corporation_name,
                "corporation_code": corporation_code,
                "members": members
            }
        }

    except Exception as e:
        logger.error(f"Failed to process corporation members endpoint for user {actual_user_id}: {e}", exc_info=True)
        return Response(
            status_code=500,
            content=json.dumps({"success": False, "message": "Internal server error processing corporation data."}),
            media_type="application/json"
        )

@corporation_router.get(
    "/ship-presets",
    description="Get corporation ship build presets. Logged-in users see private presets too.",
    responses={200: {"model": List[ShipTypePreset]}}
)
@limiter.limit("120/minute", key_func=get_auth_key)
@limiter.limit("60/minute", key_func=get_public_key)
async def get_ship_presets_endpoint(
    request: Request,
    corporation_id: str = Query(..., description="Corporation ID to fetch presets for"),
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db
    return await get_ship_presets(db, corporation_id, user_id)

@corporation_router.get(
    "/ship-orders",
    description="Get corp ship orders. Authenticated users see full details, guests see restricted basic view.",
    responses={200: {"model": List[Any]}}
)
@limiter.limit("120/minute", key_func=get_auth_key)
@limiter.limit("60/minute", key_func=get_public_key)
async def get_ship_orders_endpoint(
    request: Request,
    corporation_id: str = Query(..., description="Corporation ID to fetch orders for"),
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db
    return await get_ship_orders(db, corporation_id, user_id)

@corporation_router.get(
    "/ship-orders/by-pin",
    description="Fetch a specific ship order using its guest PIN.",
    responses={200: {"model": Any}}
)
async def get_ship_order_by_pin_endpoint(
    request: Request,
    corporation_id: str = Query(..., description="Corporation ID"),
    pin: str = Query(..., description="Guest PIN to lookup"),
):
    db = request.app.state.db
    return await get_ship_order_by_pin(db, corporation_id, pin)

@corporation_router.get(
    "/user-role",
    description="Get the current user's role for a corporation (ADMIN, USER, GUEST).",
    responses={200: {"model": Any}}
)
async def get_user_role_endpoint(
    request: Request,
    corporation_id: str = Query(..., description="Corporation ID"),
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db
    return await get_user_role(db, corporation_id, user_id)
