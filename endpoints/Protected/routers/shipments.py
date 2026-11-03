import logging
from typing import Optional
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from app.core.limiter import get_auth_key, limiter
from auth import RequireAuth
from helpers.shipments import fetch_active_shipments_structured

logger = logging.getLogger(__name__)

protected_shipments_router = APIRouter()

@protected_shipments_router.get(
    '/user',
    summary="User Active Shipments",
    description="Returns all active shipment contracts and carrier ships."
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_user_shipments(
    request: Request,
    user_id: str = Depends(RequireAuth(['contracts:read']))
):
    try:
        db = request.app.state.db

        # user_id is the account UUID returned by RequireAuth
        data = await fetch_active_shipments_structured(db, user_id)

        return JSONResponse(content=data)
    except Exception as e:
        logger.error(f"Error fetching shipments for user {user_id}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error while fetching user shipments"}
        )
