import logging

from fastapi import APIRouter, Depends, Request

from app.core.security import require_internal_origin
from services.internal import leaderboard_service

leaderboard_router = APIRouter(dependencies=[Depends(require_internal_origin)])
logger = logging.getLogger(__name__)

@leaderboard_router.get(
    "/production",
    summary="Get Top 25 Production Leaderboard with 30-Day History"
)
async def get_production_leaderboard(request: Request):
    """
    Returns the current 7-day Top 25 producers per material, 
    including their estimated value and 30-day historical trends.
    """
    db = request.app.state.db

    data = await leaderboard_service.get_formatted_production_leaderboard(db)

    if data:
        return {"success": True, "data": data}
    else:
        return {"success": False, "data": []}
