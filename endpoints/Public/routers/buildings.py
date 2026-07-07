from typing import Optional

from fastapi import APIRouter, Depends, Query, Request, Response

from app.core.limiter import get_auth_key, get_public_key, limiter
from auth import OptionalAuth
from endpoints.Public.services.buildings_service import fetch_building_data
from endpoints.Public.schemas.buildings import Building
from typing import List

buildings_router = APIRouter()

@buildings_router.get(
    "/",
    summary="Buildings Database",
    description="Retrieve building data. Leave empty for all buildings, pass a single ticker (RIG), or multiple comma-separated tickers (RIG,FRM).",
    responses={200: {"model": List[Building]}}
)
@limiter.limit("120/minute", key_func=get_auth_key)
@limiter.limit("60/minute", key_func=get_public_key)
async def get_buildings(
    request: Request,
    ticker: Optional[str] = Query(None, description="Search by Building Ticker(s) separated by commas (e.g., RIG, FRM)"),
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db

    buildings_data = await fetch_building_data(db, ticker=ticker)

    if not buildings_data:
        return Response(content="[]", media_type="application/json")

    return Response(content=buildings_data, media_type="application/json")

