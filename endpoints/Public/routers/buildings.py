from typing import Optional
from fastapi import APIRouter, Depends, Query, Request, Response

from auth import OptionalAuth 
from app.core.limiter import get_auth_key, get_public_key, limiter
from endpoints.Public.services.buildings_service import fetch_building_data

buildings_router = APIRouter()

@buildings_router.get(
    "/",
    summary="Buildings Database",
    description="Retrieve building data. Leave empty for all buildings, pass a single ticker (RIG), or multiple comma-separated tickers (RIG,FRM).",
)
@limiter.limit("120/minute", key_func=get_auth_key)
@limiter.limit("60/minute", key_func=get_public_key)
async def get_buildings(
    request: Request,
    ticker: Optional[str] = Query(None, description="Search by Building Ticker(s) separated by commas (e.g., RIG, FRM)"),
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db
    
    json_string = await fetch_building_data(db, ticker=ticker)

    return Response(
        content=json_string, 
        media_type="application/json",
        headers={
            "Cache-Control": "public, s-maxage=86400, max-age=3600"
        }
    )