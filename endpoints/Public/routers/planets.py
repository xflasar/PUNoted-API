from typing import Optional

from fastapi import APIRouter, Depends, Query, Request, Response

from app.core.limiter import get_auth_key, get_public_key, limiter
from auth import OptionalAuth
from endpoints.Public.services.planets_service import get_planet_data
from endpoints.Public.schemas.planets import PlanetMinimal, PlanetFull
from typing import List, Union

planets_router = APIRouter()

@planets_router.get(
    "/",
    summary="Planet Database",
    description="Retrieve minimal or full planet data. Pass a ticker for a specific planet, or full=true for all.",
    responses={200: {"model": Union[List[PlanetMinimal], List[PlanetFull], PlanetFull]}}
)
@limiter.limit("20/minute", key_func=get_auth_key)
@limiter.limit("10/minute", key_func=get_public_key)
async def get_planets(
    request: Request,
    ticker: Optional[str] = Query(None, description="Search by Planet Natural ID (e.g., VH-331a)"),
    full: bool = Query(False, description="Set to true to return the massive full payload for all planets"),
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db

    planets_data = await get_planet_data(db, ticker=ticker, full=full)

    if not planets_data:
        return Response(content="[]", media_type="application/json")

    return Response(content=planets_data, media_type="application/json")

