from typing import Optional
from fastapi import APIRouter, Depends, Query, Request, Response
from app.core.limiter import get_auth_key, get_public_key, limiter
from auth import OptionalAuth
from endpoints.Public.services.governance_service import (
    get_governance_terms_data,
    get_governance_motions_data
)

governance_router = APIRouter()

@governance_router.get(
    "/terms",
    summary="Planetary Government Terms & Election Candidates",
    description="Retrieve planetary government election terms, candidate election results, votes, and winners."
)
@limiter.limit("20/minute", key_func=get_auth_key)
@limiter.limit("10/minute", key_func=get_public_key)
async def get_government_terms(
    request: Request,
    planet: Optional[str] = Query(None, description="Filter by Planet Natural ID or Name (e.g. VH-778b or Shesmu)"),
    termid: Optional[str] = Query(None, description="Filter by Term ID"),
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db
    data = await get_governance_terms_data(db, planet=planet, termid=termid)
    return Response(content=data, media_type="application/json")


@governance_router.get(
    "/motions",
    summary="Planetary Government Motions & Voting Records",
    description="Retrieve planetary government motions, status, voting records, and component details."
)
@limiter.limit("20/minute", key_func=get_auth_key)
@limiter.limit("10/minute", key_func=get_public_key)
async def get_government_motions(
    request: Request,
    planet: Optional[str] = Query(None, description="Filter by Planet Natural ID or Name (e.g. VH-778b or Shesmu)"),
    motionid: Optional[str] = Query(None, description="Filter by Motion ID"),
    status: Optional[str] = Query(None, description="Filter by Motion status (e.g. ACTIVE, PASSED, REJECTED)"),
    full: bool = Query(False, description="Set to true to include full votes & component breakdown"),
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db
    data = await get_governance_motions_data(db, planet=planet, motionid=motionid, status=status, full=full)
    return Response(content=data, media_type="application/json")
