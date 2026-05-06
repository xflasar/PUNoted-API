from typing import List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from pydantic import BaseModel

from app.core.security import require_internal_origin
from helpers.governance import get_governance_overview

# from .auth import get_current_user

governance_router = APIRouter(dependencies=[Depends(require_internal_origin)])


class GovernanceRequest(BaseModel):
    planet_ids: Optional[List[str]] = None


@governance_router.post("/")
async def get_governance_data(
    request: Request,
    payload: GovernanceRequest = Body(...),
    # current_user = Depends(get_current_user)
):
    """
    Fetches governance data. Accepts an optional list of planet_ids to filter.
    """
    try:
        db = request.app.state.db
        result = await get_governance_overview(db, planet_ids=payload.planet_ids)

        if result["status"] == "error":
            raise HTTPException(status_code=500, detail=result["message"])

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
