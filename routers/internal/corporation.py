from typing import List

from fastapi import APIRouter, Depends, Request, Response

from app.core.security import require_internal_origin
from auth import get_current_user_id
from models.corp_production_models import CorpOverviewResponse
from services.internal.corporation_service import build_corp_production_response

corporation_internal_router = APIRouter(dependencies=[Depends(require_internal_origin)])


@corporation_internal_router.get(
    "/",
    response_model=List[CorpOverviewResponse],
)
async def corp_production(
    request: Request,
    response: Response,
    user_id: str = Depends(get_current_user_id),
    debug: bool = False,
):
    pool = request.app.state.db.pool

    async with pool.acquire() as conn:
        return await build_corp_production_response(
            conn=conn,
            user_id=user_id,
            debug=debug,
        )
