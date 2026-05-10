import json
from typing import List

from fastapi import APIRouter, Depends, Request, Response

from app.core.security import require_internal_origin
from auth import get_current_user_id
from endpoints.Public.services.corp_service import generate_json_data
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


# FIXME: Rework this endpoint to use the internal service and repo and add corp pricing tables
@corporation_internal_router.get(
    "/prices",
    description="Get corporation market data in JSON format.",
    response_class=Response,
    responses={
        200: {
            "content": {"application/json": {}},
            "description": "Returns corporation market data in JSON format."
        }
    }
)
async def get_corporation_prices_json(
    request: Request,
    #corp: Optional[str] = Query(None, description="Search by corp CODE."),
    user_id: str = Depends(get_current_user_id)
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
