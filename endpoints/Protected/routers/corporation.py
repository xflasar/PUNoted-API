import csv
import io
import json
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from app.core.limiter import get_auth_key, limiter
from auth import RequireAuth
from endpoints.Public.services.corp_service import generate_json_data
from services.internal.corporation_service import build_corp_production_flat_response
from endpoints.Protected.schemas.corporation import CorpProductionOverviewResponse
from models.ship_management_models import ShipTypePreset, ShipOrder
from typing import List

logger = logging.getLogger(__name__)

corporation_router = APIRouter()

@corporation_router.get(
    "/production",
    description="Get flat corporation production overview. Ideal for tabular or CSV data.",
    responses={200: {"model": CorpProductionOverviewResponse}}
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def corporation_production_overview(
    request: Request,
    corpcode: Optional[str] = Query(None, description="Corporation code to filter by"),
    user_id: str = Depends(RequireAuth(["corporation:read"])),
):
    db = request.app.state.db

    try:
        async with db.pool.acquire() as conn:
            # 1. Fetch the flat data array
            flat_data = await build_corp_production_flat_response(conn, user_id)

            # 2. Filter by specific Corp Code if requested
            if corpcode:
                target_code = corpcode.upper()
                flat_data = [row for row in flat_data if row.get("CorpCode") == target_code]

            return {
                "success": True,
                "data": flat_data
            }

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Failed to fetch flat corporation production overview: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": "Internal server error fetching corporation production."}
        )

@corporation_router.get(
    "/production/csv",
    description="Get flat corporation production overview as a downloadable CSV.",
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def corporation_production_overview_csv(
    request: Request,
    corpcode: Optional[str] = Query(None, description="Corporation code to filter by"),
    user_id: str = Depends(RequireAuth(["corporation:read"])),
):
    db = request.app.state.db

    try:
        async with db.pool.acquire() as conn:
            flat_data = await build_corp_production_flat_response(conn, user_id)

            if corpcode:
                target_code = corpcode.upper()
                flat_data = [row for row in flat_data if row.get("CorpCode") == target_code]

            if not flat_data:
                return Response(
                    content="No data found for the requested parameters.",
                    media_type="text/plain"
                )

            output = io.StringIO()
            fieldnames = flat_data[0].keys()

            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(flat_data)

            csv_content = output.getvalue()
            output.close()

            return Response(
                content=csv_content,
                media_type="text/csv",
                headers={
                    "Content-Disposition": 'attachment; filename="corporation_production.csv"'
                }
            )

    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Failed to fetch CSV corporation production overview: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": "Internal server error fetching corporation production."}
        )


""" @corporation_router.get(
    "/prices",
    description="Get corporation market data in JSON format. Public access allowed.",
    response_class=Response,
    responses={
        200: {
            "content": {"application/json": {}},
            "description": "Returns corporation market data in JSON format."
        }
    }
)
@limiter.limit("120/minute", key_func=get_auth_key)
async def get_corporation_prices_json(
    request: Request,
    user_id: str = Depends(RequireAuth(["corporation:read"]))
):
    db = request.app.state.db

    json_data = await generate_json_data(db)

    return Response(
        content=json.dumps(json_data),
        media_type="application/json",
        headers={
            "Cache-Control": "public, max-age=1800"
        }
    ) """

# GET endpoints moved to Public router to handle both Auth/OptionalAuth under a single route.
