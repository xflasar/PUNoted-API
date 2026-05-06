# endpoints/Protected/routers/vendors.py (or cx.py)

import json
from typing import Optional
from fastapi import APIRouter, Depends, Query, Request, Response

# Import OptionalAuth
from auth import OptionalAuth 
from app.core.limiter import get_auth_key, get_public_key, limiter
from endpoints.Public.services.cx_service import generate_json_data, generate_market_data_csv

cx_router = APIRouter()

@cx_router.get(
    "/prices/csv",
    description="Get CX csv data. Public access allowed.",
    response_class=Response,
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": "Returns a CSV file with market data."
        }
    }
)
@limiter.limit("120/minute", key_func=get_auth_key)
@limiter.limit("60/minute", key_func=get_public_key)
async def get_cx_prices_csv(
    request: Request,
    #cx: Optional[str] = Query(None, description="Search by CX CODE."),
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db

    csv_string = await generate_market_data_csv(db)

    return Response(
        content=csv_string, 
        media_type="text/csv",
        headers={
            "Content-Disposition": "inline; filename=cx_market_data.csv",
            "Cache-Control": "public, max-age=1800"
        }
    )

@cx_router.get(
    "/prices",
    description="Get CX market data in JSON format. Public access allowed.",
    response_class=Response,
    responses={
        200: {
            "content": {"application/json": {}},
            "description": "Returns market data in JSON format."
        }
    }
)
@limiter.limit("120/minute", key_func=get_auth_key)
@limiter.limit("60/minute", key_func=get_public_key)
async def get_cx_prices_json(
    request: Request,
    #cx: Optional[str] = Query(None, description="Search by CX CODE."),
    user_id: Optional[str] = Depends(OptionalAuth())
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