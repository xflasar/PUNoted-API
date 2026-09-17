from fastapi import Query
import json
import logging
from typing import Optional, List

from fastapi import APIRouter, Depends, Request, Response

from app.core.limiter import get_auth_key, get_public_key, limiter

from auth import OptionalAuth
from endpoints.Public.services.cx_service import (
    generate_json_data,
    generate_market_data_csv,
    get_ticker_history_service,
    get_ticker_detail_service,
)
from endpoints.Public.schemas.cx import CXPrice

logger = logging.getLogger(__name__)

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
    cx: Optional[str] = Query(None, description="Search by CX CODE."),
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db
    csv_string = await generate_market_data_csv(db, cx)

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
    responses={
        200: {
            "model": List[CXPrice],
            "description": "Returns market data in JSON format."
        }
    }
)
@limiter.limit("120/minute", key_func=get_auth_key)
@limiter.limit("60/minute", key_func=get_public_key)
async def get_cx_prices_json(
    request: Request,
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db
    cx_data = await generate_json_data(db)

    if not cx_data:
        return []
    
    return cx_data


@cx_router.get(
    "/history/{ticker}",
    description="Get historical CX price and supply records for a commodity ticker."
)
@limiter.limit("120/minute", key_func=get_auth_key)
@limiter.limit("60/minute", key_func=get_public_key)
async def get_cx_ticker_history(
    request: Request,
    ticker: str,
    exchange: str = "IC1",
    days: int = 7,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db
    return await get_ticker_history_service(
        db, ticker=ticker, exchange=exchange, days=days, start_date=start_date, end_date=end_date
    )


@cx_router.get(
    "/detail/{ticker}",
    description="Get detailed current orderbook, highs, lows, and stats for a commodity ticker."
)
@limiter.limit("120/minute", key_func=get_auth_key)
@limiter.limit("60/minute", key_func=get_public_key)
async def get_cx_ticker_detail(
    request: Request,
    ticker: str,
    exchange: str = "IC1",
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db
    return await get_ticker_detail_service(db, ticker=ticker, exchange=exchange)


