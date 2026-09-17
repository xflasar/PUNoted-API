from typing import Optional
import json
import logging
from decimal import Decimal

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from app.db.dependencies import get_db
from app.core.security import require_internal_origin
from app.core.redis_client import redis_client # Ensure this is imported

logger = logging.getLogger(__name__)

cx_internal_router = APIRouter(dependencies=[Depends(require_internal_origin)])

from endpoints.Public.services.cx_service import (
    generate_json_data,
    get_ticker_history_service,
    get_ticker_detail_service,
)
from endpoints.Public.repositories.cx_history_repo import fetch_historical_stability_map

@cx_internal_router.get("/stability-matrix", description="Get historical stability matrix data. Internal access.")
async def get_cx_stability_matrix_internal(request: Request, days: int = 7):
    try:
        db = get_db(request)
        data = await fetch_historical_stability_map(db, days=days)
        return JSONResponse(content=data)
    except Exception as e:
        logger.error(f"Error fetching internal CX stability matrix: {e}")
        return JSONResponse(content={})

@cx_internal_router.get("/prices", description="Get CX market data in JSON format. Internal access only.")
async def get_cx_prices_json_internal(request: Request, bypass_cache: bool = False):
    try:
        db = request.app.state.db
        json_data = await generate_json_data(db, bypass_cache=bypass_cache)
        return JSONResponse(content=json_data)
    except Exception as e:
        logger.error(f"Failed to fetch internal CX prices: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": "Failed to fetch internal CX prices"},
        )

@cx_internal_router.get("/history/{ticker}", description="Get historical CX price records. Internal access.")
async def get_cx_ticker_history_internal(
    request: Request,
    ticker: str,
    exchange: str = "IC1",
    days: int = 7,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    db = get_db(request)
    result = await get_ticker_history_service(
        db, ticker=ticker, exchange=exchange, days=days, start_date=start_date, end_date=end_date
    )
    return JSONResponse(content=result)

@cx_internal_router.get("/detail/{ticker}", description="Get current CX detail and orderbook. Internal access.")
async def get_cx_ticker_detail_internal(
    request: Request,
    ticker: str,
    exchange: str = "IC1",
):
    db = get_db(request)
    result = await get_ticker_detail_service(db, ticker=ticker, exchange=exchange)
    return JSONResponse(content=result)