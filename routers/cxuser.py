import logging
from typing import Optional

from fastapi import APIRouter, Body, Depends, Query, Request

from app.core.security import require_internal_origin
from auth import get_current_user_id
from helpers.cx_analysis import get_bulk_prices, get_storage_valuation

cx_router = APIRouter(dependencies=[Depends(require_internal_origin)])
logger = logging.getLogger("cx_router")

@cx_router.get("/storage-valuation")
async def get_storage_valuation_endpoint(
    request: Request,
    exchange: str = Query(..., description="Exchange ticker (e.g. IC1)"),
    storageid: Optional[str] = Query(None, description="Storage ID"),
    user_id: str = Depends(get_current_user_id),
):
    """
    Fetches storage valuation data via HTTP.
    """
    try:
        db = request.app.state.db
        print(storageid, exchange, user_id)
        return await get_storage_valuation(db, user_id, exchange, storageid)
    except Exception as e:
        logger.error(f"Error fetching storage valuation: {e}")
        return {"status": "error", "message": str(e)}


@cx_router.post("/prices")
async def get_prices_endpoint(request: Request, payload: dict = Body(...)):
    """
    Bulk price fetcher. Expects { "tickers": ["MAT1", "MAT2"], "exchange": "IC1" }
    """
    try:
        db = request.app.state.db
        tickers = payload.get("tickers", [])
        exchange = payload.get("exchange", "IC1")

        return await get_bulk_prices(db, tickers, exchange)
    except Exception as e:
        logger.error(f"Error fetching prices: {e}")
        return {"status": "error", "message": str(e)}
