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

from endpoints.Public.services.cx_service import generate_json_data
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
async def get_cx_prices_json_internal(request: Request):
    try:
        db = request.app.state.db
        json_data = await generate_json_data(db)
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
    clean_ticker = ticker.upper()
    clean_exchange = exchange.upper()
    full_ticker = f"{clean_ticker}.{clean_exchange}"

    query = """
        SELECT 
            snapshot_at AS timestamp,
            COALESCE(askprice, 0) AS askprice,
            COALESCE(bidprice, 0) AS bidprice,
            COALESCE(supply, 0) AS supply
        FROM cx_brokers_history
        WHERE (ticker = $1 OR SPLIT_PART(ticker, '.', 1) = $2)
          AND snapshot_at >= CURRENT_TIMESTAMP - ($3 || ' days')::INTERVAL
        ORDER BY snapshot_at ASC;
    """

    try:
        async with db.pool.acquire() as conn:
            rows = await conn.fetch(query, full_ticker, clean_ticker, str(days if days > 0 else 30))
            result = [
                {
                    "timestamp": r["timestamp"].isoformat() if r["timestamp"] else "",
                    "askprice": float(r["askprice"]) if r["askprice"] is not None else 0.0,
                    "bidprice": float(r["bidprice"]) if r["bidprice"] is not None else 0.0,
                    "supply": int(r["supply"]) if r["supply"] is not None else 0,
                }
                for r in rows
            ]
            return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Error fetching internal CX history for {full_ticker}: {e}")
        return JSONResponse(content=[])


@cx_internal_router.get("/detail/{ticker}", description="Get current CX detail and orderbook. Internal access.")
async def get_cx_ticker_detail_internal(
    request: Request,
    ticker: str,
    exchange: str = "IC1",
):
    db = get_db(request)
    clean_ticker = ticker.upper()
    clean_exchange = exchange.upper()
    full_ticker = f"{clean_ticker}.{clean_exchange}"

    try:
        async with db.pool.acquire() as conn:
            broker_row = await conn.fetchrow(
                """
                SELECT 
                    ticker,
                    priceaverage,
                    askprice,
                    askamount,
                    bidprice,
                    bidamount,
                    supply,
                    demand,
                    traded,
                    volume,
                    brokermaterialid,
                    xata_updatedat AS last_update
                FROM cx_brokers
                WHERE ticker = $1 OR brokermaterialid = $2
                LIMIT 1;
                """,
                full_ticker, clean_ticker
            )

            if not broker_row:
                return JSONResponse(content={
                    "found": False,
                    "ticker": clean_ticker,
                    "exchange": clean_exchange,
                    "full_ticker": full_ticker,
                    "priceaverage": 0,
                    "askprice": 0,
                    "askamount": 0,
                    "bidprice": 0,
                    "bidamount": 0,
                    "high": 0,
                    "low": 0,
                    "volume": 0,
                    "traded": 0,
                    "alltimehigh": 0,
                    "alltimelow": 0,
                    "last_update": None,
                    "bids": [],
                    "asks": []
                })

            broker_id = broker_row["brokermaterialid"]

            stats_row = await conn.fetchrow(
                """
                SELECT 
                    COALESCE(MAX(COALESCE(priceaverage, askprice, bidprice, price)), 0) AS high,
                    COALESCE(MIN(COALESCE(priceaverage, askprice, bidprice, price)), 0) AS low,
                    COALESCE(MAX(COALESCE(priceaverage, askprice, bidprice, price)), 0) AS alltimehigh,
                    COALESCE(MIN(COALESCE(priceaverage, askprice, bidprice, price)), 0) AS alltimelow
                FROM cx_brokers_history
                WHERE ticker = $1;
                """,
                full_ticker
            )

            bids = []
            asks = []

            if broker_id:
                bid_rows = await conn.fetch(
                    """
                    SELECT priceamount AS price, amount AS amount, tradername AS trader
                    FROM cx_brokers_buy_orders
                    WHERE brokermaterialid = $1
                    ORDER BY priceamount DESC LIMIT 50;
                    """,
                    broker_id
                )
                bids = [
                    {
                        "price": float(r["price"] or 0),
                        "amount": int(r["amount"] or 0),
                        "trader": r["trader"] or "Anonymous"
                    }
                    for r in bid_rows
                ]

                ask_rows = await conn.fetch(
                    """
                    SELECT priceamount AS price, amount AS amount, tradername AS trader
                    FROM cx_brokers_sell_orders
                    WHERE brokermaterialid = $1
                    ORDER BY priceamount ASC LIMIT 50;
                    """,
                    broker_id
                )
                asks = [
                    {
                        "price": float(r["price"] or 0),
                        "amount": int(r["amount"] or 0),
                        "trader": r["trader"] or "Anonymous"
                    }
                    for r in ask_rows
                ]

            return JSONResponse(content={
                "found": True,
                "ticker": clean_ticker,
                "exchange": clean_exchange,
                "full_ticker": full_ticker,
                "priceaverage": float(broker_row["priceaverage"] or 0),
                "askprice": float(broker_row["askprice"] or 0),
                "askamount": int(broker_row["askamount"] or 0),
                "bidprice": float(broker_row["bidprice"] or 0),
                "bidamount": int(broker_row["bidamount"] or 0),
                "high": float(stats_row["high"] or 0) if stats_row else 0,
                "low": float(stats_row["low"] or 0) if stats_row else 0,
                "volume": float(broker_row["volume"] or 0),
                "traded": int(broker_row["traded"] or 0),
                "alltimehigh": float(stats_row["alltimehigh"] or 0) if stats_row else 0,
                "alltimelow": float(stats_row["alltimelow"] or 0) if stats_row else 0,
                "last_update": broker_row["last_update"].isoformat() if broker_row["last_update"] else None,
                "bids": bids,
                "asks": asks
            })

    except Exception as e:
        logger.error(f"Error fetching internal CX detail for {full_ticker}: {e}")
        return JSONResponse(content={
            "found": False,
            "ticker": clean_ticker,
            "exchange": clean_exchange,
            "full_ticker": full_ticker,
            "priceaverage": 0,
            "askprice": 0,
            "askamount": 0,
            "bidprice": 0,
            "bidamount": 0,
            "high": 0,
            "low": 0,
            "volume": 0,
            "traded": 0,
            "alltimehigh": 0,
            "alltimelow": 0,
            "last_update": None,
            "bids": [],
            "asks": []
        })