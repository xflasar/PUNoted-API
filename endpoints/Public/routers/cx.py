import json
import logging
from typing import Optional, List

from fastapi import APIRouter, Depends, Request, Response

from app.core.limiter import get_auth_key, get_public_key, limiter

from auth import OptionalAuth
from endpoints.Public.services.cx_service import generate_json_data, generate_market_data_csv
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
    #cx: Optional[str] = Query(None, description="Search by CX CODE."),
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
    full_ticker = f"{ticker.upper()}.{exchange.upper()}"

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
            rows = await conn.fetch(query, full_ticker, ticker.upper(), str(days if days > 0 else 30))
            
            result = []
            for r in rows:
                result.append({
                    "timestamp": r["timestamp"].isoformat() if r["timestamp"] else "",
                    "askprice": float(r["askprice"]) if r["askprice"] is not None else 0.0,
                    "bidprice": float(r["bidprice"]) if r["bidprice"] is not None else 0.0,
                    "supply": int(r["supply"]) if r["supply"] is not None else 0,
                })
            return result
    except Exception as e:
        logger.error(f"Error fetching CX history for {full_ticker}: {e}")
        return []


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
    clean_ticker = ticker.upper()
    clean_exchange = exchange.upper()
    full_ticker = f"{clean_ticker}.{clean_exchange}"

    try:
        async with db.pool.acquire() as conn:
            # 1. Fetch main broker record
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
                WHERE ticker = $1 OR (brokermaterialid = $2 AND exchange = $3)
                LIMIT 1;
                """,
                full_ticker, clean_ticker, clean_exchange
            )

            if not broker_row:
                return {
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
                }

            broker_id = broker_row["brokermaterialid"]

            # 2. High / Low Stats from History
            stats_row = await conn.fetchrow(
                """
                SELECT 
                    COALESCE(MAX(askprice), 0) AS high,
                    COALESCE(MIN(askprice), 0) AS low,
                    COALESCE(MAX(askprice), 0) AS alltimehigh,
                    COALESCE(MIN(askprice), 0) AS alltimelow
                FROM cx_brokers_history
                WHERE ticker = $1;
                """,
                full_ticker
            )

            # 3. Orderbook Buy/Sell Orders
            bids = []
            asks = []

            if broker_id:
                bid_rows = await conn.fetch(
                    """
                    SELECT priceamount AS price, itemcount AS amount, tradername AS trader
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
                    SELECT priceamount AS price, itemcount AS amount, tradername AS trader
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

            return {
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
            }

    except Exception as e:
        logger.error(f"Error fetching CX detail for {full_ticker}: {e}")
        return {
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
        }

