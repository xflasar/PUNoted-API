import json
import logging
from typing import List, Dict, Any, Optional
from app.core.redis_client import redis_client

logger = logging.getLogger(__name__)

from collections import defaultdict
import asyncio

_ticker_locks = defaultdict(asyncio.Lock)

async def fetch_ticker_history(
    db, 
    ticker: str, 
    exchange: str = "IC1", 
    days: int = 7,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Fetches historical snapshot data for a specific material and exchange.
    Returns time series of snapshot_at, askprice, bidprice, supply.
    Includes per-ticker lock to prevent cache stampede and Redis caching.
    """
    clean_ticker = ticker.upper()
    clean_exchange = exchange.upper()
    full_ticker = f"{clean_ticker}.{clean_exchange}" if "." not in clean_ticker else clean_ticker
    base_ticker = clean_ticker.split(".")[0]
    
    redis_key = f"cx_history:{full_ticker}:{days}:{start_date}:{end_date}"

    # 1. Fast Cache Read
    try:
        cached = await redis_client.get(redis_key)
        if cached:
            return json.loads(cached) if isinstance(cached, str) else cached
    except Exception as e:
        logger.warning(f"Redis get failed for {redis_key}: {e}")

    # 2. Acquire per-ticker Lock to prevent Cache Stampede
    async with _ticker_locks[redis_key]:
        try:
            cached = await redis_client.get(redis_key)
            if cached:
                return json.loads(cached) if isinstance(cached, str) else cached
        except Exception:
            pass

        if start_date and end_date:
            query = """
                SELECT 
                    snapshot_at AS timestamp,
                    COALESCE(askprice, 0) AS askprice,
                    COALESCE(bidprice, 0) AS bidprice,
                    COALESCE(supply, 0) AS supply
                FROM cx_brokers_history
                WHERE (ticker = $1 OR (SPLIT_PART(ticker, '.', 1) = $2 AND UPPER(SPLIT_PART(ticker, '.', 2)) = $3))
                  AND snapshot_at >= $4::TIMESTAMP
                  AND snapshot_at <= $5::TIMESTAMP
                ORDER BY snapshot_at ASC;
            """
            params = [full_ticker, base_ticker, clean_exchange, start_date, end_date]
        else:
            query = """
                SELECT 
                    snapshot_at AS timestamp,
                    COALESCE(askprice, 0) AS askprice,
                    COALESCE(bidprice, 0) AS bidprice,
                    COALESCE(supply, 0) AS supply
                FROM cx_brokers_history
                WHERE (ticker = $1 OR (SPLIT_PART(ticker, '.', 1) = $2 AND UPPER(SPLIT_PART(ticker, '.', 2)) = $3))
                  AND snapshot_at >= CURRENT_TIMESTAMP - ($4 || ' days')::INTERVAL
                ORDER BY snapshot_at ASC;
            """
            params = [full_ticker, base_ticker, clean_exchange, str(days if days > 0 else 30)]


        try:
            async with db.pool.acquire() as conn:
                rows = await conn.fetch(query, *params)
                result = [
                    {
                        "timestamp": r["timestamp"].isoformat() if r["timestamp"] else "",
                        "askprice": float(r["askprice"]) if r["askprice"] is not None else 0.0,
                        "bidprice": float(r["bidprice"]) if r["bidprice"] is not None else 0.0,
                        "supply": int(r["supply"]) if r["supply"] is not None else 0,
                    }
                    for r in rows
                ]
                
                try:
                    await redis_client.set(redis_key, json.dumps(result), ex=300) # 5 min TTL
                except Exception as e:
                    logger.warning(f"Redis set failed for {redis_key}: {e}")

                return result
        except Exception as e:
            logger.error(f"Error fetching ticker history for {full_ticker}: {e}", exc_info=True)
            return []


async def fetch_historical_stability_map(db, days: int = 30) -> Dict[str, Dict[str, Any]]:
    """
    Computes time-series stability %, average price, and average supply for each material.exchange key
    from the cx_brokers_history table over the specified timeframe (days).
    Optimized with Redis caching for instant (< 5ms) response times.
    """
    cache_key = f"cx_stability_map_v3_{days}"
    try:
        cached = await redis_client.get(cache_key)
        if cached:
            return json.loads(cached) if isinstance(cached, str) else cached
    except Exception as e:
        logger.warning(f"Redis cache lookup failed for stability map: {e}")

    query = """
        WITH MaxTime AS (
            SELECT MAX(snapshot_at) AS max_time FROM cx_brokers_history
        ),
        parsed_history AS (
            SELECT 
                CASE 
                    WHEN POSITION('.' IN ticker) > 0 THEN UPPER(SPLIT_PART(ticker, '.', 1))
                    WHEN POSITION('_' IN ticker) > 0 THEN UPPER(SPLIT_PART(ticker, '_', 1))
                    WHEN POSITION('-' IN ticker) > 0 THEN UPPER(SPLIT_PART(ticker, '-', 1))
                    ELSE UPPER(ticker)
                END AS base_ticker,
                CASE 
                    WHEN POSITION('.' IN ticker) > 0 THEN UPPER(SPLIT_PART(ticker, '.', 2))
                    WHEN POSITION('_' IN ticker) > 0 THEN UPPER(SPLIT_PART(ticker, '_', 2))
                    WHEN POSITION('-' IN ticker) > 0 THEN UPPER(SPLIT_PART(ticker, '-', 2))
                    ELSE 'IC1'
                END AS exchange_code,
                CASE 
                    WHEN askprice > 0 AND bidprice > 0 THEN (askprice + bidprice)/2.0 
                    ELSE COALESCE(NULLIF(askprice, 0), NULLIF(bidprice, 0), 0) 
                END AS mid_price,
                COALESCE(supply, 0) AS supply
            FROM cx_brokers_history, MaxTime
            WHERE ticker IS NOT NULL 
              AND (max_time IS NULL OR snapshot_at >= max_time - ($1 || ' days')::INTERVAL)
        ),
        history_stats AS (
            SELECT 
                base_ticker,
                exchange_code,
                AVG(mid_price) AS avg_price,
                STDDEV(mid_price) AS std_price,
                AVG(supply) AS avg_supply,
                COUNT(*) AS sample_count
            FROM parsed_history
            WHERE mid_price > 0
            GROUP BY base_ticker, exchange_code
        )
        SELECT 
            base_ticker,
            exchange_code,
            COALESCE(avg_price, 0) AS avg_price,
            COALESCE(std_price, 0) AS std_price,
            COALESCE(avg_supply, 0) AS avg_supply,
            sample_count
        FROM history_stats;
    """
    try:
        async with db.pool.acquire() as con:
            records = await con.fetch(query, str(days))
            result = {}
            for r in records:
                ticker = r.get("base_ticker")
                ex = r.get("exchange_code")
                avg_p = float(r.get("avg_price") or 0.0)
                std_p = float(r.get("std_price") or 0.0)
                avg_sup = float(r.get("avg_supply") or 0.0)
                samples = int(r.get("sample_count") or 0)

                # Statistical Coefficient of Variation: CoV = std_p / avg_p
                # Stability Score % = 100 - (CoV * 100) bounded between 25% and 99%
                if avg_p > 0 and std_p >= 0:
                    cov = std_p / avg_p
                    stability_score = round(max(25.0, min(99.0, 100.0 - (cov * 100.0))), 1)
                else:
                    stability_score = 75.0

                key = f"{ticker}.{ex}"
                result[key] = {
                    "avg_price": round(avg_p, 2),
                    "std_price": round(std_p, 2),
                    "avg_supply": round(avg_sup, 1),
                    "stability_score": stability_score,
                    "samples": samples
                }
            
            if result:
                try:
                    await redis_client.set(cache_key, json.dumps(result), ex=60)
                except Exception as e:
                    logger.warning(f"Redis cache set failed for stability map: {e}")

            return result
    except Exception as e:
        logger.error(f"Failed to calculate historical stability map: {e}", exc_info=True)
        return {}


async def fetch_ticker_detail(db, ticker: str, exchange: str = "IC1") -> Dict[str, Any]:
    clean_ticker = ticker.upper()
    clean_exchange = exchange.upper()
    full_ticker = f"{clean_ticker}.{clean_exchange}"

    empty_response = {
        "found": False,
        "ticker": clean_ticker,
        "exchange": clean_exchange,
        "full_ticker": full_ticker,
        "priceaverage": 0,
        "askprice": 0,
        "askamount": 0,
        "bidprice": 0,
        "bidamount": 0,
        "supply": 0,
        "demand": 0,
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
                WHERE UPPER(brokermaterialid) = $1
                   OR UPPER(ticker) = $1
                   OR (UPPER(brokermaterialid) LIKE $2 || '%' AND UPPER(brokermaterialid) LIKE '%' || $3)
                   OR (UPPER(ticker) LIKE $2 || '%' AND UPPER(ticker) LIKE '%' || $3)
                LIMIT 1;
                """,
                full_ticker, clean_ticker, clean_exchange
            )

            # Fallback if cx_brokers row is absent or empty: build snapshot from cx_brokers_history
            if not broker_row:
                hist_fallback = await conn.fetchrow(
                    """
                    SELECT 
                        COALESCE(priceaverage, (askprice + bidprice)/2.0, askprice, bidprice, 0) AS priceaverage,
                        COALESCE(askprice, 0) AS askprice,
                        COALESCE(bidprice, 0) AS bidprice,
                        COALESCE(supply, 0) AS supply,
                        snapshot_at AS last_update
                    FROM cx_brokers_history
                    WHERE (UPPER(ticker) = $1 OR (UPPER(ticker) LIKE $2 || '%' AND UPPER(ticker) LIKE '%' || $3))
                    ORDER BY snapshot_at DESC
                    LIMIT 1;
                    """,
                    full_ticker, clean_ticker, clean_exchange
                )
                if hist_fallback:
                    broker_row = {
                        "priceaverage": hist_fallback["priceaverage"],
                        "askprice": hist_fallback["askprice"],
                        "askamount": hist_fallback["supply"],
                        "bidprice": hist_fallback["bidprice"],
                        "bidamount": 0,
                        "supply": hist_fallback["supply"],
                        "demand": 0,
                        "traded": 0,
                        "volume": 0,
                        "brokermaterialid": full_ticker,
                        "last_update": hist_fallback["last_update"]
                    }
                else:
                    return empty_response

            broker_id = broker_row.get("brokermaterialid") or full_ticker

            redis_key = f"cx_stats:{full_ticker}"
            stats_data = None

            try:
                cached_stats = await redis_client.get(redis_key)
                if cached_stats:
                    stats_data = json.loads(cached_stats)
            except Exception as e:
                logger.warning(f"Redis get failed for {redis_key}: {e}")
                stats_data = None

            if not stats_data:
                stats_row = await conn.fetchrow(
                    """
                    SELECT 
                        COALESCE(MAX(COALESCE(priceaverage, askprice, bidprice, price)), 0) AS high,
                        COALESCE(MIN(COALESCE(priceaverage, askprice, bidprice, price)), 0) AS low,
                        COALESCE(MAX(COALESCE(priceaverage, askprice, bidprice, price)), 0) AS alltimehigh,
                        COALESCE(MIN(COALESCE(priceaverage, askprice, bidprice, price)), 0) AS alltimelow
                    FROM cx_brokers_history
                    WHERE UPPER(ticker) = $1 
                       OR (UPPER(ticker) LIKE $2 || '%' AND UPPER(ticker) LIKE '%' || $3);
                    """,
                    full_ticker, clean_ticker, clean_exchange
                )
                stats_data = {
                    "high": float(stats_row["high"] or 0) if stats_row else 0.0,
                    "low": float(stats_row["low"] or 0) if stats_row else 0.0,
                    "alltimehigh": float(stats_row["alltimehigh"] or 0) if stats_row else 0.0,
                    "alltimelow": float(stats_row["alltimelow"] or 0) if stats_row else 0.0,
                }
                try:
                    await redis_client.set(redis_key, json.dumps(stats_data), ex=3600)
                except Exception as e:
                    logger.warning(f"Redis set failed for {redis_key}: {e}")

            bids = []
            asks = []

            if broker_id:
                bid_rows = await conn.fetch(
                    """
                    SELECT priceamount AS price, COALESCE(amount, 0) AS amount, tradername AS trader
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
                    SELECT priceamount AS price, COALESCE(amount, 0) AS amount, tradername AS trader
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
                "supply": int(broker_row["supply"] or 0),
                "demand": int(broker_row["demand"] or 0),
                "high": stats_data.get("high", 0.0),
                "low": stats_data.get("low", 0.0),
                "volume": float(broker_row["volume"] or 0),
                "traded": int(broker_row["traded"] or 0),
                "alltimehigh": stats_data.get("alltimehigh", 0.0),
                "alltimelow": stats_data.get("alltimelow", 0.0),
                "last_update": broker_row["last_update"].isoformat() if broker_row["last_update"] else None,
                "bids": bids,
                "asks": asks
            }


    except Exception as e:
        logger.error(f"Error fetching CX detail for {full_ticker}: {e}", exc_info=True)
        return empty_response

