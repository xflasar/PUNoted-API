import json
import logging
from typing import List, Dict, Any, Optional
from app.core.redis_client import redis_client

logger = logging.getLogger(__name__)

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
    Supports days (relative to max snapshot_at) and custom start_date / end_date.
    Cached in Redis for 15 minutes for maximum speed.
    """
    cache_key = f"cx_hist_ts_{ticker}_{exchange}_{days}_{start_date}_{end_date}"
    try:
        cached = await redis_client.get(cache_key)
        if cached:
            return json.loads(cached) if isinstance(cached, str) else cached
    except Exception as e:
        logger.warning(f"Redis cache lookup failed for ticker history: {e}")

    full_ticker = f"{ticker}.{exchange}" if "." not in ticker else ticker
    base_ticker = ticker.split(".")[0]
    ticker_like = f"%{base_ticker}%.%{exchange}%"

    if start_date and end_date:
        query = """
            SELECT 
                snapshot_at,
                askprice,
                bidprice,
                supply
            FROM cx_brokers_history
            WHERE (UPPER(ticker) = UPPER($1) OR UPPER(ticker) = UPPER($2) OR UPPER(ticker) LIKE UPPER($3))
              AND snapshot_at >= $4::TIMESTAMP
              AND snapshot_at <= $5::TIMESTAMP
            ORDER BY snapshot_at ASC;
        """
        params = [full_ticker, base_ticker, ticker_like, start_date, end_date]
    elif days > 0:
        query = """
            WITH MaxTime AS (
                SELECT MAX(snapshot_at) AS max_time 
                FROM cx_brokers_history 
                WHERE UPPER(ticker) = UPPER($1) OR UPPER(ticker) = UPPER($2) OR UPPER(ticker) LIKE UPPER($3)
            )
            SELECT 
                snapshot_at,
                askprice,
                bidprice,
                supply
            FROM cx_brokers_history, MaxTime
            WHERE (UPPER(ticker) = UPPER($1) OR UPPER(ticker) = UPPER($2) OR UPPER(ticker) LIKE UPPER($3))
              AND (max_time IS NULL OR snapshot_at >= max_time - ($4 || ' days')::INTERVAL)
            ORDER BY snapshot_at ASC;
        """
        params = [full_ticker, base_ticker, ticker_like, str(days)]
    else:
        query = """
            SELECT 
                snapshot_at,
                askprice,
                bidprice,
                supply
            FROM cx_brokers_history
            WHERE (UPPER(ticker) = UPPER($1) OR UPPER(ticker) = UPPER($2) OR UPPER(ticker) LIKE UPPER($3))
            ORDER BY snapshot_at ASC;
        """
        params = [full_ticker, base_ticker, ticker_like]

    try:
        async with db.pool.acquire() as con:
            records = await con.fetch(query, *params)
            result = []
            for r in records:
                snap_time = r.get("snapshot_at")
                result.append({
                    "timestamp": snap_time.isoformat() if hasattr(snap_time, "isoformat") else str(snap_time) if snap_time else None,
                    "askprice": float(r.get("askprice") or 0.0),
                    "bidprice": float(r.get("bidprice") or 0.0),
                    "supply": float(r.get("supply") or 0.0),
                })
            
            if result:
                try:
                    await redis_client.set(cache_key, json.dumps(result), ex=900)
                except Exception as e:
                    logger.warning(f"Redis cache set failed for ticker history: {e}")

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
                    await redis_client.set(cache_key, json.dumps(result), ex=900)
                except Exception as e:
                    logger.warning(f"Redis cache set failed for stability map: {e}")

            return result
    except Exception as e:
        logger.error(f"Failed to calculate historical stability map: {e}", exc_info=True)
        return {}

async def fetch_ticker_detail(db, ticker: str, exchange: str = "IC1") -> Dict[str, Any]:
    full_ticker = f"{ticker}.{exchange}" if "." not in ticker else ticker
    base_ticker = ticker.split(".")[0]
    ticker_like = f"%{base_ticker}%.%{exchange}%"
    
    broker_query = """
        SELECT *
        FROM cx_brokers
        WHERE UPPER(ticker) = UPPER($1) OR UPPER(ticker) = UPPER($2) OR UPPER(ticker) LIKE UPPER($3)
        LIMIT 1;
    """
    
    buy_orders_query = """
        SELECT 
            priceamount AS price,
            amount,
            tradername
        FROM cx_brokers_buy_orders
        WHERE brokermaterialid = $1
        ORDER BY priceamount DESC;
    """
    
    sell_orders_query = """
        SELECT 
            priceamount AS price,
            amount,
            tradername
        FROM cx_brokers_sell_orders
        WHERE brokermaterialid = $1
        ORDER BY priceamount ASC;
    """

    try:
        async with db.pool.acquire() as con:
            broker_row = await con.fetchrow(broker_query, full_ticker, base_ticker, ticker_like)
            if not broker_row:
                return {"ticker": ticker, "exchange": exchange, "found": False, "bids": [], "asks": []}
            
            b_dict = dict(broker_row)
            bm_id = b_dict.get("brokermaterialid")
            
            buy_rows = await con.fetch(buy_orders_query, bm_id) if bm_id else []
            sell_rows = await con.fetch(sell_orders_query, bm_id) if bm_id else []
            
            bids = [{"price": float(r.get("price") or 0), "amount": float(r.get("amount") or 0), "trader": r.get("tradername")} for r in buy_rows]
            asks = [{"price": float(r.get("price") or 0), "amount": float(r.get("amount") or 0), "trader": r.get("tradername")} for r in sell_rows]
            
            up_time = b_dict.get("xata_updatedat") or b_dict.get("pricetime") or b_dict.get("updatedat")
            
            return {
                "found": True,
                "ticker": base_ticker,
                "exchange": exchange,
                "full_ticker": full_ticker,
                "priceaverage": float(b_dict.get("priceaverage") or b_dict.get("price") or 0),
                "askprice": float(b_dict.get("askprice") or 0),
                "askamount": float(b_dict.get("askamount") or 0),
                "bidprice": float(b_dict.get("bidprice") or 0),
                "bidamount": float(b_dict.get("bidamount") or 0),
                "high": float(b_dict.get("high") or 0),
                "low": float(b_dict.get("low") or 0),
                "volume": float(b_dict.get("volume") or 0),
                "traded": float(b_dict.get("traded") or 0),
                "alltimehigh": float(b_dict.get("alltimehigh") or 0),
                "alltimelow": float(b_dict.get("alltimelow") or 0),
                "last_update": up_time.isoformat() if hasattr(up_time, "isoformat") else str(up_time) if up_time else None,
                "bids": bids,
                "asks": asks,
            }
    except Exception as e:
        logger.error(f"Error fetching ticker detail for {full_ticker}: {e}", exc_info=True)
        return {"ticker": ticker, "exchange": exchange, "found": False, "bids": [], "asks": []}
