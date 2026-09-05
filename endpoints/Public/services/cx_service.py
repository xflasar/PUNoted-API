from typing import Any
from typing import Dict
from typing import List
from typing import Optional
import csv
import json
import logging
from decimal import Decimal
from io import StringIO

from app.core.redis_client import redis_client
from endpoints.Public.repositories.cx_repo import fetch_pivoted_market_data

logger = logging.getLogger(__name__)

CSV_HEADERS = [
    "Ticker", "last_update", "MMBuy", "MMSell",
    "AI1-Average", "AI1-7dAvg", "AI1-30dAvg", "AI1-AskAmt", "AI1-AskPrice", "AI1-AskAvail", "AI1-BidAmt", "AI1-BidPrice", "AI1-BidAvail",
    "CI1-Average", "CI1-7dAvg", "CI1-30dAvg", "CI1-AskAmt", "CI1-AskPrice", "CI1-AskAvail", "CI1-BidAmt", "CI1-BidPrice", "CI1-BidAvail",
    "CI2-Average", "CI2-7dAvg", "CI2-30dAvg", "CI2-AskAmt", "CI2-AskPrice", "CI2-AskAvail", "CI2-BidAmt", "CI2-BidPrice", "CI2-BidAvail",
    "NC1-Average", "NC1-7dAvg", "NC1-30dAvg", "NC1-AskAmt", "NC1-AskPrice", "NC1-AskAvail", "NC1-BidAmt", "NC1-BidPrice", "NC1-BidAvail",
    "NC2-Average", "NC2-7dAvg", "NC2-30dAvg", "NC2-AskAmt", "NC2-AskPrice", "NC2-AskAvail", "NC2-BidAmt", "NC2-BidPrice", "NC2-BidAvail",
    "IC1-Average", "IC1-7dAvg", "IC1-30dAvg", "IC1-AskAmt", "IC1-AskPrice", "IC1-AskAvail", "IC1-BidAmt", "IC1-BidPrice", "IC1-BidAvail",
]

async def generate_market_data_csv(db) -> str:
    try:
        cache_key = "cx_prices_csv_data"
        
        # 1. Check Redis Cache
        cached_csv = await redis_client.get(cache_key)
        if cached_csv:
            return cached_csv

        records = await fetch_pivoted_market_data(db)

        output = StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(CSV_HEADERS)

        # Write data rows
        for record in records:
            row_data = []
            for header in CSV_HEADERS:
                value = record.get(header)
                # Strict fallback for NULL/Empty DB values
                if value is None or value == "":
                    row_data.append("0")
                else:
                    row_data.append(str(value))

            writer.writerow(row_data)

        csv_string = output.getvalue()
        output.close()

        # Cache in Redis for 30 minutes
        await redis_client.set(cache_key, csv_string, ex=1800)

        return csv_string

    except Exception as e:
        logger.error(f"Failed to generate CSV for market data: {e}", exc_info=True)
        raise


async def generate_json_data(
    db,
    tickers: Optional[List[str]] = None,
    brokermaterialid: Optional[str] = None,
    bypass_cache: bool = False,
) -> List[Dict[str, Any]]:
    """Generates JSON-serializable market data list with Redis cache layer."""
    try:
        # 1. Construct correct Redis Cache Key
        base_cache_key = "cx_prices_json_data"
        if tickers:
            cache_key = f"{base_cache_key}_filtered:{','.join(sorted(tickers))}"
        elif brokermaterialid:
            cache_key = f"{base_cache_key}_filtered:{brokermaterialid}"
        else:
            cache_key = base_cache_key

        # 2. Check Redis Cache (skip if brokermaterialid or bypass_cache is True)
        if not brokermaterialid and not bypass_cache:
            cached_raw = await redis_client.get(cache_key)
            if cached_raw:
                if isinstance(cached_raw, bytes):
                    cached_raw = cached_raw.decode("utf-8")
                return json.loads(cached_raw)

        # 3. Fetch from Database
        records = await fetch_pivoted_market_data(
            db, tickers=tickers, brokermaterialids=brokermaterialid
        )
        json_data: List[Dict[str, Any]] = []

        target_exchange: Optional[str] = None
        if brokermaterialid and "." in brokermaterialid:
            target_exchange = brokermaterialid.split(".")[1].upper()

        for record in records:
            row_dict: Dict[str, Any] = {}
            for header in CSV_HEADERS:
                if header == "last_update":
                    continue

                # If single CX update (e.g. GIN.IC1), skip fields for other exchanges (e.g. AI1-*)
                if target_exchange and "-" in header:
                    ex_prefix = header.split("-")[0].upper()
                    if ex_prefix in ("AI1", "CI1", "CI2", "NC1", "NC2", "IC1") and ex_prefix != target_exchange:
                        continue

                val = record.get(header)

                # Strict fallback to prevent React NaN errors from DB NULLs
                if val is None or val == "":
                    row_dict[header] = 0
                elif isinstance(val, Decimal):
                    row_dict[header] = float(val)
                else:
                    row_dict[header] = val

            json_data.append(row_dict)

        # 4. Store full response in Redis for 30 minutes (do not cache partial responses)
        if not brokermaterialid:
            await redis_client.set(cache_key, json.dumps(json_data), ex=1800)
        else:
            # Invalidate base cache key so future full queries fetch fresh data
            await redis_client.delete(base_cache_key)

        return json_data

    except Exception as e:
        logger.error(
            f"Failed to generate JSON data for market data: {e}", exc_info=True
        )
        raise

async def generate_partial_cx_data(db, brokermaterialid: str) -> Dict[str, Any]:
    """
    Generates a strict partial market data update dictionary for a single broker material update.
    Returns: { "WIN": { "Ticker": "WIN", "IC1-Average": ..., "IC1-7dAvg": ..., ... } }
    Guarantees NO other exchange keys (AI1, CI1, CI2, NC1, NC2) are present.
    """
    try:
        query = """
        SELECT 
            brokermaterialid,
            ticker,
            priceaverage,
            askamount,
            askprice,
            bidamount,
            bidprice,
            xata_updatedat
        FROM cx_brokers
        WHERE brokermaterialid = $1 OR ticker = $1;
        """
        async with db.pool.acquire() as conn:
            row = await conn.fetchrow(query, brokermaterialid)
            if not row:
                return {}

            ticker_full = row["ticker"]
            if not ticker_full or "." not in ticker_full:
                return {}

            mat_ticker, ex_code = ticker_full.split(".", 1)
            mat_ticker = mat_ticker.upper()
            ex_code = ex_code.upper()

            avg_query = """
            SELECT
                ROUND(COALESCE(AVG(px) FILTER (WHERE ts >= NOW() - INTERVAL '7 days'), 0)::numeric, 2) AS avg_7d,
                ROUND(COALESCE(AVG(px) FILTER (WHERE ts >= NOW() - INTERVAL '30 days'), 0)::numeric, 2) AS avg_30d
            FROM (
                SELECT 
                    COALESCE(priceaverage, askprice, bidprice, price) AS px,
                    snapshot_at AS ts
                FROM cx_brokers_history
                WHERE ticker = $1
                  AND snapshot_at >= NOW() - INTERVAL '30 days'
                  AND COALESCE(priceaverage, askprice, bidprice, price) > 0
                
                UNION ALL
                
                SELECT 
                    COALESCE(priceaverage, askprice, bidprice, price) AS px,
                    COALESCE(xata_updatedat, NOW()) AS ts
                FROM cx_brokers
                WHERE ticker = $1
                  AND COALESCE(priceaverage, askprice, bidprice, price) > 0
            ) combined;
            """
            avg_row = await conn.fetchrow(avg_query, ticker_full)

            avg_7d = float(avg_row["avg_7d"]) if avg_row and avg_row["avg_7d"] else 0.0
            avg_30d = float(avg_row["avg_30d"]) if avg_row and avg_row["avg_30d"] else 0.0

            px_avg = float(row["priceaverage"]) if row["priceaverage"] is not None else 0.0
            ask_amt = int(row["askamount"]) if row["askamount"] is not None else 0
            ask_px = float(row["askprice"]) if row["askprice"] is not None else 0.0
            bid_amt = int(row["bidamount"]) if row["bidamount"] is not None else 0
            bid_px = float(row["bidprice"]) if row["bidprice"] is not None else 0.0

            item_data = {
                "Ticker": mat_ticker,
                f"{ex_code}-Average": px_avg,
                f"{ex_code}-7dAvg": avg_7d,
                f"{ex_code}-30dAvg": avg_30d,
                f"{ex_code}-AskAmt": ask_amt,
                f"{ex_code}-AskPrice": ask_px,
                f"{ex_code}-AskAvail": ask_amt,
                f"{ex_code}-BidAmt": bid_amt,
                f"{ex_code}-BidPrice": bid_px,
                f"{ex_code}-BidAvail": bid_amt,
            }

            try:
                await redis_client.delete("cx_prices_json_data")
            except Exception:
                pass

            return {mat_ticker: item_data}

    except Exception as e:
        logger.error(f"Failed to generate partial CX data for {brokermaterialid}: {e}", exc_info=True)
        return {}