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

@cx_internal_router.get("/prices", description="Get CX market data in JSON format. Internal access only.")
async def get_cx_prices_json_internal(request: Request):
    try:
        cache_key = "internal_cx_prices_ic1"
        
        # 1. Check Redis Cache First
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            # Return cached data immediately, skipping the DB entirely
            return JSONResponse(content=json.loads(cached_data))

        db = get_db(request)

        # 2. Execute the heavy aggregation query
        query = """
        WITH HistoryAverages AS (
            SELECT 
                ticker,
                AVG(askprice) FILTER (WHERE snapshot_at >= CURRENT_DATE - INTERVAL '7 days') AS askprice_7d_avg,
                AVG(bidprice) FILTER (WHERE snapshot_at >= CURRENT_DATE - INTERVAL '7 days') AS bidprice_7d_avg,
                AVG(supply) FILTER (WHERE snapshot_at >= CURRENT_DATE - INTERVAL '7 days') AS supply_7d_avg,
                AVG(askprice) FILTER (WHERE snapshot_at >= CURRENT_DATE - INTERVAL '30 days') AS askprice_30d_avg,
                AVG(bidprice) FILTER (WHERE snapshot_at >= CURRENT_DATE - INTERVAL '30 days') AS bidprice_30d_avg,
                AVG(supply) FILTER (WHERE snapshot_at >= CURRENT_DATE - INTERVAL '30 days') AS supply_30d_avg
            FROM cx_brokers_history
            WHERE snapshot_at >= CURRENT_DATE - INTERVAL '30 days'
              AND SPLIT_PART(ticker, '.', 2) = 'IC1'
            GROUP BY ticker
        )
        SELECT 
            cxb.ticker,
            cxb.askprice,
            cxb.bidprice, 
            cxb.supply,
            h.askprice_7d_avg,
            h.bidprice_7d_avg,
            h.supply_7d_avg,
            h.askprice_30d_avg,
            h.bidprice_30d_avg,
            h.supply_30d_avg
        FROM cx_brokers cxb
        LEFT JOIN HistoryAverages h ON cxb.ticker = h.ticker
        WHERE SPLIT_PART(cxb.ticker, '.', 2) = 'IC1';
        """

        async with db.pool.acquire() as conn:
            rows = await conn.fetch(query)

        # 3. Safely parse Decimals to Floats so the React frontend can do math
        parsed_data = []
        for row in rows:
            row_dict = dict(row)
            for key, val in row_dict.items():
                if isinstance(val, Decimal):
                    row_dict[key] = float(val)
                elif val is None:
                    row_dict[key] = 0.0 # Fallback for NULL values
            parsed_data.append(row_dict)

        # 4. Wrap the response in the format expected by our React useShipPrices hook
        response_payload = parsed_data

        # 5. Cache the result in Redis (e.g., for 30 minutes / 1800 seconds)
        await redis_client.set(cache_key, json.dumps(response_payload), ex=1800)

        return JSONResponse(content=response_payload)

    except Exception as e:
        logger.error(f"Failed to fetch internal CX prices: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": "Internal server error"}
        )