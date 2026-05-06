from fastapi import APIRouter, Depends, Request, Response
import json

from app.core.security import require_internal_origin
from app.services.materials_service import MaterialsService
from app.api.db.dependencies import get_db

cx_internal_router = APIRouter(dependencies=[Depends(require_internal_origin)])

@cx_internal_router.get("/prices", description="Get CX market data in JSON format. Internal access only.")
async def get_cx_prices_json_internal(
    request: Request,
):
    db = get_db(request)

    query = """
    WITH HistoryAverages AS (
        SELECT 
            ticker,
            -- 7-Day Averages
            AVG(askprice) FILTER (WHERE snapshot_at >= CURRENT_DATE - INTERVAL '7 days') AS askprice_7d_avg,
            AVG(bidprice) FILTER (WHERE snapshot_at >= CURRENT_DATE - INTERVAL '7 days') AS bidprice_7d_avg,
            AVG(supply) FILTER (WHERE snapshot_at >= CURRENT_DATE - INTERVAL '7 days') AS supply_7d_avg,
            
            -- 30-Day Averages
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

    # Acquire a connection from the pool and ensure it is released
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(query)

    parsed_data = [dict(row) for row in rows]

    return Response(
        content=json.dumps(parsed_data, default=str), 
        media_type="application/json",
        headers={
            "Cache-Control": "no-store"
        }
    )