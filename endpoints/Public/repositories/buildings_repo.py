import logging

logger = logging.getLogger(__name__)

SQL_FETCH_BUILDINGS_BASE = """
WITH BuildingCostsAgg AS (
    SELECT 
        bbm.buildingid, 
        jsonb_agg(
            jsonb_build_object(
                'CommodityName', m.name,
                'CommodityTicker', m.ticker,
                'Weight', m.weight,
                'Volume', m.volume,
                'Amount', bbm.amount
            )
        ) as costs_json
    FROM building_build_materials bbm
    JOIN materials m ON bbm.materialid = m.materialid
    GROUP BY bbm.buildingid
),
WorkforceAgg AS (
    SELECT 
        buildingid,
        SUM(CASE WHEN workforcelevel = 'PIONEER' THEN capacity ELSE 0 END) as pioneers,
        SUM(CASE WHEN workforcelevel = 'SETTLER' THEN capacity ELSE 0 END) as settlers,
        SUM(CASE WHEN workforcelevel = 'TECHNICIAN' THEN capacity ELSE 0 END) as technicians,
        SUM(CASE WHEN workforcelevel = 'ENGINEER' THEN capacity ELSE 0 END) as engineers,
        SUM(CASE WHEN workforcelevel = 'SCIENTIST' THEN capacity ELSE 0 END) as scientists
    FROM building_workforce_capacities
    GROUP BY buildingid
)
SELECT 
    COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'BuildingCosts', COALESCE(bca.costs_json, '[]'::jsonb),
                'BuildingId', b.buildingid,
                'Name', b.name,
                'Ticker', b.ticker,
                'Expertise', b.expertisecategory,
                'Pioneers', COALESCE(wa.pioneers, 0),
                'Settlers', COALESCE(wa.settlers, 0),
                'Technicians', COALESCE(wa.technicians, 0),
                'Engineers', COALESCE(wa.engineers, 0),
                'Scientists', COALESCE(wa.scientists, 0),
                'AreaCost', b.area,
                'UserNameSubmitted', 'System', 
                'Timestamp', to_char(b.xata_updatedat, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
            ) ORDER BY b.ticker
        ), 
        '[]'::jsonb
    )::text as final_json
FROM buildings b
LEFT JOIN BuildingCostsAgg bca ON b.buildingid = bca.buildingid
LEFT JOIN WorkforceAgg wa ON b.buildingid = wa.buildingid
"""

async def get_buildings_json(db, tickers: list = None) -> str:
    query = SQL_FETCH_BUILDINGS_BASE

    try:
        async with db.pool.acquire() as conn:
            if tickers:
                query += " WHERE b.ticker = ANY($1::text[])"
                json_str = await conn.fetchval(query, tickers)

                return json_str or "[]"
            else:
                json_str = await conn.fetchval(query)
                return json_str or "[]"

    except Exception as e:
        logger.error(f"Error fetching buildings data: {e}", exc_info=True)
        raise e
