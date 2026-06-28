
import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Path, Request

from app.core.security import require_internal_origin
from auth import get_current_user_id

sites_router = APIRouter(dependencies=[Depends(require_internal_origin)])
logger = logging.getLogger(__name__)

@sites_router.get(
    "/user_site_platforms/{site_id}",
    summary="Get Site Details and Nested Production Lines",
    description="Retrieves site-wide building/platform details, a list of all production lines, and aggregated repair materials.",
    response_model=Dict[str, Any],
)
async def get_user_siteplatforms(
    request: Request,
    user_account_id: str = Depends(get_current_user_id),
    site_id: str = Path(..., description="The ID of the site to query."),
):
    """
    Executes three separate queries to fetch site-wide platform data, line data,
    and aggregated repair materials, then merges them into the desired nested object structure.
    """

    # 1. QUERY A: Site-Wide Platform & Building Aggregation
    # Fetches all relevant building/platform data and aggregates it into arrays.
    sql_query_A = """
        SELECT
            p.naturalid AS planet_name,
            ARRAY_AGG(b.ticker ORDER BY b.ticker) AS site_building_tickers,
            ARRAY_AGG(sp.condition ORDER BY b.ticker) AS site_platform_conditions
        FROM
            public.sites s
        INNER JOIN
            public.users u ON u.userdataid = s.userid
        INNER JOIN
            public.site_platforms sp ON sp.siteid = s.siteid
        INNER JOIN
            public.buildings b ON b.buildingid = sp.buildingid
        INNER JOIN
            public.planets p ON p.planetid = s.addressplanetid
        WHERE
            s.siteid = $1
        AND
            u.accountid = $2
        AND
            (b.type = 'PRODUCTION' OR b.type = 'RESOURCES')
        GROUP BY
            s.siteid, p.naturalid;
    """

    # 2. QUERY B: Simple Production Line Details
    # Fetches a clean list of all production lines on the site (no platform joins).
    sql_query_B = """
        SELECT
            pl.productionlineid AS line_id,
            pl.condition AS line_condition,
            pl.type AS line_type
        FROM
            public.site_production_lines pl
        INNER JOIN
            public.sites s ON s.siteid = pl.siteid
        INNER JOIN
            public.users u ON u.userdataid = s.userid
        WHERE
            pl.siteid = $1
        AND
            u.accountid = $2
        ORDER BY
            pl.productionlineid;
    """

    # 3. QUERY C: Platform Repair Material Aggregation (New Query)
    # Fetches the sum of repair materials needed for all platforms on the site.
    sql_query_C = """
        WITH UsersWithSellOrders AS (
            SELECT DISTINCT
                u.userdataid,
                uvo.materialid
            FROM
                user_vendor_orders uvo
            INNER JOIN
                user_vendors uv ON uv.vendorid = uvo.vendorid
            INNER JOIN
                users u ON u.accountid = uuid(uv.userid)
            WHERE
                uvo.ordertype = 'sell'
        ),
        FilteredUserSupply AS (
            SELECT
                m.ticker,
                SUM(sti.quantity) AS total_inventory_at_hrt
            FROM
                storage_items sti
            INNER JOIN
                materials m ON m.materialid = sti.materialid
            INNER JOIN
                storages st ON sti.storageid = st.storageid
            INNER JOIN
                warehouses wh ON st.addressableid = wh.warehouseid
            INNER JOIN
                stations stat ON stat.warehouseid = wh.warehouseid
            INNER JOIN
                UsersWithSellOrders uws 
                    ON uws.userdataid = st.userid 
                    AND uws.materialid = sti.materialid 
            WHERE
                stat.naturalid = 'HRT' -- Filter by location
            GROUP BY
                m.ticker
        )
        SELECT
            m.ticker,
            pm.materialtype,
            mp.price AS corp_price,
            cb.price AS market_price,
            cb.supply AS market_supply,
            SUM(pm.amount) AS total_amount,
            COALESCE(fus.total_inventory_at_hrt, 0) AS corp_supply
        FROM
            public.platform_materials pm
        INNER JOIN
            site_platforms sp ON pm.platformid = sp.platformid
        INNER JOIN
            sites s ON s.siteid = sp.siteid
        INNER JOIN
            materials m ON m.materialid = pm.materialid
        INNER JOIN
            material_prices mp ON m.ticker = mp.ticker
        INNER JOIN
            cx_brokers cb ON cb.materialid = m.materialid
        LEFT JOIN
            FilteredUserSupply fus ON fus.ticker = m.ticker
        WHERE
            s.siteid = $1
            AND pm.materialtype = 'repair'
            AND cb.currencyid = 'ICA'
        GROUP BY
            m.ticker,
            pm.materialtype,
            mp.price,
            cb.price,
            cb.supply,
            fus.total_inventory_at_hrt
        ORDER BY
            m.ticker;
    """

    pool = request.app.state.db.pool

    try:
        async with pool.acquire() as conn:
            # --- EXECUTE QUERY A: Get Site-Wide Aggregated Data (Requires site_id and user_account_id) ---
            site_records = await conn.fetch(sql_query_A, site_id, user_account_id)

            # Initialize with defaults in case the site exists but has no relevant buildings
            site_data = {
                "siteid": site_id,
                "planet_name": "Unknown",
                "site_building_tickers": [],
                "site_platform_conditions": [],
                "production_lines": [],
                "platform_repair_list": [],
            }

            if site_records:
                # Merge the aggregated data into the structure
                site_data.update(dict(site_records[0]))

            # --- EXECUTE QUERY B: Get Production Line Details (Requires site_id and user_account_id) ---
            line_records = await conn.fetch(sql_query_B, site_id, user_account_id)

            # --- EXECUTE QUERY C: Get Platform Repair Materials (Requires only site_id) ---
            repair_records = await conn.fetch(sql_query_C, site_id)

            # Add production lines
            site_data["production_lines"] = [dict(r) for r in line_records]

            # Add repair material list
            site_data["platform_repair_list"] = [dict(r) for r in repair_records]

            return site_data

    except Exception as e:
        logger.error(
            f"Database error in get_user_siteplatforms for site_id {site_id}: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=500,
            detail="Internal server error while fetching site production data.",
        )
