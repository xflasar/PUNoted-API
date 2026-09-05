import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Path, Request

from app.core.security import require_internal_origin
from app.db.dependencies import get_db
from auth import get_current_user_id

sites_router = APIRouter(dependencies=[Depends(require_internal_origin)])
logger = logging.getLogger(__name__)

@sites_router.get("/all_user_sites")
async def get_all_user_sites(request: Request, user_id: str = Depends(get_current_user_id)):
    db = get_db(request)
    try:
        async with db.pool.acquire() as conn:
            query = """
            WITH Me AS (
                SELECT u.accountid::text as my_account_id, ud.displayname as username, cd.companycode
                FROM users u
                LEFT JOIN users_data ud ON u.userdataid = ud.userid
                LEFT JOIN company_data cd ON u.userdataid = cd.userdataid
                WHERE u.accountid = $1::uuid
            ),
            MyOwnedSites AS (
                SELECT s.siteid::text as siteid
                FROM sites s
                JOIN users u ON u.userdataid = s.userid
                WHERE u.accountid = $1::uuid
            ),
            AllLeaseElements AS (
                SELECT 
                    ugs.userid::uuid as setting_owner_account_id,
                    l->>'siteId' as siteid,
                    l->>'tenant' as tenant
                FROM (
                    SELECT 
                        userid, 
                        NULLIF(TRIM(internal_leased_sites::text), '') AS leased_text 
                    FROM user_global_settings
                ) ugs
                CROSS JOIN LATERAL jsonb_array_elements(
                    CASE 
                        WHEN ugs.leased_text IS NULL THEN '[]'::jsonb
                        WHEN jsonb_typeof(ugs.leased_text::jsonb) = 'array' THEN ugs.leased_text::jsonb
                        WHEN jsonb_typeof(ugs.leased_text::jsonb) = 'string' THEN
                            CASE 
                                WHEN TRIM(ugs.leased_text::jsonb #>> '{}') LIKE '[%' 
                                    THEN (ugs.leased_text::jsonb #>> '{}')::jsonb
                                ELSE '[]'::jsonb
                            END
                        ELSE '[]'::jsonb 
                    END
                ) l
            ),
            MyOutboundLeases AS (
                SELECT ale.siteid, ale.tenant, 'Outbound' as lease_type
                FROM AllLeaseElements ale
                JOIN MyOwnedSites os ON ale.siteid = os.siteid
                CROSS JOIN Me
                WHERE ale.setting_owner_account_id = $1::uuid
                  AND ale.tenant IS NOT NULL
                  AND ale.tenant != Me.username
                  AND ale.tenant != Me.companycode
                  AND ale.tenant != Me.username || ' (' || Me.companycode || ')'
            ),
            MyInboundLeases AS (
                SELECT ale.siteid, 
                       (SELECT COALESCE(ud2.displayname, cd2.companyname, 'Unknown') 
                        FROM users u2 
                        LEFT JOIN users_data ud2 ON ud2.userid = u2.userdataid 
                        LEFT JOIN company_data cd2 ON cd2.userdataid = u2.userdataid 
                        WHERE u2.accountid = ale.setting_owner_account_id) as landlord,
                       'Inbound' as lease_type
                FROM AllLeaseElements ale
                CROSS JOIN Me
                LEFT JOIN MyOwnedSites os ON ale.siteid = os.siteid
                WHERE ale.setting_owner_account_id != $1::uuid
                  AND os.siteid IS NULL
                  AND (
                      ale.tenant = Me.username 
                      OR ale.tenant = Me.companycode 
                      OR ale.tenant = Me.username || ' (' || Me.companycode || ')'
                  )
            ),
            AccessibleSites AS (
                SELECT o.siteid, TRUE as am_owner, outbound.tenant as leased_to, NULL::text as leased_from, COALESCE(outbound.lease_type, 'owned') as lease_type
                FROM MyOwnedSites o
                LEFT JOIN MyOutboundLeases outbound ON outbound.siteid = o.siteid
                UNION ALL
                SELECT inbound.siteid, FALSE as am_owner, NULL::text as leased_to, inbound.landlord as leased_from, inbound.lease_type as lease_type
                FROM MyInboundLeases inbound
            ),
            SiteBuildingBOM AS (
                SELECT 
                    sp.siteid,
                    m.ticker,
                    SUM(COALESCE(pm.amount, bbm.amount, 0)) as amount
                FROM site_platforms sp
                LEFT JOIN platform_materials pm ON pm.platformid = sp.platformid AND (pm.materialtype = 'build' OR pm.materialtype = 'construction')
                LEFT JOIN building_build_materials bbm ON bbm.buildingid = sp.buildingid
                JOIN materials m ON m.materialid = COALESCE(pm.materialid, bbm.materialid)
                GROUP BY sp.siteid, m.ticker
            ),
            SiteBuildingDetails AS (
                SELECT 
                    sp.siteid,
                    b.ticker,
                    COALESCE(b.area, 0) as area
                FROM site_platforms sp
                JOIN buildings b ON b.buildingid = sp.buildingid
            )
            SELECT 
                s.siteid,
                p.naturalid AS planet_name,
                p.planetid,
                CASE 
                    WHEN COALESCE(phys.surface, p.surface) IS NULL THEN ''
                    WHEN COALESCE(phys.surface, p.surface)::text = 'true' THEN 'ROCKY'
                    WHEN COALESCE(phys.surface, p.surface)::text = 'false' THEN 'GASEOUS'
                    ELSE COALESCE(phys.surface, p.surface)::text 
                END AS planet_surface,
                COALESCE(phys.pressure, 1.0) AS planet_pressure,
                COALESCE(phys.gravity, 1.0) AS planet_gravity,
                COALESCE(phys.temperature, p.temperature, 20.0) AS planet_temperature,
                COALESCE(ud.displayname, cd.companycode, 'Unknown') AS owner_name,
                cd.companycode AS owner_company_code,
                acc.am_owner,
                acc.lease_type,
                acc.leased_to,
                acc.leased_from,
                (acc.lease_type = 'Outbound' OR acc.lease_type = 'Inbound') AS is_leased,
                COALESCE(
                    (SELECT ARRAY_AGG(b.ticker ORDER BY b.ticker)
                     FROM site_platforms sp2
                     JOIN buildings b ON b.buildingid = sp2.buildingid
                     WHERE sp2.siteid = s.siteid),
                    '{}'
                ) AS site_building_tickers,
                COALESCE(
                    (SELECT jsonb_object_agg(bom.ticker, bom.amount)
                     FROM SiteBuildingBOM bom
                     WHERE bom.siteid = s.siteid),
                    '{}'::jsonb
                ) AS site_building_materials,
                COALESCE(
                    (SELECT jsonb_agg(jsonb_build_object('ticker', bd.ticker, 'area', bd.area))
                     FROM SiteBuildingDetails bd
                     WHERE bd.siteid = s.siteid),
                    '[]'::jsonb
                ) AS site_building_details
            FROM AccessibleSites acc
            JOIN sites s ON s.siteid::text = acc.siteid
            JOIN planets p ON p.planetid = s.addressplanetid
            LEFT JOIN planet_physical_data phys ON phys.planetid = p.planetid
            JOIN users u ON u.userdataid = s.userid
            LEFT JOIN users_data ud ON ud.userid = u.userdataid
            LEFT JOIN company_data cd ON cd.userdataid = u.userdataid
            GROUP BY s.siteid, p.naturalid, p.planetid, phys.surface, p.surface, phys.pressure, phys.gravity, phys.temperature, p.temperature, ud.displayname, cd.companycode, acc.am_owner, acc.lease_type, acc.leased_to, acc.leased_from;
            """
            rows = await conn.fetch(query, user_id)
            sites = []
            for r in rows:
                raw_mats = r["site_building_materials"]
                base_mats = dict(raw_mats) if isinstance(raw_mats, dict) else {}

                raw_details = r["site_building_details"]
                building_details = list(raw_details) if isinstance(raw_details, list) else []

                surface = str(r["planet_surface"] or "").upper()
                pressure = float(r["planet_pressure"]) if r["planet_pressure"] is not None else 1.0
                gravity = float(r["planet_gravity"]) if r["planet_gravity"] is not None else 1.0
                temperature = float(r["planet_temperature"]) if r["planet_temperature"] is not None else 20.0

                for b in building_details:
                    area = int(b.get("area") or 0)

                    # Surface
                    if surface == "ROCKY" and area > 0:
                        base_mats["MCG"] = base_mats.get("MCG", 0) + (area * 4)
                    elif surface == "GASEOUS" and area > 0:
                        import math
                        base_mats["AEF"] = base_mats.get("AEF", 0) + math.ceil(area / 3.0)

                    # Atmospheric Pressure
                    if pressure < 0.25 and area > 0:
                        base_mats["SEA"] = base_mats.get("SEA", 0) + (area * 1)
                    elif pressure > 2.0:
                        base_mats["HSE"] = base_mats.get("HSE", 0) + 1

                    # Gravity
                    if gravity < 0.25:
                        base_mats["MGC"] = base_mats.get("MGC", 0) + 1
                    elif gravity > 2.5:
                        base_mats["BL"] = base_mats.get("BL", 0) + 1

                    # Temperature
                    if temperature < -25 and area > 0:
                        base_mats["INS"] = base_mats.get("INS", 0) + (area * 10)
                    elif temperature > 75:
                        base_mats["TSH"] = base_mats.get("TSH", 0) + 1

                sites.append({
                    "site_id": r["siteid"],
                    "planet_name": r["planet_name"],
                    "planet_id": r["planetid"],
                    "owner_name": r["owner_name"],
                    "owner_company_code": r["owner_company_code"],
                    "am_owner": r["am_owner"],
                    "lease_type": r["lease_type"],
                    "leased_to": r["leased_to"],
                    "leased_from": r["leased_from"],
                    "is_leased": r["is_leased"],
                    "site_building_tickers": list(r["site_building_tickers"] or []),
                    "site_building_materials": base_mats
                })
            return {"sites": sites}
    except Exception as e:
        logger.error(f"Error fetching all_user_sites: {e}", exc_info=True)
        return {"sites": []}

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
