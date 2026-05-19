import json
import logging
from collections import defaultdict
from datetime import datetime

from fastapi import APIRouter, Depends, Request

from app.core.security import require_internal_origin
from auth import get_current_user_id

production_router = APIRouter(dependencies=[Depends(require_internal_origin)])
logger = logging.getLogger(__name__)

MS_PER_DAY = 1000 * 60 * 60 * 24.0


# --- HELPER: Safe JSON Parsing ---
def safe_json(value):
    if value is None:
        return []
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return []
    return value


# --- QUERY 0: RESOLVE ALLOWED SITES & LEASE CONTEXT ---
SQL_GET_ALLOWED_SITES = """
WITH Me AS (
    SELECT ud.displayname as username, cd.companycode
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
MyOutboundLeases AS (
    SELECT l->>'siteId' as siteid, l->>'tenant' as tenant
    FROM user_global_settings ugs
    CROSS JOIN jsonb_array_elements(COALESCE(ugs.internal_leased_sites, '[]'::jsonb)) l
    WHERE ugs.userid::text = $1::text
),
MyInboundLeases AS (
    SELECT l->>'siteId' as siteid, 
           (SELECT COALESCE(ud2.displayname, cd2.companyname, 'Unknown') 
            FROM users u2 
            LEFT JOIN users_data ud2 ON ud2.userid = u2.userdataid 
            LEFT JOIN company_data cd2 ON cd2.userdataid = u2.userdataid 
            WHERE u2.accountid::text = ugs.userid::text) as landlord
    FROM user_global_settings ugs
    CROSS JOIN jsonb_array_elements(COALESCE(ugs.internal_leased_sites, '[]'::jsonb)) l
    CROSS JOIN Me
    WHERE ugs.userid::text != $1::text
      AND (
          l->>'tenant' = Me.username 
          OR l->>'tenant' = Me.companycode 
          OR l->>'tenant' = Me.username || ' (' || Me.companycode || ')'
      )
)
SELECT 
    o.siteid,
    TRUE as am_owner,
    outbound.tenant as leased_to,
    NULL as leased_from
FROM MyOwnedSites o
LEFT JOIN MyOutboundLeases outbound ON outbound.siteid = o.siteid

UNION ALL

SELECT 
    inbound.siteid,
    FALSE as am_owner,
    NULL as leased_to,
    inbound.landlord as leased_from
FROM MyInboundLeases inbound;
"""

# --- QUERY 1: SITES & INFRASTRUCTURE ---
SQL_GET_SITES_AND_INFRA = """
WITH UserSites AS (
    SELECT 
        s.siteid::text as siteid, s.area, s.investedpermits, s.maximumpermits, s.foundedtimestamp, 
        p.naturalid AS planet_name, p.name AS planet_name_alt
    FROM sites s
    INNER JOIN planets p ON p.planetid = s.addressplanetid
    WHERE s.siteid::text = ANY($1::text[])
)
SELECT 
    us.*,
    (
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
            'material_id', ssi.materialid::text, 'ticker', m.ticker, 'amount', ssi.quantity
        )), '[]'::jsonb)
        FROM storage_items ssi
        JOIN storages st ON st.storageid = ssi.storageid
        JOIN materials m ON m.materialid = ssi.materialid
        WHERE st.addressableid::text = us.siteid
    ) AS storage_items,
    (
        SELECT jsonb_build_object(
            'overall', COALESCE(AVG(CASE WHEN b.type IN ('PRODUCTION', 'RESOURCES') THEN sp.condition END), 0.0),
            'tickers', (
                SELECT COALESCE(jsonb_agg(DISTINCT b2.ticker ORDER BY b2.ticker), '[]'::jsonb) 
                FROM site_platforms sp2 JOIN buildings b2 ON b2.buildingid = sp2.buildingid 
                WHERE sp2.siteid::text = us.siteid
            ),
            'conditions', COALESCE(jsonb_agg(jsonb_build_object('building_ticker', b.ticker, 'platform_condition', sp.condition)), '[]'::jsonb)
        )
        FROM site_platforms sp
        JOIN buildings b ON b.buildingid = sp.buildingid
        WHERE sp.siteid::text = us.siteid
    ) AS platform_data
FROM UserSites us;
"""

# --- QUERY 2: LINES & ORDERS ---
SQL_GET_LINES_AND_QUEUES = """
SELECT 
    pl.siteid::text as siteid,
    pl.productionlineid::text as productionlineid,
    pl.type,
    pl.slots,
    pl.capacity,
    pl.efficiency,
    pl.condition,
    (
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
            'order_id', po.orderid::text,
            'created', po.created,
            'completion', po.completion,
            'duration', po.duration,
            'halted', po.halted,
            'recurring', po.recurring,
            'completed', po.completed,
            'started', po.started,
            'recipe_id', po.recipeid::text
        ) ORDER BY po.created ASC), '[]'::jsonb)
        FROM site_production_line_orders po
        WHERE po.productionlineid = pl.productionlineid
        AND po.completion IS NULL
    ) AS production_orders
FROM site_production_lines pl
WHERE pl.siteid::text = ANY($1::text[]);
"""

# --- QUERY 3: RECIPE PARTS (SPLIT) ---
SQL_FETCH_RECIPES_CORE = """
WITH Targets AS (
    SELECT unnest($1::text[]) as t_id, unnest($2::text[]) as l_id
)
SELECT 
    r.productiontemplateid::text as recipe_id, 
    r.productionlineid::text as line_id,
    r.duration,
    r.name,
    r.efficiency,
    r.effortfactor as effort_factor
FROM production_recipes r
JOIN Targets t ON r.productiontemplateid::text = t.t_id AND r.productionlineid::text = t.l_id
"""

SQL_FETCH_RECIPE_INPUTS = """
WITH Targets AS (
    SELECT unnest($1::text[]) as t_id, unnest($2::text[]) as l_id
)
SELECT 
    i.productiontemplateid::text as recipe_id, 
    i.productionlineid::text as line_id,
    m.ticker, 
    i.factor
FROM production_recipe_input_factors i
JOIN Targets t ON i.productiontemplateid::text = t.t_id AND i.productionlineid::text = t.l_id
JOIN materials m ON m.materialid = i.materialid
"""

SQL_FETCH_RECIPE_OUTPUTS = """
WITH Targets AS (
    SELECT unnest($1::text[]) as t_id, unnest($2::text[]) as l_id
)
SELECT 
    o.productiontemplateid::text as recipe_id, 
    o.productionlineid::text as line_id,
    m.ticker, 
    o.factor
FROM production_recipe_output_factors o
JOIN Targets t ON o.productiontemplateid::text = t.t_id AND o.productionlineid::text = t.l_id
JOIN materials m ON m.materialid = o.materialid
"""


@production_router.get("/user_production")
async def get_user_production(
    request: Request,
    user_id: str = Depends(get_current_user_id),
):
    try:
        async with request.app.state.db.pool.acquire() as conn:
            
            # --- STEP 0: RESOLVE ALLOWED SITES & LEASE CONTEXT ---
            allowed_sites_records = await conn.fetch(SQL_GET_ALLOWED_SITES, user_id)
            if not allowed_sites_records:
                return {"success": True, "data": {}}

            target_site_ids = list(set([r["siteid"] for r in allowed_sites_records]))
            
            lease_context = {}
            for row in allowed_sites_records:
                sid = row["siteid"]
                am_owner = row["am_owner"]
                leased_to = row["leased_to"]
                leased_from = row["leased_from"]
                
                is_leased = False
                tenant_str = None
                
                if am_owner and leased_to:
                    is_leased = True
                    tenant_str = leased_to
                elif not am_owner and leased_from:
                    is_leased = True
                    tenant_str = f"Owner: {leased_from}"
                    
                lease_context[sid] = {
                    "isLeased": is_leased,
                    "tenant": tenant_str
                }

            # --- STEP 1: SITES ---
            sites_records = await conn.fetch(SQL_GET_SITES_AND_INFRA, target_site_ids)

            if not sites_records:
                return {"success": True, "data": {}}

            results = {}
            for row in sites_records:
                sid = row["siteid"]
                p_data = safe_json(row["platform_data"])
                if not isinstance(p_data, dict):
                    p_data = {}

                storage_data = safe_json(row["storage_items"])
                context = lease_context.get(sid, {"isLeased": False, "tenant": None})

                results[sid] = {
                    "siteid": sid,
                    "planet_name": row["planet_name"],
                    "planet_name_alt": row["planet_name_alt"],
                    "area": row["area"],
                    "invested_permits": row["investedpermits"],
                    "maximum_permits": row["maximumpermits"],
                    "founded_timestamp": row["foundedtimestamp"].isoformat() if row["foundedtimestamp"] else None,
                    "overall_platform_condition": p_data.get("overall", 0.0),
                    "site_building_tickers": p_data.get("tickers", []),
                    "site_platform_conditions": p_data.get("conditions", []),
                    "platform_repair_list": [],
                    "storage_items": storage_data,
                    "production_lines": [],
                    "site_daily_flow": {},
                    "isLeased": context["isLeased"],
                    "tenant": context["tenant"],
                }

            # --- STEP 2: LINES & ORDER PAIRS ---
            lines_records = await conn.fetch(SQL_GET_LINES_AND_QUEUES, target_site_ids)

            recipe_line_pairs = set()
            site_lines_map = {}

            for row in lines_records:
                line_id = row["productionlineid"]
                orders = safe_json(row["production_orders"])

                for order in orders:
                    rid = order.get("recipe_id")
                    if rid:
                        recipe_line_pairs.add((rid, line_id))

                sid = row["siteid"]
                if sid not in site_lines_map:
                    site_lines_map[sid] = []

                site_lines_map[sid].append(
                    {
                        "line_id": line_id,
                        "type": row["type"],
                        "slots": row["slots"],
                        "capacity": row["capacity"],
                        "efficiency": row["efficiency"],
                        "condition": row["condition"],
                        "production_orders": orders,
                    }
                )

            # --- STEP 3: SPLIT RECIPE FETCHING ---
            recipe_map = {}

            if recipe_line_pairs:
                target_r_ids, target_l_ids = map(list, zip(*recipe_line_pairs))

                # 3.1 Fetch Core
                core_rows = await conn.fetch(SQL_FETCH_RECIPES_CORE, target_r_ids, target_l_ids)
                for r in core_rows:
                    key = (r["line_id"], r["recipe_id"])
                    recipe_map[key] = {
                        "name": r["name"],
                        "efficiency": r["efficiency"],
                        "effort_factor": r["effort_factor"],
                        "duration": r["duration"],
                        "inputs": [],
                        "outputs": [],
                    }

                # 3.2 Fetch Inputs
                input_rows = await conn.fetch(SQL_FETCH_RECIPE_INPUTS, target_r_ids, target_l_ids)
                for i in input_rows:
                    key = (i["line_id"], i["recipe_id"])
                    if key in recipe_map:
                        recipe_map[key]["inputs"].append({"ticker": i["ticker"], "factor": i["factor"]})

                # 3.3 Fetch Outputs
                output_rows = await conn.fetch(SQL_FETCH_RECIPE_OUTPUTS, target_r_ids, target_l_ids)
                for o in output_rows:
                    key = (o["line_id"], o["recipe_id"])
                    if key in recipe_map:
                        recipe_map[key]["outputs"].append({"ticker": o["ticker"], "factor": o["factor"]})

            # --- STEP 4: STITCH & CALCULATE ---
            for site_id, site_data in results.items():
                raw_lines = site_lines_map.get(site_id, [])
                daily_flow = {}

                # 4a. Initialize Flow with Current Storage
                for item in site_data["storage_items"]:
                    ticker = item["ticker"]
                    if ticker not in daily_flow:
                        daily_flow[ticker] = {"flow": 0.0, "currentAmount": 0.0}
                    daily_flow[ticker]["currentAmount"] = item["amount"]

                hydrated_lines = []

                for line in raw_lines:
                    line_id = line["line_id"]
                    # 4b. Hydrate Orders
                    for order in line["production_orders"]:
                        rid = order.get("recipe_id")
                        if rid and (line_id, rid) in recipe_map:
                            order["production_recipe"] = recipe_map[(line_id, rid)]
                        else:
                            order["production_recipe"] = {
                                "name": "Unknown",
                                "inputs": [],
                                "outputs": [],
                            }

                    hydrated_lines.append(line)

                    line_unscaled_flow = defaultdict(float)

                    orders = line.get("production_orders", [])
                    if not orders:
                        continue

                    orders.sort(
                        key=lambda o: datetime.fromisoformat(o["created"]) if o.get("created") else datetime.max
                    )
                    total_ms = sum((float(o.get("duration") or 0)) for o in orders)

                    if total_ms <= 0:
                        continue

                    daily_cycles = (line.get("capacity", 0) * MS_PER_DAY) / total_ms

                    for active_order in orders:
                        recipe = active_order.get("production_recipe") or {}
                        order_duration = float(active_order.get("duration") or 0)
                        recipe_duration = float(recipe.get("duration") or 0)

                        if recipe_duration == 0:
                            continue

                        duration_multiplier = order_duration / recipe_duration

                        inputs = recipe.get("inputs") or []
                        for inp in inputs:
                            ticker = inp.get("ticker")
                            if not ticker:
                                continue
                            factor = -inp.get("factor", 0) * duration_multiplier
                            line_unscaled_flow[ticker] += factor

                        outputs = recipe.get("outputs") or []
                        for out in outputs:
                            ticker = out.get("ticker")
                            if not ticker:
                                continue
                            factor = out.get("factor", 0) * duration_multiplier
                            line_unscaled_flow[ticker] += factor

                    # 4c. Scale Flow by Line Capacity & Duration
                    for ticker, unscaled_flow in line_unscaled_flow.items():
                        r_flow = unscaled_flow * daily_cycles
                        if ticker not in daily_flow:
                            daily_flow[ticker] = {"flow": 0.0, "currentAmount": 0.0}
                        daily_flow[ticker]["flow"] += r_flow

                site_data["production_lines"] = hydrated_lines
                site_data["site_daily_flow"] = daily_flow

            return {"success": True, "data": results}

    except Exception as e:
        logger.error(f"Error fetching production data: {e}", exc_info=True)
        return {"success": False, "message": f"An error occurred: {str(e)}"}