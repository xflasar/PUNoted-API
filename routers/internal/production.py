import json
import logging
from collections import defaultdict
from datetime import datetime
from typing import Optional, Dict, List, Any

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
AllLeaseElements AS (
    SELECT 
        ugs.userid::uuid as setting_owner_account_id,
        l->>'siteId' as siteid, 
        l->>'tenant' as tenant
    FROM user_global_settings ugs
    CROSS JOIN jsonb_array_elements(
        CASE 
            WHEN ugs.internal_leased_sites IS NULL THEN '[]'::jsonb
            WHEN jsonb_typeof(ugs.internal_leased_sites::jsonb) = 'array' 
                THEN ugs.internal_leased_sites::jsonb
            WHEN jsonb_typeof(ugs.internal_leased_sites::jsonb) = 'string' 
                 AND jsonb_typeof((ugs.internal_leased_sites::jsonb #>> '{}')::jsonb) = 'array' 
                THEN (ugs.internal_leased_sites::jsonb #>> '{}')::jsonb
                
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
)
SELECT 
    o.siteid,
    TRUE as am_owner,
    outbound.tenant as leased_to,
    NULL as leased_from,
    COALESCE(outbound.lease_type, 'owned') as lease_type
FROM MyOwnedSites o
LEFT JOIN MyOutboundLeases outbound ON outbound.siteid = o.siteid

UNION ALL

SELECT 
    inbound.siteid,
    FALSE as am_owner,
    NULL as leased_to,
    inbound.landlord as leased_from,
    inbound.lease_type as lease_type
FROM MyInboundLeases inbound;
"""

# --- QUERY 1: SITES & INFRASTRUCTURE ---
SQL_GET_SITES_AND_INFRA = """
WITH UserSites AS (
    SELECT 
        s.siteid::text as siteid, s.area, s.investedpermits, s.maximumpermits, s.foundedtimestamp, 
        p.naturalid AS planet_name, p.name AS planet_name_alt, 
        p.planetid::text as planetid, 
        s.userid::text as userid
    FROM sites s
    INNER JOIN planets p ON p.planetid = s.addressplanetid
    WHERE s.siteid::text = ANY($1::text[])
)
SELECT 
    us.*,
    -- 1. STRICTLY SITE STORAGE
    (
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
            'material_id', ssi.materialid::text, 
            'ticker', m.ticker, 
            'amount', ssi.quantity,
            'type', 'site'
        )), '[]'::jsonb)
        FROM storages st
        JOIN storage_items ssi ON st.storageid = ssi.storageid
        JOIN materials m ON m.materialid = ssi.materialid
        WHERE st.addressableid::text = us.siteid 
    ) AS site_storage_items,
    
    -- 2. STRICTLY WAREHOUSE STORAGE (Belonging to the Site Owner)
    (
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
            'material_id', ssi.materialid::text, 
            'ticker', m.ticker, 
            'amount', ssi.quantity,
            'type', 'warehouse'
        )), '[]'::jsonb)
        FROM storages st
        JOIN storage_items ssi ON st.storageid = ssi.storageid
        JOIN materials m ON m.materialid = ssi.materialid
        JOIN warehouses wh ON wh.warehouseid = st.addressableid
        WHERE wh.addressplanet = us.planetid 
              AND st.userid IN (us.userid)
    ) AS warehouse_storage_items,
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
        AND (po.completion IS NULL OR po.completion > NOW() OR po.completed IS NOT TRUE)
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
                lease_type = None
                
                if am_owner and leased_to:
                    is_leased = True
                    tenant_str = leased_to
                    lease_type = "Outbound"
                elif not am_owner and leased_from:
                    is_leased = True
                    tenant_str = leased_from
                    lease_type = "Inbound"
                lease_context[sid] = {
                    "type": row.get("lease_type"),
                    "isLeased": is_leased,
                    "tenant": tenant_str,
                    "lease_type": lease_type,
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

                context = lease_context.get(sid, {"type": None, "isLeased": False, "tenant": None})

                results[sid] = {
                    "siteid": sid,
                    "planet_name": row["planet_name"],
                    "planet_name_alt": row["planet_name_alt"],
                    "planetid": row["planetid"],
                    "area": row["area"],
                    "invested_permits": row["investedpermits"],
                    "maximum_permits": row["maximumpermits"],
                    "founded_timestamp": row["foundedtimestamp"].isoformat() if row["foundedtimestamp"] else None,
                    "overall_platform_condition": p_data.get("overall", 0.0),
                    "site_building_tickers": p_data.get("tickers", []),
                    "site_platform_conditions": p_data.get("conditions", []),
                    "platform_repair_list": [],
                    "production_lines": [],
                    "site_daily_flow": {},
                    "type": context["lease_type"],
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
                        "line_daily_flow": {},
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
                # Combine site and warehouse storage for initial flow calculation
                ##for item in site_data["site_storage_items"] + site_data["warehouse_storage_items"]:
                ##    ticker = item["ticker"]
                 ##   if ticker not in daily_flow:
                 ##       daily_flow[ticker] = {"flow": 0.0, "currentAmount": 0.0}
                  ##  daily_flow[ticker]["currentAmount"] += item["amount"]

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

                    line["queue"] = line["production_orders"]
                    hydrated_lines.append(line)

                    line_unscaled_flow = defaultdict(float)

                    orders = line.get("production_orders", [])
                    if not orders:
                        continue

                    # 1. Non-recurring orders (One-time manual craft jobs)
                    batch_active_orders = [o for o in orders if not o.get("recurring") and o.get("completion")]
                    batch_queued_orders = [o for o in orders if not o.get("recurring") and not o.get("completion")]

                    line["line_batch_orders"] = {
                        "active": defaultdict(float),
                        "queued": defaultdict(float),
                    }

                    for batch_order, is_active in [(o, True) for o in batch_active_orders] + [(o, False) for o in batch_queued_orders]:
                        b_recipe = batch_order.get("production_recipe") or {}
                        b_order_dur = float(batch_order.get("duration") or 0)
                        b_recipe_dur = float(b_recipe.get("duration") or 0)
                        b_multiplier = (b_order_dur / b_recipe_dur) if b_recipe_dur > 0 else 1.0

                        for inp_factor in (b_recipe.get("inputs") or []):
                            inp_ticker = inp_factor.get("ticker")
                            if inp_ticker:
                                qty = float(inp_factor.get("factor", 0)) * b_multiplier
                                if inp_ticker not in daily_flow:
                                    daily_flow[inp_ticker] = {"flow": 0.0, "currentAmount": 0.0, "batchProdActive": 0.0, "batchProdQueued": 0.0, "batchConsActive": 0.0, "batchConsQueued": 0.0}
                                if is_active:
                                    daily_flow[inp_ticker]["batchConsActive"] += qty
                                else:
                                    daily_flow[inp_ticker]["batchConsQueued"] += qty

                        for out_factor in (b_recipe.get("outputs") or []):
                            out_ticker = out_factor.get("ticker")
                            if out_ticker:
                                qty = float(out_factor.get("factor", 0)) * b_multiplier
                                target_bucket = "active" if is_active else "queued"
                                line["line_batch_orders"][target_bucket][out_ticker] += qty
                                if out_ticker not in daily_flow:
                                    daily_flow[out_ticker] = {"flow": 0.0, "currentAmount": 0.0, "batchProdActive": 0.0, "batchProdQueued": 0.0, "batchConsActive": 0.0, "batchConsQueued": 0.0}
                                if is_active:
                                    daily_flow[out_ticker]["batchProdActive"] += qty
                                else:
                                    daily_flow[out_ticker]["batchProdQueued"] += qty

                    # 2. Recurring orders (Continuous Daily Flow)
                    recurring_template_orders = [o for o in orders if o.get("recurring") and not o.get("completion")]
                    recurring_active_orders = [o for o in orders if o.get("recurring") and o.get("completion")]
                    flow_orders = recurring_template_orders if recurring_template_orders else recurring_active_orders

                    if not flow_orders:
                        continue

                    flow_orders.sort(
                        key=lambda o: datetime.fromisoformat(o["created"]) if o.get("created") else datetime.max
                    )
                    total_ms = sum((float(o.get("duration") or 0)) for o in flow_orders)

                    if total_ms <= 0:
                        continue

                    daily_cycles = (line.get("capacity", 0) * MS_PER_DAY) / total_ms

                    for active_order in flow_orders:
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
                            daily_flow[ticker] = {"flow": 0.0, "currentAmount": 0.0, "batchProdActive": 0.0, "batchProdQueued": 0.0, "batchConsActive": 0.0, "batchConsQueued": 0.0}
                        daily_flow[ticker]["flow"] += r_flow
                        line["line_daily_flow"][ticker] = r_flow

                site_data["production_lines"] = hydrated_lines
                site_data["site_daily_flow"] = daily_flow

            payload = {
                "owned": [],
                "inbound": [],
                "outbound": []
            }

            for site in results.values():
                if site.get("type") == "Inbound":
                    payload["inbound"].append(site)
                elif site.get("type") == "Outbound":
                    payload["outbound"].append(site)
                else:
                    payload["owned"].append(site)

            return {"success": True, "data": payload}

    except Exception as e:
        logger.error(f"Error fetching production data: {e}", exc_info=True)
        return {"success": False, "message": f"An error occurred: {str(e)}"}

@production_router.post("/get_ship_production")
async def get_ship_production(request: Request, payload: Optional[dict] = None):
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            records = await conn.fetch(
                "SELECT orderid, orderwaittime, price, shiptype, username, position FROM ship_production ORDER BY orderid ASC"
            )
            ship_orders = [dict(record) for record in records]

            storage_records = await conn.fetch("""SELECT
                                                    mt.ticker,
                                                    si.quantity
                                                FROM
                                                    storages AS s
                                                INNER JOIN
                                                    warehouses AS w ON w.warehouseid = s.addressableid
                                                INNER JOIN
                                                    storage_items AS si ON si.storageid = s.storageid
                                                INNER JOIN
                                                    materials AS mt ON mt.materialid = si.materialid
                                                INNER JOIN
                                                    systems AS sys ON w.addresssystem = sys.systemid
                                                INNER JOIN
                                                    users_data AS ud ON ud.userid = s.userid
                                                INNER JOIN
                                                    stations AS st ON st.warehouseid = w.warehouseid
                                                WHERE
                                                    sys.name = 'Hortus'
                                                    AND ud.displayname = 'Filefolders'
                                                    AND st.name != 'Hortus'
                                                    AND mt.ticker IN ('MSL', 'FFC', 'LHP', 'CQL', 'QCR', 'WCB', 'LFL', 'HCB', 'BR1', 'SFE', 'MFE', 'SSC', 'LFE', 'FSE', 'CQM', 'LCB', 'VCB', 'CQS', 'BRS', 'SSL');
                                                """)
            storage_items = [dict(record) for record in storage_records]
        data = {"shiporders": ship_orders, "storageitems": storage_items}
        return {"success": True, "data": data}

    except Exception as e:
        logger.error(f"Failed to fetch ship production data: {e}", exc_info=True)
        return {"success": False, "message": f"Failed to retrieve ship production data: {e}"}

SQL_FETCH_USER_WORKFORCE_INTERNAL = """
    SELECT
        wf.siteid::text AS siteid,
        wf.level,
        wf.population,
        wf.reserve,
        wf.capacity,
        wf.required,
        wf.satisfaction,
        needs_data.needs
    FROM
        workforces wf
    INNER JOIN LATERAL (
        SELECT
            jsonb_agg(
                jsonb_build_object(
                    'ticker', m.ticker,
                    'category', wfn.category,
                    'essential', wfn.essential,
                    'satisfaction', wfn.satisfaction,
                    'unitsperinterval', wfn.unitsperinterval,
                    'unitsper100', wfn.unitsper100,
                    'currentamount', COALESCE(si.quantity, 0) 
                )
            ) AS needs
        FROM
            workforce_needs wfn
        INNER JOIN
            materials m ON m.materialid = wfn.materialid
        LEFT JOIN
            storages st ON st.addressableid = wf.siteid
        LEFT JOIN
            storage_items si ON si.storageid = st.storageid AND si.materialid = wfn.materialid
        WHERE
            wfn.workforceid = wf.workforceid
    ) needs_data ON TRUE
    WHERE
        wf.siteid::text = ANY($1::text[]);
"""

@production_router.get("/user_workforce_with_needs")
async def get_user_workforce_with_needs(request: Request, user_id: str = Depends(get_current_user_id)):
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            allowed_sites_records = await conn.fetch(SQL_GET_ALLOWED_SITES, user_id)
            if not allowed_sites_records:
                return {"success": True, "data": {}}

            target_site_ids = list(set([r["siteid"] for r in allowed_sites_records]))
            records = await conn.fetch(SQL_FETCH_USER_WORKFORCE_INTERNAL, target_site_ids)

        workforce_by_site: dict = {}
        for record in records:
            mutable_record = dict(record)
            site_id = str(mutable_record.pop("siteid"))
            needs_data = mutable_record.get("needs")
            if isinstance(needs_data, str):
                mutable_record["needs"] = json.loads(needs_data)

            if site_id not in workforce_by_site:
                workforce_by_site[site_id] = []
            workforce_by_site[site_id].append(mutable_record)

        return {"success": True, "data": workforce_by_site}
    except Exception as e:
        logger.error(f"Failed to fetch user workforce with needs: {e}", exc_info=True)
        return {"success": False, "message": f"Failed to retrieve workforce data: {e}"}