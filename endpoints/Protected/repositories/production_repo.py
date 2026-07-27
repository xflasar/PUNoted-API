import json
import logging
from collections import defaultdict
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

MS_PER_DAY = 86400000

# --- QUERY 1: LINES & ORDERS ---
SQL_GET_LINES_AND_QUEUES = """
WITH AllowedSites AS (
    -- 1. Sites owned by the users
    SELECT 
        u.username,
        s.siteid,
        s.addressplanetid,
        s.addresssystemid,
        COALESCE(
            (SELECT TRUE FROM user_global_settings ugs2
             CROSS JOIN jsonb_array_elements(
                 CASE 
                     WHEN ugs2.internal_leased_sites IS NULL THEN '[]'::jsonb
                     WHEN jsonb_typeof(ugs2.internal_leased_sites::jsonb) = 'array' 
                         THEN ugs2.internal_leased_sites::jsonb
                     ELSE '[]'::jsonb 
                 END
             ) l2
             WHERE ugs2.userid = u.accountid::text
               AND l2->>'siteId' = s.siteid
               AND l2->>'tenant' IS NOT NULL
               LIMIT 1
            ), FALSE
        ) as is_leased,
        COALESCE(
            (SELECT 'Outbound'::text FROM user_global_settings ugs2
             CROSS JOIN jsonb_array_elements(
                 CASE 
                     WHEN ugs2.internal_leased_sites IS NULL THEN '[]'::jsonb
                     WHEN jsonb_typeof(ugs2.internal_leased_sites::jsonb) = 'array' 
                         THEN ugs2.internal_leased_sites::jsonb
                     ELSE '[]'::jsonb 
                 END
             ) l2
             WHERE ugs2.userid = u.accountid::text
               AND l2->>'siteId' = s.siteid
               AND l2->>'tenant' IS NOT NULL
               LIMIT 1
            ), 'owned'::text
        ) as lease_type,
        (SELECT l2->>'tenant'::text FROM user_global_settings ugs2
         CROSS JOIN jsonb_array_elements(
             CASE 
                 WHEN ugs2.internal_leased_sites IS NULL THEN '[]'::jsonb
                 WHEN jsonb_typeof(ugs2.internal_leased_sites::jsonb) = 'array' 
                     THEN ugs2.internal_leased_sites::jsonb
                 ELSE '[]'::jsonb 
             END
         ) l2
         WHERE ugs2.userid = u.accountid::text
           AND l2->>'siteId' = s.siteid
           AND l2->>'tenant' IS NOT NULL
           LIMIT 1
        ) as leased_to,
        NULL::text as leased_from
    FROM sites s
    JOIN users u ON u.userdataid = s.userid
    WHERE u.username = ANY($1::text[])

    UNION DISTINCT

    -- 2. Sites leased by the users as tenants (configured in landlord's settings)
    SELECT 
        u_tenant.username,
        l->>'siteId' as siteid,
        s.addressplanetid,
        s.addresssystemid,
        TRUE as is_leased,
        'Inbound'::text as lease_type,
        NULL::text as leased_to,
        (SELECT COALESCE(ud2.displayname, cd2.companyname, 'Unknown') 
         FROM users u2 
         LEFT JOIN users_data ud2 ON ud2.userid = u2.userdataid 
         LEFT JOIN company_data cd2 ON cd2.userdataid = u2.userdataid 
         WHERE u2.accountid::text = ugs.userid) as leased_from
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
    JOIN sites s ON s.siteid = l->>'siteId'
    JOIN users u_tenant ON (
        l->>'tenant' = u_tenant.username
        OR l->>'tenant' = (SELECT companycode FROM company_data WHERE userdataid = u_tenant.userdataid)
        OR l->>'tenant' = u_tenant.username || ' (' || (SELECT companycode FROM company_data WHERE userdataid = u_tenant.userdataid) || ')'
    )
    WHERE u_tenant.username = ANY($1::text[])
)
SELECT 
    ast.username,
    ast.addressplanetid as planetid,
    p.naturalid as planetnaturalid,
    p.name as planetname,
    pl.siteid,
    pl.productionlineid,
    pl.type,
    pl.slots,
    pl.capacity,
    pl.efficiency,
    pl.condition,
    pl.xata_updatedat,
    ast.is_leased,
    ast.lease_type,
    ast.leased_to,
    ast.leased_from,
    (
        SELECT COALESCE(jsonb_agg(jsonb_build_object(
            'OrderId', po.orderid,
            'Created', po.created,
            'Completion', po.completion,
            'DurationMs', po.duration,
            'Halted', po.halted,
            'Recurring', po.recurring,
            'Completed', po.completed,
            'Started', po.started,
            'RecipeId', po.recipeid
        ) ORDER BY po.created ASC), '[]'::jsonb)
        FROM site_production_line_orders po
        WHERE po.productionlineid = pl.productionlineid
    ) AS production_orders
FROM site_production_lines pl
INNER JOIN AllowedSites ast ON ast.siteid = pl.siteid
LEFT JOIN planets p ON p.planetid = ast.addressplanetid
LEFT JOIN systems sys ON sys.systemid = ast.addresssystemid
WHERE ($2::text IS NULL OR (
      p.name ILIKE $2 OR 
      p.naturalid ILIKE $2 OR
      sys.name ILIKE $2 OR
      sys.naturalid ILIKE $2
  ));
"""

# --- QUERY 2: RECIPE PARTS (SPLIT) ---
SQL_FETCH_RECIPES_CORE = """
WITH Targets AS (
    SELECT unnest($1::text[]) as t_id, unnest($2::text[]) as l_id
)
SELECT 
    r.productiontemplateid as recipe_id, 
    r.productionlineid as line_id,
    r.duration,
    r.name,
    r.efficiency,
    r.effortfactor as effort_factor
FROM production_recipes r
JOIN Targets t ON r.productiontemplateid = t.t_id AND r.productionlineid = t.l_id
"""

SQL_FETCH_RECIPE_INPUTS = """
WITH Targets AS (
    SELECT unnest($1::text[]) as t_id, unnest($2::text[]) as l_id
)
SELECT 
    i.productiontemplateid as recipe_id, 
    i.productionlineid as line_id,
    m.ticker, 
    i.factor
FROM production_recipe_input_factors i
JOIN Targets t ON i.productiontemplateid = t.t_id AND i.productionlineid = t.l_id
JOIN materials m ON m.materialid = i.materialid
"""

SQL_FETCH_RECIPE_OUTPUTS = """
WITH Targets AS (
    SELECT unnest($1::text[]) as t_id, unnest($2::text[]) as l_id
)
SELECT 
    o.productiontemplateid as recipe_id, 
    o.productionlineid as line_id,
    m.ticker, 
    o.factor
FROM production_recipe_output_factors o
JOIN Targets t ON o.productiontemplateid = t.t_id AND o.productionlineid = t.l_id
JOIN materials m ON m.materialid = o.materialid
"""

async def search_production_lines(conn, usernames_list: list, location: str = None, burn: bool = False, simple: bool = False) -> str:
    p_location = f"%{location}%" if location else None

    # 1. Fetch Lines and Orders
    rows = await conn.fetch(SQL_GET_LINES_AND_QUEUES, usernames_list, p_location)

    # Fetch Workforce Consumption if burn=True
    workforce_needs_rows = []
    if burn:
        SQL_GET_WORKFORCE_CONSUMPTION = """
        WITH AllowedSites AS (
            -- 1. Sites owned by the users
            SELECT 
                u.username,
                s.siteid,
                s.addressplanetid,
                FALSE as is_leased,
                'owned'::text as lease_type,
                NULL::text as leased_to,
                NULL::text as leased_from
            FROM sites s
            JOIN users u ON u.userdataid = s.userid
            WHERE u.username = ANY($1::text[])

            UNION DISTINCT

            -- 2. Sites leased by the users as tenants (configured in landlord's settings)
            SELECT 
                u_tenant.username,
                l->>'siteId' as siteid,
                s.addressplanetid,
                TRUE as is_leased,
                'Inbound'::text as lease_type,
                NULL::text as leased_to,
                (SELECT COALESCE(ud2.displayname, cd2.companyname, 'Unknown') 
                 FROM users u2 
                 LEFT JOIN users_data ud2 ON ud2.userid = u2.userdataid 
                 LEFT JOIN company_data cd2 ON cd2.userdataid = u2.userdataid 
                 WHERE u2.accountid::text = ugs.userid) as leased_from
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
            JOIN sites s ON s.siteid = l->>'siteId'
            JOIN users u_tenant ON (
                l->>'tenant' = u_tenant.username
                OR l->>'tenant' = (SELECT companycode FROM company_data WHERE userdataid = u_tenant.userdataid)
                OR l->>'tenant' = u_tenant.username || ' (' || (SELECT companycode FROM company_data WHERE userdataid = u_tenant.userdataid) || ')'
            )
            WHERE u_tenant.username = ANY($1::text[])
        )
        SELECT 
            ast.username,
            ast.siteid,
            p.naturalid as planetnaturalid,
            p.name as planetname,
            ast.is_leased,
            ast.lease_type,
            ast.leased_to,
            ast.leased_from,
            m.ticker,
            SUM(wn.unitsperinterval) as workforce_consumption
        FROM workforces w
        JOIN workforce_needs wn ON wn.workforceid = w.workforceid
        JOIN AllowedSites ast ON ast.siteid = w.siteid
        LEFT JOIN planets p ON p.planetid = ast.addressplanetid
        JOIN materials m ON m.materialid = wn.materialid
        WHERE ($2::text IS NULL OR (
              p.name ILIKE $2 OR 
              p.naturalid ILIKE $2
          ))
        GROUP BY ast.username, ast.siteid, p.naturalid, p.name, ast.is_leased, ast.lease_type, ast.leased_to, ast.leased_from, m.ticker;
        """
        workforce_needs_rows = await conn.fetch(SQL_GET_WORKFORCE_CONSUMPTION, usernames_list, p_location)

    if not rows and not workforce_needs_rows:
        return "[]"

    raw_lines = []
    unique_targets = set()

    # 2. Extract unique line/recipe pairs
    for row in rows:
        pl = dict(row)
        pl["orders"] = json.loads(pl["production_orders"]) if isinstance(pl["production_orders"], str) else pl["production_orders"]
        pl["is_leased"] = row["is_leased"]
        pl["lease_type"] = row["lease_type"]
        pl["leased_to"] = row["leased_to"]
        pl["leased_from"] = row["leased_from"]
        raw_lines.append(pl)

        for order in pl["orders"]:
            if order.get("RecipeId"):
                unique_targets.add((order["RecipeId"], pl["productionlineid"]))

    t_ids = [t[0] for t in unique_targets]
    l_ids = [t[1] for t in unique_targets]

    # 3. Fetch Recipe Details Sequentially
    recipe_map = {}
    if t_ids and l_ids:
        core_rows = await conn.fetch(SQL_FETCH_RECIPES_CORE, t_ids, l_ids)
        input_rows = await conn.fetch(SQL_FETCH_RECIPE_INPUTS, t_ids, l_ids)
        output_rows = await conn.fetch(SQL_FETCH_RECIPE_OUTPUTS, t_ids, l_ids)

        # Build Composite Key Map (LineID, RecipeID)
        for r in core_rows:
            key = (r["line_id"], r["recipe_id"])
            recipe_map[key] = {
                "DurationMs": float(r["duration"] or 0),
                "Name": r["name"],
                "Inputs": [],
                "Outputs": []
            }

        for i in input_rows:
            key = (i["line_id"], i["recipe_id"])
            if key in recipe_map:
                recipe_map[key]["Inputs"].append({
                    "MaterialTicker": i["ticker"],
                    "MaterialAmount": float(i["factor"]),
                })

        for o in output_rows:
            key = (o["line_id"], o["recipe_id"])
            if key in recipe_map:
                recipe_map[key]["Outputs"].append({
                    "MaterialTicker": o["ticker"],
                    "MaterialAmount": float(o["factor"]),
                })

    # 4. Stitch & Group by User
    grouped_data = {}

    if burn:
        for row in workforce_needs_rows:
            username = row["username"]
            planet_id = row["planetname"] or row["planetnaturalid"]
            site_id = row["siteid"]
            is_leased = row["is_leased"]
            lease_type = row["lease_type"]
            leased_to = row["leased_to"]
            leased_from = row["leased_from"]
            ticker = row["ticker"]
            cons = float(row["workforce_consumption"] or 0)
            if cons > 0:
                if username not in grouped_data:
                    grouped_data[username] = {
                        "BurnData": defaultdict(lambda: defaultdict(lambda: {"production": 0.0, "consumption": 0.0})),
                        "Lines": []
                    }
                key = (planet_id, site_id, is_leased, lease_type, leased_to, leased_from)
                grouped_data[username]["BurnData"][key][ticker]["consumption"] += cons

    for pl in raw_lines:
        username = pl["username"]
        line_id = pl["productionlineid"]

        if username not in grouped_data:
            grouped_data[username] = {
                "BurnData": defaultdict(lambda: defaultdict(lambda: {"production": 0.0, "consumption": 0.0})),
                "Lines": []
            }

        active_orders = [o for o in pl.get("orders", []) if o.get("Completed") is False]
        processed_orders = []

        # --- BURN CALCULATION LOGIC ---
        if burn:
            total_ms = sum((float(o.get("DurationMs") or 0)) for o in active_orders)

            if total_ms > 0:
                daily_cycles = (pl.get("capacity", 0) * MS_PER_DAY) / total_ms
                planet_id = pl.get("planetname") or pl.get("planetnaturalid")
                site_id = pl["siteid"]
                is_leased = pl.get("is_leased", False)
                lease_type = pl.get("lease_type", "owned")
                leased_to = pl.get("leased_to")
                leased_from = pl.get("leased_from")
                key = (planet_id, site_id, is_leased, lease_type, leased_to, leased_from)

                for order in active_orders:
                    r_id = order.get("RecipeId")
                    recipe_data = recipe_map.get((line_id, r_id))

                    if not recipe_data or recipe_data["DurationMs"] == 0:
                        continue

                    order_duration = float(order.get("DurationMs") or 0)
                    duration_multiplier = order_duration / recipe_data["DurationMs"]

                    for inp in recipe_data["Inputs"]:
                        factor = inp["MaterialAmount"] * duration_multiplier
                        grouped_data[username]["BurnData"][key][inp["MaterialTicker"]]["consumption"] += (factor * daily_cycles)

                    for out in recipe_data["Outputs"]:
                        factor = out["MaterialAmount"] * duration_multiplier
                        grouped_data[username]["BurnData"][key][out["MaterialTicker"]]["production"] += (factor * daily_cycles)

        # --- STANDARD DATA LOGIC ---
        else:
            now_utc = datetime.now(timezone.utc)

            for order in active_orders:
                r_id = order.get("RecipeId")
                recipe_data = recipe_map.get((line_id, r_id), {"Inputs": [], "Outputs": []})

                inputs = [
                    {**i, "ProductionLineInputId": f"{order['OrderId']}-{i['MaterialTicker']}"}
                    for i in recipe_data["Inputs"]
                ]

                outputs = [
                    {**o, "ProductionLineOutputId": f"{order['OrderId']}-{o['MaterialTicker']}"}
                    for o in recipe_data["Outputs"]
                ]

                # Calculate Completed Percentage
                completed_pct = None
                started_str = order.get("Started")
                completion_str = order.get("Completion")

                if started_str and completion_str:
                    try:
                        # 1. Parse the strings (handles both 'Z' trailing and missing timezone)
                        s_dt = datetime.fromisoformat(started_str.replace("Z", "+00:00"))
                        c_dt = datetime.fromisoformat(completion_str.replace("Z", "+00:00"))

                        # 2. Force naive datetimes to be UTC-aware
                        if s_dt.tzinfo is None:
                            s_dt = s_dt.replace(tzinfo=timezone.utc)
                        if c_dt.tzinfo is None:
                            c_dt = c_dt.replace(tzinfo=timezone.utc)

                        # 3. Calculate seconds
                        total_seconds = (c_dt - s_dt).total_seconds()
                        elapsed_seconds = (now_utc - s_dt).total_seconds()

                        if total_seconds <= 0:
                            completed_pct = 100.0
                        else:
                            # 4. Calculate percentage and clamp it
                            raw_pct = (elapsed_seconds / total_seconds) * 100
                            completed_pct = max(0.0, min(100.0, raw_pct))
                            completed_pct = round(completed_pct, 2)

                    except (ValueError, TypeError):
                        # Silent fail if data is genuinely corrupted
                        pass

                processed_orders.append({
                    **order,
                    "Inputs": inputs,
                    "Outputs": outputs,
                    "CompletedPercentage": completed_pct
                })

                if "RecipeId" in order:
                    del order["RecipeId"]

            grouped_data[username]["Lines"].append({
                "ProductionLineId": line_id,
                "SiteId": pl["siteid"],
                "PlanetId": pl["planetid"],
                "PlanetNaturalId": pl["planetnaturalid"],
                "PlanetName": pl["planetname"],
                "Type": pl["type"],
                "Capacity": pl["capacity"],
                "Efficiency": pl["efficiency"],
                "Condition": pl["condition"],
                "UserNameSubmitted": username,
                "Timestamp": pl["xata_updatedat"].isoformat() if pl["xata_updatedat"] else None,
                "IsLeased": pl.get("is_leased", False),
                "LeaseType": pl.get("lease_type", "owned"),
                "LeasedTo": pl.get("leased_to"),
                "LeasedFrom": pl.get("leased_from"),
                "Orders": processed_orders,
            })

    if burn and simple:
        simple_burn_data = defaultdict(lambda: defaultdict(float))

        for username_key, data in grouped_data.items():
            for key_tuple, tickers in data["BurnData"].items():
                planet_id = key_tuple[0]
                for ticker, flows in tickers.items():
                    cons = flows["consumption"]
                    if cons > 0:
                        simple_burn_data[planet_id][ticker] += round(cons, 2)

        final_simple_dict = {
            planet: dict(materials)
            for planet, materials in simple_burn_data.items()
        }

        return json.dumps(final_simple_dict)

    # 5. Transform to Final Output Structure
    final_output = []

    for u, data in grouped_data.items():
        if burn:
            formatted_burn = {}
            for key_tuple, tickers in data["BurnData"].items():
                planet_id = key_tuple[0]
                site_id = key_tuple[1]
                is_leased = key_tuple[2]
                lease_type = key_tuple[3]
                leased_to = key_tuple[4]
                leased_from = key_tuple[5]

                if planet_id not in formatted_burn:
                    formatted_burn[planet_id] = []

                for ticker, flows in tickers.items():
                    prod = flows["production"]
                    cons = flows["consumption"]
                    formatted_burn[planet_id].append({
                        "MaterialTicker": ticker,
                        "Production": round(prod, 2),
                        "Consumption": round(cons, 2),
                        "Net": round(prod - cons, 2),
                        "SiteId": site_id,
                        "IsLeased": is_leased,
                        "LeaseType": lease_type,
                        "LeasedTo": leased_to,
                        "LeasedFrom": leased_from
                    })

            final_output.append({"Username": u, "BurnRates": formatted_burn})
        else:
            final_output.append({"Username": u, "Production": data["Lines"]})

    return json.dumps(final_output)
