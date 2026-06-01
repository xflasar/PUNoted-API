import json
import logging
from collections import defaultdict
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

MS_PER_DAY = 86400000

# --- QUERY 1: LINES & ORDERS ---
SQL_GET_LINES_AND_QUEUES = """
SELECT 
    u.username,
    s.addressplanetid as planetid,
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
INNER JOIN sites s ON s.siteid = pl.siteid
INNER JOIN users u ON u.userdataid = s.userid
LEFT JOIN planets p ON p.planetid = s.addressplanetid
LEFT JOIN systems sys ON sys.systemid = s.addresssystemid
WHERE u.username = ANY($1::text[])
  AND ($2::text IS NULL OR (
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
    if not rows:
        return "[]"

    raw_lines = []
    unique_targets = set()

    # 2. Extract unique line/recipe pairs
    for row in rows:
        pl = dict(row)
        pl["orders"] = json.loads(pl["production_orders"])
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
                planet_id = pl.get("planetname", "planetnaturalid")

                for order in active_orders:
                    r_id = order.get("RecipeId")
                    recipe_data = recipe_map.get((line_id, r_id))

                    if not recipe_data or recipe_data["DurationMs"] == 0:
                        continue

                    order_duration = float(order.get("DurationMs") or 0)
                    duration_multiplier = order_duration / recipe_data["DurationMs"]

                    for inp in recipe_data["Inputs"]:
                        factor = inp["MaterialAmount"] * duration_multiplier
                        grouped_data[username]["BurnData"][planet_id][inp["MaterialTicker"]]["consumption"] += (factor * daily_cycles)

                    for out in recipe_data["Outputs"]:
                        factor = out["MaterialAmount"] * duration_multiplier
                        grouped_data[username]["BurnData"][planet_id][out["MaterialTicker"]]["production"] += (factor * daily_cycles)

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
                "Orders": processed_orders,
            })

    if burn and simple:
        simple_burn_data = defaultdict(lambda: defaultdict(float))

        for username_key, data in grouped_data.items():
            for planet_id, tickers in data["BurnData"].items():
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
            for planet_id, tickers in data["BurnData"].items():
                formatted_burn[planet_id] = []
                for ticker, flows in tickers.items():
                    prod = flows["production"]
                    cons = flows["consumption"]
                    formatted_burn[planet_id].append({
                        "MaterialTicker": ticker,
                        "Production": round(prod, 2),
                        "Consumption": round(cons, 2),
                        "Net": round(prod - cons, 2)
                    })

            final_output.append({"Username": u, "BurnRates": formatted_burn})
        else:
            final_output.append({"Username": u, "Production": data["Lines"]})

    return json.dumps(final_output)
