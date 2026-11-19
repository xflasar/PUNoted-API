import copy
import time
from collections import defaultdict
from typing import Any, Dict, List

from fastapi import HTTPException

from helpers.corp_production_calc import process_corp_production_and_workforce
from models.corp_production_models import (
    CorpOverviewResponse,
    ProducerConsumerItem,
    ProductionSummaryItem,
)
from repositories.corp_production_repo import (
    build_recipe_map,
    fetch_corp_flat_orders,
    fetch_corp_workforce,
    fetch_recipe_components,
)

# ==========================================
# 1. CORE BUILDER (returns a LIST)
# ==========================================


async def build_corp_production_response(conn, user_id: str, debug=False) -> List[CorpOverviewResponse]:
    t0 = time.perf_counter()

    # A. Identify the Family (Main + Subs)
    family_metadata = await get_corp_family_metadata(conn, user_id)

    if not family_metadata:
        raise HTTPException(status_code=404, detail="No corporation found for this user.")

    # B. Fetch ALL Members for these corporations
    family_ids = [c["id"] for c in family_metadata]
    members_map, proxies_map = await get_family_members(conn, family_ids)

    response_list = []

    # C. Loop through each Corporation in the family
    for corp in family_metadata:
        corp_id = corp["id"]
        corp_members = members_map.get(corp_id, [])
        proxy_id = proxies_map.get(corp_id)

        summary = []
        corp_balances = []

        if proxy_id:
            # --- 1. Fetch Flat Orders (For this specific Corp) ---
            flat_orders = await fetch_corp_flat_orders(conn, proxy_id)

            if flat_orders:
                pairs = {(str(row["recipeid"]), str(row["productionlineid"])) for row in flat_orders if row["recipeid"]}

                # --- 3. Fetch Components ---
                core_rows, input_rows, output_rows = await fetch_recipe_components(conn, pairs)

                # --- 4. Build Recipe Map ---
                recipe_map = build_recipe_map(core_rows, input_rows, output_rows)

                # --- 5. Reconstruct Lines ---
                sites_map = {}
                for row in flat_orders:
                    sid, lid = row["siteid"], str(row["productionlineid"])
                    if sid not in sites_map:
                        sites_map[sid] = {
                            "player_name": row["player_name"],
                            "location_name": row["location_name"],
                            "is_accurate": row["is_accurate"],
                            "lines": {},
                        }

                    if lid not in sites_map[sid]["lines"]:
                        sites_map[sid]["lines"][lid] = {
                            "capacity": row["capacity"],
                            "condition": row["condition"],
                            "production_orders": [],
                            "efficiency": row.get("efficiency", 1.0),
                        }

                    r_key = (str(row["recipeid"]), lid) if row.get("recipeid") else None
                    # Deep copy prevents in-place list mutation across orders and players
                    rec_obj = copy.deepcopy(recipe_map.get(r_key, {})) if r_key in recipe_map else {}
                    if rec_obj and row.get("building_ticker"):
                        rec_obj["building"] = row["building_ticker"]

                    sites_map[sid]["lines"][lid]["production_orders"].append(
                        {
                            "order_id": row["orderid"],
                            "recurring": row.get("recurring"),
                            "created": row["created"].isoformat() if row["created"] else None,
                            "completion": row["completion"].isoformat() if row["completion"] else None,
                            "duration": row["order_duration"],
                            "production_recipe": rec_obj,
                        }
                    )

                # --- 6. Prepare Calc Input ---
                prod_raw = []
                for sdata in sites_map.values():
                    lines_list = [l for l in sdata["lines"].values()]
                    prod_raw.append(
                        {
                            "player_name": sdata["player_name"],
                            "location_name": sdata["location_name"],
                            "is_accurate": sdata["is_accurate"],
                            "production_lines": lines_list,
                        }
                    )

                # --- 7. Workforce ---
                wf_raw = await fetch_corp_workforce(conn, proxy_id)

                # --- 8. Calculate Flow ---
                corp_flow = process_corp_production_and_workforce(prod_raw, wf_raw)

                # --- 9. Fetch Storage Quantities, Prices, and Balances for Corp Members ---
                member_user_ids = [
                    m["companyCode"] if isinstance(m, dict) else getattr(m, "companyCode", "")
                    for m in corp_members
                    if (m["companyCode"] if isinstance(m, dict) else getattr(m, "companyCode", ""))
                ]
                member_udids = await conn.fetch(
                    "SELECT userdataid FROM users WHERE accountid::text = ANY($1) OR userdataid = ANY($1);",
                    member_user_ids
                )
                ud_list = [r["userdataid"] for r in member_udids if r["userdataid"]]

                storage_map = {}
                s_rows = await conn.fetch(
                    """
                    SELECT m.ticker, SUM(si.quantity) as qty
                    FROM storage_items si
                    JOIN storages s ON si.storageid = s.storageid
                    JOIN materials m ON si.materialid = m.materialid
                    JOIN corporation_shareholders cs ON s.userid = cs.userid
                    WHERE cs.corporationid = $1
                    GROUP BY m.ticker;
                    """,
                    corp_id,
                )
                storage_map = {r["ticker"]: float(r["qty"] or 0) for r in s_rows}

                cx_prices_rows = await conn.fetch(
                    "SELECT ticker, price, askprice, bidprice FROM cx_brokers WHERE price > 0 OR askprice > 0;"
                )
                price_map = {}
                for r in cx_prices_rows:
                    t = r["ticker"]
                    p = float(r["price"] or r["askprice"] or r["bidprice"] or 0)
                    if t not in price_map or p > 0:
                        price_map[t] = p

                corp_balances = []
                if ud_list:
                    b_rows = await conn.fetch(
                        "SELECT balancecurrencycode, SUM(balanceamount) as total_bal FROM user_currency_accounts WHERE userdataid = ANY($1) GROUP BY balancecurrencycode;",
                        ud_list
                    )
                    corp_balances = [{"currency": r["balancecurrencycode"], "amount": float(r["total_bal"])} for r in b_rows]

                # --- 10. Format Summary ---
                tot_corp_prod = sum(info["prod_total"] for info in corp_flow.values())
                all_corp_tickers = set(corp_flow.keys()) | set(storage_map.keys())

                for ticker in all_corp_tickers:
                    info = corp_flow.get(ticker, {
                        "producers": {},
                        "consumers": {},
                        "prod_total": 0.0,
                        "prod_acc": 0.0,
                        "prod_est": 0.0,
                        "cons_total": 0.0,
                        "cons_acc": 0.0,
                        "cons_est": 0.0,
                        "user_recipes_used": {},
                        "user_recipe_inputs": {},
                    })
                    producers = [
                        ProducerConsumerItem(
                            loc=l,
                            player=p,
                            amount=round(v["acc"] + v["est"], 2),
                            isAccurate=(v["est"] == 0),
                            condition=0.0,
                            batchProdActive=round(v.get("batch_active", 0.0), 2),
                            batchProdQueued=round(v.get("batch_queued", 0.0), 2),
                            batchConsActive=0.0,
                            batchConsQueued=0.0,
                        )
                        for (l, p), v in info["producers"].items()
                    ]
                    consumers = [
                        ProducerConsumerItem(
                            loc=l,
                            player=p,
                            amount=round(v["acc"] + v["est"], 2),
                            isAccurate=(v["est"] == 0),
                            condition=0.0,
                            batchProdActive=0.0,
                            batchProdQueued=0.0,
                            batchConsActive=round(v.get("batch_active", 0.0), 2),
                            batchConsQueued=round(v.get("batch_queued", 0.0), 2),
                        )
                        for (l, p), v in info["consumers"].items()
                    ]

                    prod_tot = round(info["prod_total"], 2)
                    cons_tot = round(info["cons_total"], 2)
                    share_pct = round((prod_tot / tot_corp_prod * 100), 1) if tot_corp_prod > 0 else 0.0

                    raw_user_recipes = info.get("user_recipes_used")
                    formatted_recipes = []
                    if raw_user_recipes:
                        for r_key, r_data in raw_user_recipes.items():
                            user_list = []
                            if "users" in r_data:
                                for (p_name, loc_name), u_metrics in r_data["users"].items():
                                    user_list.append({
                                        "player": p_name,
                                        "loc": loc_name,
                                        "dailyOutput": round(u_metrics["daily_output"], 2),
                                        "dailyCycles": round(u_metrics["daily_cycles"], 2),
                                    })
                            formatted_recipes.append({
                                "recipeKey": r_key,
                                "building": r_data.get("building", ""),
                                "dailyOutput": round(r_data.get("daily_output", 0.0), 2),
                                "dailyCycles": round(r_data.get("daily_cycles", 0.0), 2),
                                "outputAmount": r_data.get("output_amount", 1.0),
                                "inputs": {k: round(v, 2) for k, v in r_data.get("inputs", {}).items()},
                                "outputs": {k: round(v, 2) for k, v in r_data.get("outputs", {}).items()},
                                "users": user_list,
                            })

                    summary.append(
                        ProductionSummaryItem(
                            ticker=ticker,
                            productionTotal=prod_tot,
                            productionAccurate=round(info["prod_acc"], 2),
                            productionEstimated=round(info["prod_est"], 2),
                            consumptionTotal=cons_tot,
                            consumptionAccurate=round(info["cons_acc"], 2),
                            consumptionEstimated=round(info["cons_est"], 2),
                            net=round(prod_tot - cons_tot, 2),
                            storageQty=round(storage_map.get(ticker, 0.0), 2),
                            price=round(price_map.get(ticker, 0.0), 2),
                            marketSharePct=share_pct,
                            batchProdActive=round(info.get("batch_prod_active", 0.0), 2),
                            batchProdQueued=round(info.get("batch_prod_queued", 0.0), 2),
                            batchConsActive=round(info.get("batch_cons_active", 0.0), 2),
                            batchConsQueued=round(info.get("batch_cons_queued", 0.0), 2),
                            producers=producers,
                            consumers=consumers,
                            userRecipeInputs={k: round(v, 2) for k, v in info.get("user_recipe_inputs", {}).items()} if info.get("user_recipe_inputs") else None,
                            userRecipesUsed=formatted_recipes if formatted_recipes else None,
                        )
                    )

                summary.sort(key=lambda x: abs(x.net), reverse=True)

        response_list.append(
            CorpOverviewResponse(
                name=corp["name"],
                code=corp["code"],
                memberCount=corp["member_count"],
                headquarters=" - ",
                productionSummary=summary,
                productionCount=len(summary),
                consumptionCount=len(summary),
                members=corp_members,
                balances=corp_balances,
            )
        )

    return response_list


# ==========================================
# 1.5. FLAT BUILDER
# ==========================================

async def build_corp_production_flat_response(conn, user_id: str) -> List[Dict[str, Any]]:
    """
    Executes the exact same data fetch and calculation pipeline as the core builder, 
    but formats the output into a flat array optimized for CSV export or tabular frontend display.
    """
    family_metadata = await get_corp_family_metadata(conn, user_id)
    if not family_metadata:
        raise HTTPException(status_code=404, detail="No corporation found for this user.")

    family_ids = [c["id"] for c in family_metadata]
    _, proxies_map = await get_family_members(conn, family_ids)

    flat_results = []

    for corp in family_metadata:
        corp_id = corp["id"]
        corp_code = corp["code"]
        proxy_id = proxies_map.get(corp_id)

        if proxy_id:
            flat_orders = await fetch_corp_flat_orders(conn, proxy_id)

            if flat_orders:
                pairs = {(str(row["recipeid"]), str(row["productionlineid"])) for row in flat_orders if row["recipeid"]}

                core_rows, input_rows, output_rows = await fetch_recipe_components(conn, pairs)

                recipe_map = build_recipe_map(core_rows, input_rows, output_rows)

                sites_map = {}
                for row in flat_orders:
                    sid, lid = row["siteid"], str(row["productionlineid"])
                    if sid not in sites_map:
                        sites_map[sid] = {
                            "player_name": row["player_name"],
                            "location_name": row["location_name"],
                            "is_accurate": row["is_accurate"],
                            "lines": {},
                        }
                    if lid not in sites_map[sid]["lines"]:
                        sites_map[sid]["lines"][lid] = {
                            "capacity": row["capacity"],
                            "condition": row["condition"],
                            "production_orders": [],
                            "efficiency": row.get("efficiency", 1.0),
                        }
                    r_key = (str(row["recipeid"]), lid) if row.get("recipeid") else None
                    rec_obj = copy.deepcopy(recipe_map.get(r_key, {})) if r_key in recipe_map else {}
                    if rec_obj and row.get("building_ticker"):
                        rec_obj["building"] = row["building_ticker"]

                    sites_map[sid]["lines"][lid]["production_orders"].append({
                        "order_id": row["orderid"],
                        "recurring": row.get("recurring"),
                        "created": row["created"].isoformat() if row["created"] else None,
                        "completion": row["completion"].isoformat() if row["completion"] else None,
                        "duration": row["order_duration"],
                        "production_recipe": rec_obj,
                    })

                prod_raw = []
                for sdata in sites_map.values():
                    lines_list = [l for l in sdata["lines"].values()]
                    prod_raw.append({
                        "player_name": sdata["player_name"],
                        "location_name": sdata["location_name"],
                        "is_accurate": sdata["is_accurate"],
                        "production_lines": lines_list,
                    })

                wf_raw = await fetch_corp_workforce(conn, proxy_id)
                corp_flow = process_corp_production_and_workforce(prod_raw, wf_raw)

                for ticker, info in corp_flow.items():
                    loc_player_map = defaultdict(lambda: {"production": 0.0, "consumption": 0.0})

                    for (loc, player), v in info["producers"].items():
                        loc_player_map[(loc, player)]["production"] += (v["acc"] + v["est"])

                    for (loc, player), v in info["consumers"].items():
                        loc_player_map[(loc, player)]["consumption"] += (v["acc"] + v["est"])

                    for (loc, player), flows in loc_player_map.items():
                        flat_results.append({
                            "CorpCode": corp_code,
                            "CompanyName": player,
                            "PlanetName": loc,
                            "MaterialTicker": ticker,
                            "Production": round(flows["production"], 2),
                            "Consumption": round(flows["consumption"], 2)
                        })

    return flat_results


# ==========================================
# 2. HELPER: GET FAMILY METADATA
# ==========================================

async def get_corp_family_metadata(conn: Any, user_id: str) -> List[Dict[str, Any]]:
    """
    Finds the 'Family' of corporations linked to the user.
    1. Finds User's Current Corp.
    2. Checks if it is a Sub or Main via corporation_subsidiaries.
    3. Returns list of metadata for Main + All Subs.
    """
    FAMILY_QUERY = """
    WITH UserCorp AS (
        SELECT COALESCE(ud.corporationid, cs.corporationid) as id
        FROM users u 
        JOIN users_data ud ON u.userdataid = ud.userid 
        LEFT JOIN corporation_shareholders cs ON ud.userid = cs.userid 
        WHERE u.accountid = $1 
          AND (ud.corporationid IS NOT NULL OR cs.corporationid IS NOT NULL)
        ORDER BY (ud.corporationid IS NOT NULL) DESC
        LIMIT 1
    ),
    MainID AS (
        SELECT COALESCE(
            (SELECT corporationmainid FROM corporation_subsidiaries WHERE corporationsubid = (SELECT id FROM UserCorp)),
            (SELECT id FROM UserCorp)
        ) as id
    ),
    FamilyIDs AS (
        SELECT id FROM MainID
        UNION
        SELECT corporationsubid FROM corporation_subsidiaries WHERE corporationmainid = (SELECT id FROM MainID)
    )
    SELECT 
        c.id, c.name, c.code,
        (SELECT COUNT(DISTINCT companycode) FROM corporation_shareholders WHERE corporationid = c.id) as member_count
    FROM corporations c
    WHERE c.id IN (SELECT id FROM FamilyIDs)
    ORDER BY c.code
    """

    rows = await conn.fetch(FAMILY_QUERY, user_id)
    return [dict(r) for r in rows]


# ==========================================
# 3. HELPER: GET FAMILY MEMBERS
# ==========================================

async def get_family_members(conn: Any, corp_ids: List[str]):
    """
    Fetches members for all provided corporation IDs.
    Returns:
      1. map: corp_id -> List[MemberDict]
      2. map: corp_id -> proxy_user_account_id (Best candidate to fetch data)
    """
    MEMBERS_QUERY = """
    SELECT 
        cs.corporationid,
        COALESCE(cs.companycode, '') AS companycode,
        COALESCE(cs.companyname, '') AS companyname,
        CASE WHEN ud.userid IS NOT NULL THEN TRUE ELSE FALSE END AS is_synchronized,
        u.xata_updatedat AS last_active,
        ud.xata_createdat AS joineddate,
        u.accountid
    FROM corporation_shareholders cs
    LEFT JOIN users_data ud ON cs.userid = ud.userid
    LEFT JOIN users u ON ud.userid = u.userdataid
    WHERE cs.corporationid = ANY($1::text[])
    ORDER BY cs.companyname;
    """

    records = await conn.fetch(MEMBERS_QUERY, corp_ids)

    members_map = defaultdict(list)
    proxies_map = {}

    for r in records:
        cid = r["corporationid"]

        member_obj = {
            "companyCode": r["companycode"],
            "companyName": r["companyname"],
            "isSynchronized": r["is_synchronized"],
            "lastActive": r["last_active"].isoformat() if r["last_active"] else None,
            "joinedDate": r["joineddate"].isoformat() if r["joineddate"] else None,
        }
        members_map[cid].append(member_obj)

        if r["accountid"] and cid not in proxies_map:
            proxies_map[cid] = r["accountid"]

    return members_map, proxies_map


# ==========================================
# 4. HELPER: GET 7-DAY FUEL CONSUMPTION
# ==========================================

async def get_corp_fuel_consumption(conn: Any, user_id: str, days: int = 7) -> Dict[str, Any]:
    """
    Calculates total and daily rolling fuel consumption (STL and FTL) for the user's
    entire corporation family (main + subsidiaries) over the requested number of days.
    Also prices it using material_prices (Corp price) and cx_brokers (CX IC1 price).
    """
    FUEL_QUERY = """
    WITH UserCorp AS (
        SELECT COALESCE(ud.corporationid, cs.corporationid) AS id
        FROM users u 
        JOIN users_data ud ON u.userdataid = ud.userid 
        LEFT JOIN corporation_shareholders cs ON ud.userid = cs.userid 
        WHERE u.accountid = $1 
          AND (ud.corporationid IS NOT NULL OR cs.corporationid IS NOT NULL)
        ORDER BY (ud.corporationid IS NOT NULL) DESC
        LIMIT 1
    ),
    MainCorp AS (
        SELECT COALESCE(
            (SELECT corporationmainid FROM corporation_subsidiaries WHERE corporationsubid = (SELECT id FROM UserCorp)),
            (SELECT id FROM UserCorp)
        ) AS id
    ),
    FamilyCorpIDs AS (
        SELECT id FROM MainCorp
        UNION
        SELECT corporationsubid FROM corporation_subsidiaries WHERE corporationmainid = (SELECT id FROM MainCorp)
    ),
    FamilyUserIDs AS (
        SELECT DISTINCT cs.userid
        FROM corporation_shareholders cs
        WHERE cs.corporationid IN (SELECT id FROM FamilyCorpIDs)
          AND cs.userid IS NOT NULL
    ),
    Consumption AS (
        SELECT 
            COALESCE(SUM(sf.stltotalconsumption), 0) AS total_stl,
            COALESCE(SUM(sf.ftltotalconsumption), 0) AS total_ftl,
            COUNT(sf.id) AS total_flights
        FROM ship_flights sf
        WHERE sf.userid IN (SELECT userid FROM FamilyUserIDs)
          AND sf.departuretimestamp >= NOW() - ($2 || ' days')::INTERVAL
    ),
    CorpPrices AS (
        SELECT
            MAX(CASE WHEN ticker = 'SF' THEN price END) AS corp_sf_price,
            MAX(CASE WHEN ticker = 'FF' THEN price END) AS corp_ff_price
        FROM material_prices
        WHERE ticker IN ('SF', 'FF')
    ),
    CxPrices AS (
        SELECT
            MAX(CASE WHEN ticker = 'SF.IC1' THEN price END) AS cx_sf_price,
            MAX(CASE WHEN ticker = 'FF.IC1' THEN price END) AS cx_ff_price
        FROM cx_brokers
        WHERE ticker IN ('SF.IC1', 'FF.IC1')
    )
    SELECT 
        c.total_flights,
        c.total_stl,
        c.total_ftl,
        COALESCE(cp.corp_sf_price, 0) AS corp_sf_price,
        COALESCE(cp.corp_ff_price, 0) AS corp_ff_price,
        COALESCE(cx.cx_sf_price, 0) AS cx_sf_price,
        COALESCE(cx.cx_ff_price, 0) AS cx_ff_price
    FROM Consumption c
    CROSS JOIN CorpPrices cp
    CROSS JOIN CxPrices cx;
    """

    row = await conn.fetchrow(FUEL_QUERY, user_id, str(days))
    if not row:
        return {
            "days": days,
            "total_flights": 0,
            "total_stl": 0,
            "total_ftl": 0,
            "corp_pricing": {"sf_price": 0, "ff_price": 0, "stl_cost": 0, "ftl_cost": 0, "total_cost": 0},
            "cx_pricing": {"exchange": "IC1", "sf_price": 0, "ff_price": 0, "stl_cost": 0, "ftl_cost": 0, "total_cost": 0},
            "savings_vs_cx": 0,
            "daily_average": {"stl": 0, "ftl": 0, "corp_cost": 0, "cx_cost": 0},
        }

    total_flights = int(row["total_flights"] or 0)
    total_stl = int(row["total_stl"] or 0)
    total_ftl = int(row["total_ftl"] or 0)
    corp_sf = float(row["corp_sf_price"] or 0)
    corp_ff = float(row["corp_ff_price"] or 0)
    cx_sf = float(row["cx_sf_price"] or 0)
    cx_ff = float(row["cx_ff_price"] or 0)

    corp_stl_cost = round(total_stl * corp_sf, 2)
    corp_ftl_cost = round(total_ftl * corp_ff, 2)
    corp_total = round(corp_stl_cost + corp_ftl_cost, 2)

    cx_stl_cost = round(total_stl * cx_sf, 2)
    cx_ftl_cost = round(total_ftl * cx_ff, 2)
    cx_total = round(cx_stl_cost + cx_ftl_cost, 2)

    savings = round(cx_total - corp_total, 2)
    div_days = max(days, 1)

    return {
        "days": days,
        "total_flights": total_flights,
        "total_stl": total_stl,
        "total_ftl": total_ftl,
        "corp_pricing": {
            "sf_price": corp_sf,
            "ff_price": corp_ff,
            "stl_cost": corp_stl_cost,
            "ftl_cost": corp_ftl_cost,
            "total_cost": corp_total,
        },
        "cx_pricing": {
            "exchange": "IC1",
            "sf_price": cx_sf,
            "ff_price": cx_ff,
            "stl_cost": cx_stl_cost,
            "ftl_cost": cx_ftl_cost,
            "total_cost": cx_total,
        },
        "savings_vs_cx": savings,
        "daily_average": {
            "stl": round(total_stl / div_days, 1),
            "ftl": round(total_ftl / div_days, 1),
            "corp_cost": round(corp_total / div_days, 2),
            "cx_cost": round(cx_total / div_days, 2),
        },
    }


# ==========================================
# 5. HELPER: GET MATERIAL TRADE COMPARISON (CORP CONTRACTS VS CX)
# ==========================================

async def get_corp_trade_summary(conn: Any, user_id: str, days: int = 7) -> Dict[str, Any]:
    """
    Returns aggregated material transactions for the corporation family over the specified days:
    - Internal trade between corp members via contracts (quantity, contract count).
    - External trade on CX by corp members (bought quantity/cost/avg price, sold quantity/revenue/avg price).
    """
    days = max(1, min(days, 365))

    FAMILY_QUERY = """
    WITH UserCorp AS (
        SELECT COALESCE(ud.corporationid, cs.corporationid) AS id
        FROM users u 
        JOIN users_data ud ON u.userdataid = ud.userid 
        LEFT JOIN corporation_shareholders cs ON ud.userid = cs.userid 
        WHERE u.accountid = $1 
          AND (ud.corporationid IS NOT NULL OR cs.corporationid IS NOT NULL)
        ORDER BY (ud.corporationid IS NOT NULL) DESC
        LIMIT 1
    ),
    MainCorp AS (
        SELECT COALESCE(
            (SELECT corporationmainid FROM corporation_subsidiaries WHERE corporationsubid = (SELECT id FROM UserCorp)),
            (SELECT id FROM UserCorp)
        ) AS id
    ),
    FamilyCorpIDs AS (
        SELECT id FROM MainCorp
        UNION
        SELECT corporationsubid FROM corporation_subsidiaries WHERE corporationmainid = (SELECT id FROM MainCorp)
    )
    SELECT DISTINCT cs.userid, cs.companycode
    FROM corporation_shareholders cs
    WHERE cs.corporationid IN (SELECT id FROM FamilyCorpIDs)
      AND cs.userid IS NOT NULL;
    """
    member_rows = await conn.fetch(FAMILY_QUERY, user_id)
    if not member_rows:
        return {"days": days, "trades": {}}

    member_userids = [r["userid"] for r in member_rows if r["userid"]]
    member_codes = [r["companycode"] for r in member_rows if r["companycode"]]

    CX_QUERY = """
    SELECT 
        m.ticker,
        o.type AS order_type,
        SUM(t.amount) AS total_amount,
        SUM(t.amount * o.limitamount) AS total_value,
        COUNT(DISTINCT o.orderid) AS order_count
    FROM comex_trade_orders o
    INNER JOIN comex_trade_orders_trades t ON o.orderid = t.orderId
    JOIN materials m ON o.materialid = m.materialid
    WHERE o.userid = ANY($1::text[])
      AND o.created >= NOW() - ($2 || ' days')::INTERVAL
    GROUP BY m.ticker, o.type;
    """
    cx_rows = await conn.fetch(CX_QUERY, member_userids, str(days))

    CONTRACTS_QUERY = """
    SELECT 
        mat.ticker,
        SUM(cm.amount) AS total_amount,
        COUNT(DISTINCT c.id) AS contract_count
    FROM contracts c
    JOIN contract_conditions cc ON c.id = cc.contractid AND c.party = cc.contractparty
    JOIN contract_materials cm ON cc.id = cm.contractconditionid AND cc.contractparty = cm.contractparty
    JOIN materials mat ON cm.materialid = mat.materialid
    WHERE c.userid = ANY($1::text[])
      AND (c.partnercode = ANY($2::text[]) OR c.partnerid = ANY($1::text[]))
      AND c.party = 'PROVIDER'
      AND c.status IN ('FULFILLED', 'CLOSED', 'OPEN')
      AND c.date >= NOW() - ($3 || ' days')::INTERVAL
    GROUP BY mat.ticker;
    """
    contracts_rows = await conn.fetch(CONTRACTS_QUERY, member_userids, member_codes, str(days))

    trades: Dict[str, Any] = {}

    for r in contracts_rows:
        ticker = r["ticker"]
        amt = float(r["total_amount"] or 0)
        cnt = int(r["contract_count"] or 0)
        if ticker not in trades:
            trades[ticker] = {
                "corp_contracts": {"amount": amt, "count": cnt},
                "cx": {
                    "bought_amount": 0.0,
                    "bought_value": 0.0,
                    "bought_avg_price": 0.0,
                    "sold_amount": 0.0,
                    "sold_value": 0.0,
                    "sold_avg_price": 0.0,
                },
            }
        else:
            trades[ticker]["corp_contracts"] = {"amount": amt, "count": cnt}

    for r in cx_rows:
        ticker = r["ticker"]
        order_type = r["order_type"]
        amt = float(r["total_amount"] or 0)
        val = float(r["total_value"] or 0)
        avg_price = round(val / amt, 2) if amt > 0 else 0.0

        if ticker not in trades:
            trades[ticker] = {
                "corp_contracts": {"amount": 0.0, "count": 0},
                "cx": {
                    "bought_amount": 0.0,
                    "bought_value": 0.0,
                    "bought_avg_price": 0.0,
                    "sold_amount": 0.0,
                    "sold_value": 0.0,
                    "sold_avg_price": 0.0,
                },
            }

        if order_type == "BUYING":
            trades[ticker]["cx"]["bought_amount"] = amt
            trades[ticker]["cx"]["bought_value"] = val
            trades[ticker]["cx"]["bought_avg_price"] = avg_price
        elif order_type == "SELLING":
            trades[ticker]["cx"]["sold_amount"] = amt
            trades[ticker]["cx"]["sold_value"] = val
            trades[ticker]["cx"]["sold_avg_price"] = avg_price

    return {"days": days, "trades": trades}
