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
                template_ids = list(set(str(row["recipeid"]) for row in flat_orders if row["recipeid"]))

                # --- 3. Fetch Components ---
                core_rows, input_rows, output_rows = await fetch_recipe_components(conn, template_ids)

                # --- 4. Build Recipe Map ---
                recipe_map = {}
                for r in core_rows:
                    recipe_map[r["recipe_id"]] = {
                        "duration": r["duration"],
                        "inputs": [],
                        "outputs": [],
                    }
                for i in input_rows:
                    k = i["recipe_id"]
                    if k in recipe_map:
                        recipe_map[k]["inputs"].append({"ticker": i["ticker"], "factor": i["factor"]})
                for o in output_rows:
                    k = o["recipe_id"]
                    if k in recipe_map:
                        recipe_map[k]["outputs"].append({"ticker": o["ticker"], "factor": o["factor"]})

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

                    r_key = str(row["recipeid"]) if row.get("recipeid") else ""
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
                template_ids = list(set(str(row["recipeid"]) for row in flat_orders if row["recipeid"]))

                core_rows, input_rows, output_rows = await fetch_recipe_components(conn, template_ids)

                recipe_map = {}
                for r in core_rows:
                    recipe_map[r["recipe_id"]] = {
                        "duration": r["duration"], "inputs": [], "outputs": []
                    }
                for i in input_rows:
                    k = i["recipe_id"]
                    if k in recipe_map:
                        recipe_map[k]["inputs"].append({"ticker": i["ticker"], "factor": i["factor"]})
                for o in output_rows:
                    k = o["recipe_id"]
                    if k in recipe_map:
                        recipe_map[k]["outputs"].append({"ticker": o["ticker"], "factor": o["factor"]})

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
                    r_key = str(row["recipeid"]) if row.get("recipeid") else ""
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
        SELECT c.id 
        FROM users u 
        JOIN users_data ud ON u.userdataid = ud.userid 
        JOIN corporation_shareholders cs ON ud.userid = cs.userid 
        JOIN corporations c ON cs.corporationid = c.id 
        WHERE u.accountid = $1 LIMIT 1
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