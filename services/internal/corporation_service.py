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
    # Returns list of dicts: [{'id': ..., 'name': ..., 'code': ..., 'member_count': ...}]
    family_metadata = await get_corp_family_metadata(conn, user_id)

    if not family_metadata:
        raise HTTPException(status_code=404, detail="No corporation found for this user.")

    # B. Fetch ALL Members for these corporations
    # Returns Dict[corp_id, List[Member]] and Dict[corp_id, proxy_user_id]
    family_ids = [c["id"] for c in family_metadata]
    members_map, proxies_map = await get_family_members(conn, family_ids)

    response_list = []

    # C. Loop through each Corporation in the family
    for corp in family_metadata:
        corp_id = corp["id"]
        corp_members = members_map.get(corp_id, [])

        # We need a valid user_id (Proxy) within this specific corporation to fetch its production/workforce.
        # If the requesting user is in this corp, use them. Otherwise use any synchronized member.
        # If no synchronized members exist, we can't fetch production data.
        proxy_id = proxies_map.get(corp_id)

        # If the current user belongs to this specific corp, prefer their ID (safer for RLS if applicable)
        # (Logic handled in get_family_members to prioritize active users)

        summary = []

        if proxy_id:
            # --- 1. Fetch Flat Orders (For this specific Corp) ---
            flat_orders = await fetch_corp_flat_orders(conn, proxy_id)

            if flat_orders:
                # --- 2. Extract Recipes ---
                unique_pairs = set()
                for row in flat_orders:
                    if row["recipeid"]:
                        unique_pairs.add((str(row["recipeid"]), str(row["productionlineid"])))

                template_ids = [p[0] for p in unique_pairs]
                line_ids = [p[1] for p in unique_pairs]

                # --- 3. Fetch Components ---
                core_rows, input_rows, output_rows = await fetch_recipe_components(conn, template_ids, line_ids)

                # --- 4. Build Recipe Map ---
                recipe_map = {}
                for r in core_rows:
                    recipe_map[f"{r['recipe_id']}_{r['line_id']}"] = {
                        "duration": r["duration"],
                        "inputs": [],
                        "outputs": [],
                    }
                for i in input_rows:
                    k = f"{i['recipe_id']}_{i['line_id']}"
                    if k in recipe_map:
                        recipe_map[k]["inputs"].append({"ticker": i["ticker"], "factor": i["factor"]})
                for o in output_rows:
                    k = f"{o['recipe_id']}_{o['line_id']}"
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

                    r_key = f"{row['recipeid']}_{lid}"
                    sites_map[sid]["lines"][lid]["production_orders"].append(
                        {
                            "order_id": row["orderid"],
                            "created": row["created"].isoformat() if row["created"] else None,
                            "completion": row["completion"].isoformat() if row["completion"] else None,
                            "duration": row["order_duration"],
                            "production_recipe": recipe_map.get(r_key, {}),
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

                # --- 9. Format Summary ---
                for ticker, info in corp_flow.items():
                    producers = [
                        ProducerConsumerItem(
                            loc=l,
                            player=p,
                            amount=round(v["acc"] + v["est"], 2),
                            isAccurate=(v["est"] == 0),
                            condition=0.0,
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
                        )
                        for (l, p), v in info["consumers"].items()
                    ]

                    summary.append(
                        ProductionSummaryItem(
                            ticker=ticker,
                            productionTotal=round(info["prod_total"], 2),
                            productionAccurate=round(info["prod_acc"], 2),
                            productionEstimated=round(info["prod_est"], 2),
                            consumptionTotal=round(info["cons_total"], 2),
                            consumptionAccurate=round(info["cons_acc"], 2),
                            consumptionEstimated=round(info["cons_est"], 2),
                            net=round(info["prod_total"] - info["cons_total"], 2),
                            producers=producers,
                            consumers=consumers,
                        )
                    )

                summary.sort(key=lambda x: abs(x.net), reverse=True)

        # Append this Corporation's Data object
        response_list.append(
            CorpOverviewResponse(
                name=corp["name"],
                code=corp["code"],
                memberCount=corp["member_count"],
                headquarters=" - ",  # Fix later
                productionSummary=summary,
                productionCount=len(summary),
                consumptionCount=len(summary),
                members=corp_members,
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
                unique_pairs = set()
                for row in flat_orders:
                    if row["recipeid"]:
                        unique_pairs.add((str(row["recipeid"]), str(row["productionlineid"])))

                template_ids = [p[0] for p in unique_pairs]
                line_ids = [p[1] for p in unique_pairs]

                core_rows, input_rows, output_rows = await fetch_recipe_components(conn, template_ids, line_ids)

                recipe_map = {}
                for r in core_rows:
                    recipe_map[f"{r['recipe_id']}_{r['line_id']}"] = {
                        "duration": r["duration"], "inputs": [], "outputs": []
                    }
                for i in input_rows:
                    k = f"{i['recipe_id']}_{i['line_id']}"
                    if k in recipe_map:
                        recipe_map[k]["inputs"].append({"ticker": i["ticker"], "factor": i["factor"]})
                for o in output_rows:
                    k = f"{o['recipe_id']}_{o['line_id']}"
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
                    r_key = f"{row['recipeid']}_{lid}"
                    sites_map[sid]["lines"][lid]["production_orders"].append({
                        "order_id": row["orderid"],
                        "created": row["created"].isoformat() if row["created"] else None,
                        "completion": row["completion"].isoformat() if row["completion"] else None,
                        "duration": row["order_duration"],
                        "production_recipe": recipe_map.get(r_key, {}),
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

                # --- FLATTENING LOGIC ---
                for ticker, info in corp_flow.items():
                    # Group by (Location, Player) to unify production and consumption rows
                    loc_player_map = defaultdict(lambda: {"production": 0.0, "consumption": 0.0})

                    # Aggregate Producers
                    for (loc, player), v in info["producers"].items():
                        loc_player_map[(loc, player)]["production"] += (v["acc"] + v["est"])

                    # Aggregate Consumers (Includes Workforce Consumption based on process logic)
                    for (loc, player), v in info["consumers"].items():
                        loc_player_map[(loc, player)]["consumption"] += (v["acc"] + v["est"])

                    # Build final flat dictionaries
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
    -- Identify the MainID. 
    -- If UserCorp is a sub, get its main. If it's a main (or unlinked), use UserCorp.id
    MainID AS (
        SELECT COALESCE(
            (SELECT corporationmainid FROM corporation_subsidiaries WHERE corporationsubid = (SELECT id FROM UserCorp)),
            (SELECT id FROM UserCorp)
        ) as id
    ),
    -- Collect all IDs (Main + Subs)
    FamilyIDs AS (
        SELECT id FROM MainID
        UNION
        SELECT corporationsubid FROM corporation_subsidiaries WHERE corporationmainid = (SELECT id FROM MainID)
    )
    -- Fetch Metadata for all found IDs
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
        u.accountid -- We need this to proxy requests for subs
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

        # Build Member Object
        member_obj = {
            "companyCode": r["companycode"],
            "companyName": r["companyname"],
            "isSynchronized": r["is_synchronized"],
            "lastActive": r["last_active"].isoformat() if r["last_active"] else None,
            "joinedDate": r["joineddate"].isoformat() if r["joineddate"] else None,
        }
        members_map[cid].append(member_obj)

        # Determine Proxy: Prefer synchronized users. Overwrite previous if current is better.
        # Logic: If we don't have a proxy yet, and this user has an accountid, take it.
        # You might want to prioritize the *requesting* user if they are in this list, but any valid member works for RLS usually.
        if r["accountid"] and cid not in proxies_map:
            proxies_map[cid] = r["accountid"]

    return members_map, proxies_map
