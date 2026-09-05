import logging
from typing import Any, Dict, Optional
from helpers.corp_production_calc import process_corp_production_and_workforce

logger = logging.getLogger(__name__)

async def compute_and_broadcast_site_delta(db, global_ws_manager, site_id: str, user_id: str):
    """
    Computes site production metrics for a single site and broadcasts a
    CORP_SITE_PRODUCTION_DELTA message to the user's corporation WebSocket channel.
    """
    if not site_id or not user_id:
        return

    try:
        async with db.pool.acquire() as conn:
            # 1. Fetch Corp ID for this user
            corp_row = await conn.fetchrow(
                """
                SELECT cs.corporationid::text as corporationid
                FROM corporation_shareholders cs
                JOIN users u ON u.userdataid = cs.userid
                WHERE u.accountid::text = $1 OR u.userdataid::text = $1
                LIMIT 1;
                """,
                str(user_id)
            )
            if not corp_row or not corp_row["corporationid"]:
                return

            corp_id = corp_row["corporationid"]

            # 2. Fetch site player and location details
            site_row = await conn.fetchrow(
                """
                SELECT 
                    s.siteid::text as siteid,
                    p.naturalid as location_name,
                    COALESCE(cs.companycode, u.username) as player_name,
                    (ud.subscriptionlevel = 'PRO' AND ud.subscriptionexpiry > NOW()) as is_accurate
                FROM sites s
                JOIN users u ON s.userid = u.userdataid
                JOIN planets p ON p.planetid = s.addressplanetid
                LEFT JOIN users_data ud ON ud.userid = u.userdataid
                LEFT JOIN corporation_shareholders cs ON cs.userid = u.userdataid
                WHERE s.siteid::text = $1;
                """,
                site_id
            )

            if not site_row:
                return

            player_name = site_row["player_name"]
            location_name = site_row["location_name"]
            is_accurate = bool(site_row["is_accurate"])

            # 3. Fetch production lines & orders for this site
            lines_rows = await conn.fetch(
                """
                SELECT 
                    pl.productionlineid::text as line_id,
                    pl.capacity,
                    pl.efficiency,
                    pl.condition,
                    po.orderid::text as order_id,
                    po.recurring,
                    po.created,
                    po.completion,
                    po.duration as order_duration,
                    po.recipeid::text as recipe_id
                FROM site_production_lines pl
                LEFT JOIN site_production_line_orders po ON po.productionlineid = pl.productionlineid
                    AND (po.completion IS NULL OR po.completion > NOW() OR po.completed IS NOT TRUE)
                WHERE pl.siteid::text = $1;
                """,
                site_id
            )

            lines_map = {}
            unique_pairs = set()

            for r in lines_rows:
                lid = r["line_id"]
                if lid not in lines_map:
                    lines_map[lid] = {
                        "capacity": r["capacity"],
                        "condition": r["condition"],
                        "efficiency": r.get("efficiency", 1.0),
                        "production_orders": [],
                    }

            recipe_ids = list(set(str(r["recipe_id"]) for r in lines_rows if r.get("recipe_id")))

            if recipe_ids:
                # Fetch recipe components
                recipe_map = {}
                c_rows = await conn.fetch(
                    """
                    SELECT DISTINCT ON (productiontemplateid::text) productiontemplateid::text as recipe_id, duration
                    FROM production_recipes
                    WHERE productiontemplateid::text = ANY($1::text[]);
                    """,
                    recipe_ids
                )
                for r in c_rows:
                    recipe_map[r['recipe_id']] = {
                        "duration": r["duration"],
                        "inputs": [],
                        "outputs": [],
                    }

                in_rows = await conn.fetch(
                    """
                    SELECT productiontemplateid::text as recipe_id, materialid as ticker, factor
                    FROM production_recipe_input_factors
                    WHERE productiontemplateid::text = ANY($1::text[]);
                    """,
                    recipe_ids
                )
                for i in in_rows:
                    k = i['recipe_id']
                    if k in recipe_map:
                        recipe_map[k]["inputs"].append({"ticker": i["ticker"], "factor": i["factor"]})

                out_rows = await conn.fetch(
                    """
                    SELECT productiontemplateid::text as recipe_id, materialid as ticker, factor
                    FROM production_recipe_output_factors
                    WHERE productiontemplateid::text = ANY($1::text[]);
                    """,
                    recipe_ids
                )
                for o in out_rows:
                    k = o['recipe_id']
                    if k in recipe_map:
                        recipe_map[k]["outputs"].append({"ticker": o["ticker"], "factor": o["factor"]})

                # Reconstruct line orders
                for r in lines_rows:
                    lid = r["line_id"]
                    if r["order_id"] and r["recipe_id"]:
                        r_key = str(r['recipe_id'])
                        lines_map[lid]["production_orders"].append({
                            "order_id": r["order_id"],
                            "recurring": r.get("recurring"),
                            "created": r["created"].isoformat() if r["created"] else None,
                            "completion": r["completion"].isoformat() if r["completion"] else None,
                            "duration": r["order_duration"],
                            "production_recipe": recipe_map.get(r_key, {}),
                        })

            lines_list = list(lines_map.values())
            prod_raw = [{
                "player_name": player_name,
                "location_name": location_name,
                "is_accurate": is_accurate,
                "production_lines": lines_list,
            }]

            # Calculate site flow
            corp_flow = process_corp_production_and_workforce(prod_raw, [])

            # Format delta payload
            formatted_materials = []
            for ticker, info in corp_flow.items():
                p_entry = info["producers"].get((location_name, player_name), {})
                c_entry = info["consumers"].get((location_name, player_name), {})

                p_tot = p_entry.get("acc", 0.0) + p_entry.get("est", 0.0)
                c_tot = c_entry.get("acc", 0.0) + c_entry.get("est", 0.0)

                raw_recipes = info.get("user_recipes_used", {})
                rec_list = []
                for r_key, r_data in raw_recipes.items():
                    user_metrics = r_data.get("users", {}).get((player_name, location_name), {})
                    if user_metrics.get("daily_output", 0.0) > 0:
                        rec_list.append({
                            "recipeKey": r_key,
                            "building": r_data["building"],
                            "dailyOutput": round(user_metrics["daily_output"], 2),
                            "dailyCycles": round(user_metrics["daily_cycles"], 2),
                            "outputAmount": r_data["output_amount"],
                            "inputs": {k: round(v, 2) for k, v in r_data["inputs"].items()},
                        })

                if p_tot > 0 or c_tot > 0 or p_entry.get("batch_active") or c_entry.get("batch_active") or rec_list:
                    formatted_materials.append({
                        "ticker": ticker,
                        "prodAmount": round(p_tot, 2),
                        "consAmount": round(c_tot, 2),
                        "batchProdActive": round(p_entry.get("batch_active", 0.0), 2),
                        "batchProdQueued": round(p_entry.get("batch_queued", 0.0), 2),
                        "batchConsActive": round(c_entry.get("batch_active", 0.0), 2),
                        "batchConsQueued": round(c_entry.get("batch_queued", 0.0), 2),
                        "userRecipesUsed": rec_list,
                    })

            delta_payload = {
                "type": "CORP_SITE_PRODUCTION_DELTA",
                "siteid": site_id,
                "player": player_name,
                "loc": location_name,
                "is_accurate": is_accurate,
                "materials": formatted_materials,
            }

            await global_ws_manager.broadcast(f"map:corp:{corp_id}", delta_payload)
            logger.debug(f"[WEBSOCKET] Broadcasted CORP_SITE_PRODUCTION_DELTA for site {site_id} on map:corp:{corp_id}")

    except Exception as e:
        logger.error(f"Failed to compute and broadcast site delta for site {site_id}: {e}", exc_info=True)
