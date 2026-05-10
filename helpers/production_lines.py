import asyncio
from collections import defaultdict
from typing import Any, Dict, List, Tuple, Union

import asyncpg
import orjson


async def get_production_data_nested(
    db_obj: Union[asyncpg.Pool, asyncpg.Connection],
    accountid: str,
    include_logistics_data: bool = False
) -> Dict[str, Any]:
    """
    Fetches and structures all production, platform, and storage data for a user's sites.
    """

    # 1. GET ALL SITES FOR THE USER
    query_sites = """
        SELECT
            site.siteid AS site_id, p.name AS planet_name, site.area,
            site.investedpermits, site.maximumpermits, site.foundedtimestamp
        FROM sites AS site
        INNER JOIN planets AS p ON p.planetid = site.addressplanetid
        INNER JOIN users_data AS ud ON ud.userid = site.userid
        INNER JOIN users AS u ON u.userdataid = ud.userid
        WHERE u.accountid = $1;
    """

    sites_records = await db_obj.fetch(query_sites, accountid)

    if not sites_records:
        return {}

    site_ids = [s["site_id"] for s in sites_records]
    final_data = {
        s["site_id"]: {
            "planet_name": s["planet_name"],
            # Initialize keys
            "production_lines": [],
            "storage_items": [],
            "siteStorage": None,
        }
        for s in sites_records
    }

    # 2. PREPARE THE DATA FETCHING TASKS

    tasks_def: List[Tuple[str, str, list]] = []

    # A. Production Lines
    tasks_def.append((
        "lines",
        "SELECT * FROM site_production_lines WHERE siteid = ANY($1::text[])",
        [site_ids]
    ))

    # We need to execute LINES immediately to get line_ids for the next steps

    # --- IMMEDIATE EXECUTION BLOCK 1 (Lines) ---
    lines_records = await db_obj.fetch("SELECT * FROM site_production_lines WHERE siteid = ANY($1::text[])", site_ids)
    line_ids = [l["productionlineid"] for l in lines_records]
    lines_map = {l["productionlineid"]: dict(l) for l in lines_records}

    # --- IMMEDIATE EXECUTION BLOCK 2 (Orders) ---
    orders_records = await db_obj.fetch(
        "SELECT * FROM site_production_line_orders WHERE productionlineid = ANY($1::text[]) AND started IS NULL",
        line_ids,
    )
    orders_by_line = defaultdict(list)
    for order in orders_records:
        orders_by_line[order["productionlineid"]].append(dict(order))

    recipe_ids = list(set([o["recipeid"] for o in orders_records if o.get("recipeid")]))

    # 3. BATCH THE REMAINING HEAVY QUERIES

    if recipe_ids:
        input_select = "SELECT pri.productiontemplateid, mti.ticker, pri.factor, pri.id"
        output_select = "SELECT pro.productiontemplateid, mto.ticker, pro.factor, pro.id"
        if include_logistics_data:
            input_select += ", mti.weight, mti.volume"
            output_select += ", mto.weight, mto.volume"

        tasks_def.append((
            "recipes",
            "SELECT * FROM production_recipes WHERE productiontemplateid = ANY($1::text[]) AND productionlineid = ANY($2::text[])",
            [recipe_ids, line_ids]
        ))
        tasks_def.append((
            "inputs",
            f"{input_select} FROM production_recipe_input_factors AS pri JOIN materials AS mti ON mti.materialid = pri.materialid WHERE pri.productiontemplateid = ANY($1::text[]) AND pri.productionlineid = ANY($2::text[])",
            [recipe_ids, line_ids]
        ))
        tasks_def.append((
            "outputs",
            f"{output_select} FROM production_recipe_output_factors AS pro JOIN materials AS mto ON mto.materialid = pro.materialid WHERE pro.productiontemplateid = ANY($1::text[]) AND pro.productionlineid = ANY($2::text[])",
            [recipe_ids, line_ids]
        ))

    tasks_def.append((
        "storage",
        """
        WITH target_entities AS (
            -- 1. SITE STORAGE (Active)
            SELECT 
                s.siteid AS entity_id, 
                s.siteid AS parent_site_id, -- Link to itself
                'SITE' AS type
            FROM sites s 
            INNER JOIN users u ON s.userid = u.userdataid
            WHERE s.siteid = ANY($1::text[]) 
              AND u.accountid = $2

            UNION ALL

            -- 2. WAREHOUSE STORAGE (Passive)
            SELECT 
                w.warehouseid AS entity_id,
                s.siteid AS parent_site_id, -- Link to the Site it sits on
                'WAREHOUSE' AS type
            FROM warehouses w
            INNER JOIN users u ON w.userid = u.userdataid
            INNER JOIN sites s ON s.siteid = ANY($1::text[])
            WHERE 
                u.accountid = $2
                AND (
                    (s.addressplanetid IS NOT NULL AND w.addressplanet = s.addressplanetid)
                )
        )
        SELECT 
            te.parent_site_id,
            te.entity_id as target_id,
            te.type, 
            st.storageid, 
            st.weightcapacity, st.volumecapacity, st.weightload, st.volumeload,
            COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'material_id', ssi.materialid, 
                        'ticker', m.ticker, 
                        'volume', m.volume, 
                        'weight', m.weight, 
                        'amount', ssi.quantity
                    )
                ) FILTER (WHERE ssi.materialid IS NOT NULL), 
                '[]'::jsonb
            ) AS storage_items
        
        FROM storages st
        JOIN target_entities te ON st.addressableid = te.entity_id
        LEFT JOIN storage_items ssi ON st.storageid = ssi.storageid
        LEFT JOIN materials m ON m.materialid = ssi.materialid
        
        GROUP BY te.parent_site_id, te.entity_id, te.type, st.storageid;
        """,
        [site_ids, accountid]
    ))

    # 4. EXECUTE THE BATCH
    results_map = {}

    if isinstance(db_obj, asyncpg.Connection):
        for name, sql, params in tasks_def:
            results_map[name] = await db_obj.fetch(sql, *params)
    else:
        coros = [db_obj.fetch(sql, *params) for name, sql, params in tasks_def]
        results = await asyncio.gather(*coros, return_exceptions=True)

        # Map results back to names
        for i, (name, _, _) in enumerate(tasks_def):
            results_map[name] = results[i]

    # Helper to safely extract results
    def get_res(name):
        res = results_map.get(name)
        if isinstance(res, Exception) or res is None:
            return []
        return res

    recipes_records = get_res("recipes")
    inputs_records = get_res("inputs")
    outputs_records = get_res("outputs")
    storage_records = get_res("storage")

    # 5. STITCH THE DATA TOGETHER
    recipes_map = {r["productiontemplateid"]: dict(r) for r in recipes_records}
    inputs_by_recipe, outputs_by_recipe = defaultdict(list), defaultdict(list)

    for i in inputs_records:
        item = {"id": i["id"], "ticker": i["ticker"], "factor": i["factor"]}
        if include_logistics_data:
            item.update({"weight": i.get("weight", 0), "volume": i.get("volume", 0)})
        inputs_by_recipe[i["productiontemplateid"]].append(item)

    for o in outputs_records:
        item = {"id": o["id"], "ticker": o["ticker"], "factor": o["factor"]}
        if include_logistics_data:
            item.update({"weight": o.get("weight", 0), "volume": o.get("volume", 0)})
        outputs_by_recipe[o["productiontemplateid"]].append(item)

    # Attach Recipes to Orders
    for _, orders in orders_by_line.items():
        for order in orders:
            recipe_id = order.get("recipeid")
            if recipe_id in recipes_map:
                recipe = recipes_map[recipe_id]
                order["production_recipe"] = {
                    "name": recipe.get("name"),
                    "duration": recipe.get("duration"),
                    "efficiency": recipe.get("efficiency"),
                    "effort_factor": recipe.get("effortfactor"),
                    "inputs": inputs_by_recipe.get(recipe_id, []),
                    "outputs": outputs_by_recipe.get(recipe_id, []),
                }

    # Attach Lines to Sites
    for line_id, line in lines_map.items():
        line["production_orders"] = orders_by_line.get(line_id, [])
        site_id = line.get("siteid")
        if site_id in final_data:
            final_data[site_id]["production_lines"].append({
                "line_id": line["productionlineid"],
                "type": line.get("type"),
                "slots": line.get("slots"),
                "capacity": line.get("capacity"),
                "efficiency": line.get("efficiency"),
                "condition": line.get("condition"),
                "production_orders": line["production_orders"],
            })

    # Attach Storage to Sites
    for record in storage_records:
        parent_id = record["parent_site_id"]

        if parent_id in final_data:
            storage_data = {
                "id": record.get("storageid"),
                "name": "Site Warehouse" if record["type"] == 'WAREHOUSE' else "Site Storage",
                "maxTonnage": record.get("weightcapacity"),
                "maxVolume": record.get("volumecapacity"),
                "currentTonnage": record.get("weightload"),
                "currentVolume": record.get("volumeload"),
            }

            # Parse Items
            items_raw = record.get("storage_items")
            items = orjson.loads(items_raw) if isinstance(items_raw, str) else (items_raw or [])

            if record["type"] == 'SITE':
                final_data[parent_id]["siteStorage"] = storage_data
                final_data[parent_id]["storage_items"] = items # Active items
            elif record["type"] == 'WAREHOUSE':
                final_data[parent_id]["warehouseStorage"] = storage_data
                final_data[parent_id]["warehouse_items"] = items # Passive items

    return final_data
