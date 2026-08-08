import logging
import time
from itertools import chain
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


async def handle_production_lines_data_message(db, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    converted_data = raw_payload.get("data", {})

    site_id = converted_data.get("siteid")
    production_lines = converted_data.get("production_lines", [])

    if not site_id:
        logger.warning("Received production lines message without siteid.")
        return {"success": False, "message": "Missing siteid"}

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                # 1. LOCK THE SITE ROW to prevent concurrent message collisions for the same site
                await con.execute("SELECT siteid FROM sites WHERE siteid = $1 FOR UPDATE;", site_id)

                # 2. Fetch existing production lines for THIS site only
                query = "SELECT productionlineid FROM site_production_lines WHERE siteid = $1;"
                query_response = await con.fetch(query, site_id)
                existing_ids = {record["productionlineid"] for record in query_response}

                incoming_ids = {
                    record["productionlineid"] 
                    for record in production_lines 
                    if record.get("productionlineid")
                }

                # 3. Clean up stale lines removed by the user in-game
                lines_to_delete = existing_ids - incoming_ids
                if lines_to_delete:
                    # Clean child orders first to avoid FK constraint errors
                    await con.execute(
                        "DELETE FROM site_production_line_orders WHERE productionlineid = ANY($1::text[]);",
                        list(lines_to_delete)
                    )
                    await con.execute(
                        "DELETE FROM site_production_lines WHERE siteid = $1 AND productionlineid = ANY($2::text[]);",
                        site_id, list(lines_to_delete)
                    )

                if not production_lines:
                    return {"success": True, "message": f"Cleared all lines for site {site_id}."}

                # 4. Separate inserts vs updates
                records_to_insert = []
                records_to_update = []
                orders = []
                production_templates = []

                for record in production_lines:
                    p_id = record.get("productionlineid")
                    if not p_id:
                        continue

                    orders.extend(record.get("orders", []) or [])
                    production_templates.extend(record.get("production_templates", []) or [])

                    clean_rec = {
                        k: v for k, v in record.items() 
                        if k not in ["orders", "production_templates", "efficiency_factors", "workforces"]
                    }

                    if p_id not in existing_ids:
                        records_to_insert.append(clean_rec)
                    else:
                        records_to_update.append(clean_rec)

                # 5. Bulk Inserts (Guaranteed Column Alignment)
                if records_to_insert:
                    all_keys = list(records_to_insert[0].keys())
                    cols = ", ".join(all_keys)
                    placeholders = ", ".join([f"${i + 1}" for i in range(len(all_keys))])
                    
                    insert_sql = f"""
                        INSERT INTO site_production_lines ({cols}) 
                        VALUES ({placeholders}) 
                        ON CONFLICT (productionlineid) DO NOTHING;
                    """
                    tuples_to_insert = [tuple(r.get(k) for k in all_keys) for r in records_to_insert]
                    await con.executemany(insert_sql, tuples_to_insert)

                # 6. Bulk Updates
                if records_to_update:
                    for rec in records_to_update:
                        r_id = rec.pop("productionlineid")
                        if not rec:
                            continue
                        set_clause = ", ".join([f"{k} = ${i + 2}" for i, k in enumerate(rec.keys())])
                        update_sql = f"UPDATE site_production_lines SET {set_clause} WHERE productionlineid = $1;"
                        await con.execute(update_sql, r_id, *rec.values())

                # 7. Process orders strictly for incoming lines in this specific site message
                await process_orders(con, orders, incoming_ids)
                await process_production_templates(con, production_templates)

        logger.debug(f"Processed site {site_id} ({len(production_lines)} lines) in {time.perf_counter() - start_time:.2f}s")
        return {"success": True, "message": f"Processed site {site_id}"}

    except Exception as e:
        logger.error(f"Error handling site {site_id}: {e}", exc_info=True)
        raise


async def process_production_templates(con, recipes_with_factors: List[Dict[str, Any]]):
    """
    Asynchronously processes a list of recipes, flattens the nested factors,
    and performs a bulk UPSERT operation on all three tables within the transaction.
    """
    if not recipes_with_factors:
        return

    all_input_factors = list(chain.from_iterable(r.get("input_factors", []) for r in recipes_with_factors))
    all_output_factors = list(chain.from_iterable(r.get("output_factors", []) for r in recipes_with_factors))

    recipes_data_for_db = [
        {k: v for k, v in r.items() if k not in ["input_factors", "output_factors"]} for r in recipes_with_factors
    ]

    if not recipes_data_for_db:
        return

    recipe_keys = list(recipes_data_for_db[0].keys())
    recipe_columns = ", ".join(recipe_keys)
    recipe_placeholders = ", ".join([f"${i + 1}" for i in range(len(recipe_keys))])
    update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in recipe_keys if col != "productiontemplateid"])
    recipes_tuples = [tuple(r.values()) for r in recipes_data_for_db]

    SQL_UPSERT_RECIPES = f"""
        INSERT INTO production_recipes ({recipe_columns}) 
        VALUES ({recipe_placeholders})
        ON CONFLICT (productiontemplateid, productionlineid) DO UPDATE 
        SET {update_clause};
    """

    await con.executemany(SQL_UPSERT_RECIPES, recipes_tuples)

    factor_keys = []
    if all_input_factors:
        factor_keys = list(all_input_factors[0].keys())
    elif all_output_factors:
        factor_keys = list(all_output_factors[0].keys())

    if factor_keys:
        factor_columns = ", ".join(factor_keys)
        factor_placeholders = ", ".join([f"${i + 1}" for i in range(len(factor_keys))])
        factor_update_clause = ", ".join(
            [f"{col} = EXCLUDED.{col}" for col in factor_keys if col not in ("productiontemplateid", "materialid")]
        )

        if all_input_factors:
            input_factors_tuples = [tuple(f.values()) for f in all_input_factors]
            SQL_UPSERT_INPUT_FACTORS = f"""
                INSERT INTO production_recipe_input_factors ({factor_columns})
                VALUES ({factor_placeholders})
                ON CONFLICT (productiontemplateid, materialid, productionlineid) DO UPDATE 
                SET {factor_update_clause};
            """
            await con.executemany(SQL_UPSERT_INPUT_FACTORS, input_factors_tuples)

        if all_output_factors:
            output_factors_tuples = [tuple(f.values()) for f in all_output_factors]
            SQL_UPSERT_OUTPUT_FACTORS = f"""
                INSERT INTO production_recipe_output_factors ({factor_columns})
                VALUES ({factor_placeholders})
                ON CONFLICT (productiontemplateid, materialid, productionlineid) DO UPDATE 
                SET {factor_update_clause};
            """
            await con.executemany(SQL_UPSERT_OUTPUT_FACTORS, output_factors_tuples)


async def process_orders(con, orders: List[Dict[str, Any]], all_incoming_line_ids: set):
    if not all_incoming_line_ids:
        return

    await con.execute(
        "DELETE FROM site_production_line_orders WHERE productionlineid = ANY($1::text[]);",
        list(all_incoming_line_ids),
    )

    # Now proceed with re-inserting whatever orders came in
    if not orders:
        return

    records_to_insert = [
        {k: v for k, v in o.items() if k not in ["inputs", "outputs"]}
        for o in orders
        if o.get("orderid") and o.get("productionlineid")
    ]

    if not records_to_insert:
        return

    keys = ", ".join(records_to_insert[0].keys())
    values_placeholders = ", ".join([f"${i + 1}" for i in range(len(records_to_insert[0]))])
    query = f"INSERT INTO site_production_line_orders ({keys}) VALUES ({values_placeholders}) ON CONFLICT (orderid) DO NOTHING;"
    await con.executemany(query, [list(rec.values()) for rec in records_to_insert])


async def process_effficiency_factors(efficiency_factors: List[Dict[str, Any]]):
    return


async def process_workforce(workforce: List[Dict[str, Any]]):
    return
