import logging
import time
from itertools import chain
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


async def handle_production_lines_data_message(db, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.debug("Starting processing production lines data.")
    converted_data = raw_payload["data"]

    site_id = converted_data["siteid"]
    production_lines = converted_data["production_lines"]

    # It's valid for a site to have 0 production lines.
    # The check `if not production_lines` would prevent clearing all lines.

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                # 1. Fetch all existing production lines for the site
                query = "SELECT productionlineid FROM site_production_lines WHERE siteid=$1;"
                query_response = await con.fetch(query, site_id)
                existing_production_lines_ids = {record["productionlineid"] for record in query_response}

                # 2. Get all incoming production line IDs from the payload
                incoming_production_line_ids = {
                    record.get("productionlineid") for record in production_lines if record.get("productionlineid")
                }

                # 3. Determine which existing lines are NOT in the payload and delete them
                lines_to_delete = existing_production_lines_ids - incoming_production_line_ids
                if lines_to_delete:
                    logger.debug(f"Deleting {len(lines_to_delete)} stale production lines for site {site_id}.")
                    # Assuming ON DELETE CASCADE is set for foreign keys in related tables
                    # (e.g., site_production_line_orders). If not, manual deletion is needed.
                    delete_query = (
                        "DELETE FROM site_production_lines WHERE siteid = $1 AND productionlineid = ANY($2::text[]);"
                    )
                    await con.execute(delete_query, site_id, list(lines_to_delete))

                # If there are no production lines in the payload, we're done.
                if not production_lines:
                    return {
                        "success": True,
                        "message": "No production lines in payload. Stale lines (if any) deleted.",
                    }

                # 4. Separate incoming lines into records to insert vs. update
                records_to_insert = []
                records_to_update = []

                orders = []
                production_templates = []
                efficiency_factors = []
                workforces = []

                for record in production_lines:
                    production_line_id = record.get("productionlineid")
                    if not production_line_id:
                        continue

                    orders.extend(record.get("orders", []))
                    production_templates.extend(record.get("production_templates", []))
                    efficiency_factors.extend(record.get("efficiency_factors", []))
                    workforces.extend(record.get("workforces", []))

                    temp_record = record.copy()
                    temp_record.pop("orders", None)
                    temp_record.pop("production_templates", None)
                    temp_record.pop("efficiency_factors", None)
                    temp_record.pop("workforces", None)

                    if production_line_id not in existing_production_lines_ids:
                        records_to_insert.append(temp_record)
                    else:
                        records_to_update.append(temp_record)

                # 5. Perform inserts and updates
                if records_to_insert:
                    logger.debug(f"Found {len(records_to_insert)} new production lines. Performing bulk insert.")
                    keys = ", ".join(records_to_insert[0].keys())
                    values_placeholders = ", ".join([f"${i + 1}" for i in range(len(records_to_insert[0]))])
                    insert_query = f"INSERT INTO site_production_lines ({keys}) VALUES ({values_placeholders}) ON CONFLICT (productionlineid) DO NOTHING;"
                    await con.executemany(insert_query, [list(rec.values()) for rec in records_to_insert])

                if records_to_update:
                    logger.debug(f"Found {len(records_to_update)} existing production lines. Performing bulk update.")
                    for record_to_update in records_to_update:
                        update_data = record_to_update.copy()
                        record_id = update_data.pop("productionlineid")
                        update_fields = ", ".join([f"{key} = ${i + 2}" for i, key in enumerate(update_data.keys())])
                        update_query = f"UPDATE site_production_lines SET {update_fields} WHERE productionlineid = $1;"
                        await con.execute(update_query, record_id, *update_data.values())

                # 6. Process nested data for the upserted lines
                await process_orders(con, orders, incoming_production_line_ids)
                await process_production_templates(con, production_templates)
                # Placeholder for other nested data processing
                # await process_effficiency_factors(efficiency_factors)
                # await process_workforce(workforces)

        end_time = time.perf_counter()
        logger.debug(f"Finished processing production lines data in {end_time - start_time:.2f} seconds.")
        return {"success": True, "message": "Processed production lines data."}
    except Exception as e:
        logger.error(f"Error processing production line data: {e}", exc_info=True)
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