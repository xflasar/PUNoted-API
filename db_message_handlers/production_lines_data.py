from helpers import logistics_engine
import logging
import time
from itertools import chain
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


async def handle_production_lines_data_message(db, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    converted_data = raw_payload.get("data", {}) if isinstance(raw_payload, dict) else {}

    site_id = converted_data.get("siteid")
    logger.info(f"[INFO] Handling production lines data message for site {site_id}")
    production_lines = converted_data.get("production_lines", [])

    # 1. WARNING: Payload structural anomalies
    if not site_id:
        logger.warning(
            f"[WARNING] Payload missing 'siteid'! Cannot process lines. Payload keys received: {list(converted_data.keys())}"
        )
        return {"success": False, "message": "Missing siteid"}

    if not isinstance(production_lines, list):
        logger.warning(
            f"[WARNING] 'production_lines' field for site {site_id} is not a list! Got: {type(production_lines)}"
        )
        return {"success": False, "message": "Invalid production_lines data structure"}

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                # Lock row to prevent race conditions during rapid multi-site messages
                await con.execute("SELECT siteid FROM sites WHERE siteid = $1 FOR UPDATE;", site_id)

                # 2. Fetch existing production lines
                query = "SELECT productionlineid FROM site_production_lines WHERE siteid=$1;"
                query_response = await con.fetch(query, site_id)
                existing_production_lines_ids = {record["productionlineid"] for record in query_response}

                # 3. Extract incoming line IDs
                incoming_production_line_ids = {
                    record.get("productionlineid")
                    for record in production_lines
                    if isinstance(record, dict) and record.get("productionlineid")
                }

                # DEBUG: Catch silent drop where APEX sends invalid line records without IDs
                if len(incoming_production_line_ids) < len(production_lines):
                    logger.debug(
                        f"[SILENT DETECT] Site {site_id}: {len(production_lines) - len(incoming_production_line_ids)} "
                        f"production line records were missing 'productionlineid' in payload!"
                    )

                # 4. Handle stale line deletions
                lines_to_delete = existing_production_lines_ids - incoming_production_line_ids
                if lines_to_delete:
                    logger.warning(
                        f"[WARNING] Site {site_id}: Wiping {len(lines_to_delete)} stale lines from DB -> {lines_to_delete}"
                    )
                    # Clean child orders first to prevent FK constraint failures
                    await con.execute(
                        "DELETE FROM site_production_line_orders WHERE productionlineid = ANY($1::text[]);",
                        list(lines_to_delete),
                    )
                    await con.execute(
                        "DELETE FROM site_production_lines WHERE siteid = $1 AND productionlineid = ANY($2::text[]);",
                        site_id,
                        list(lines_to_delete),
                    )

                # DEBUG / WARNING: Empty production lines check
                if not production_lines:
                    if existing_production_lines_ids:
                        logger.warning(
                            f"[WARNING] Site {site_id} payload contains ZERO production lines! All {len(existing_production_lines_ids)} existing DB lines were wiped."
                        )
                    else:
                        logger.debug(f"[SILENT DETECT] Site {site_id} sent empty production lines array (idle/empty site).")
                    
                    return {
                        "success": True,
                        "message": "No production lines in payload. Stale lines (if any) deleted.",
                    }

                # 5. Categorize inserts vs updates & extract child structures
                records_to_insert = []
                records_to_update = []
                orders = []
                production_templates = []
                efficiency_factors = []
                workforces = []

                for record in production_lines:
                    if not isinstance(record, dict):
                        logger.warning(f"[WARNING] Invalid record type inside production_lines for site {site_id}: {type(record)}")
                        continue

                    p_id = record.get("productionlineid")
                    if not p_id:
                        continue

                    orders.extend(record.get("orders", []) or [])
                    production_templates.extend(record.get("production_templates", []) or [])
                    efficiency_factors.extend(record.get("efficiency_factors", []) or [])
                    workforces.extend(record.get("workforces", []) or [])

                    temp_record = record.copy()
                    temp_record.pop("orders", None)
                    temp_record.pop("production_templates", None)
                    temp_record.pop("efficiency_factors", None)
                    temp_record.pop("workforces", None)

                    if p_id not in existing_production_lines_ids:
                        records_to_insert.append(temp_record)
                    else:
                        records_to_update.append(temp_record)

                # 6. Perform Inserts
                if records_to_insert:
                    all_keys = list(records_to_insert[0].keys())
                    keys = ", ".join(all_keys)
                    placeholders = ", ".join([f"${i + 1}" for i in range(len(all_keys))])
                    insert_query = f"INSERT INTO site_production_lines ({keys}) VALUES ({placeholders}) ON CONFLICT (productionlineid) DO NOTHING;"
                    
                    insert_tuples = [tuple(rec.get(k) for k in all_keys) for rec in records_to_insert]
                    res = await con.executemany(insert_query, insert_tuples)

                    # DEBUG: Detect silent conflict drops
                    logger.debug(f"[SILENT DETECT] Site {site_id}: Bulk inserted {len(records_to_insert)} lines -> Result: {res}")

                # 7. Perform Updates
                if records_to_update:
                    for record_to_update in records_to_update:
                        update_data = record_to_update.copy()
                        record_id = update_data.pop("productionlineid", None)
                        if not record_id or not update_data:
                            logger.warning(f"[WARNING] Site {site_id}: Attempted UPDATE on empty record for line {record_id}")
                            continue
                        
                        update_fields = ", ".join([f"{key} = ${i + 2}" for i, key in enumerate(update_data.keys())])
                        update_query = f"UPDATE site_production_lines SET {update_fields} WHERE productionlineid = $1;"
                        res = await con.execute(update_query, record_id, *update_data.values())

                        # DEBUG: Catch silent UPDATE failures where 0 rows were updated
                        if "UPDATE 0" in res:
                            logger.debug(
                                f"[SILENT DETECT] Site {site_id}: UPDATE query succeeded but modified 0 rows for line {record_id}!"
                            )

                # 8. Process nested orders & templates
                await process_orders(con, site_id, orders, incoming_production_line_ids)
                await process_production_templates(con, site_id, production_templates)

        elapsed = time.perf_counter() - start_time
        if elapsed > 1.5:
            logger.warning(f"[WARNING] Slow transaction! Site {site_id} took {elapsed:.2f}s to process.")

        return {"success": True, "message": "Processed production lines data."}

    except Exception as e:
        logger.error(
            f"[ERROR] Transaction FAILED for site {site_id}! Error: {str(e)}", 
            exc_info=True
        )
        raise


async def process_orders(con, site_id: str, orders: List[Dict[str, Any]], line_ids_for_site: set):
    if not line_ids_for_site:
        logger.warning(f"[WARNING] process_orders called for site {site_id} with empty line_ids set!")
        return

    # Delete existing orders strictly for the lines belonging to THIS site
    delete_res = await con.execute(
        "DELETE FROM site_production_line_orders WHERE productionlineid = ANY($1::text[]);",
        list(line_ids_for_site),
    )

    if not orders:
        logger.debug(f"[SILENT DETECT] Site {site_id}: Cleared previous orders, but incoming payload contains 0 active orders.")
        return

    records_to_insert = [
        {k: v for k, v in o.items() if k not in ["inputs", "outputs"]}
        for o in orders
        if isinstance(o, dict) and o.get("orderid") and o.get("productionlineid")
    ]

    # DEBUG / WARNING: Payload contained orders, but none passed validation
    if len(records_to_insert) < len(orders):
        logger.warning(
            f"[WARNING] Site {site_id}: Payload sent {len(orders)} orders, but {len(orders) - len(records_to_insert)} were dropped due to missing 'orderid' or 'productionlineid'!"
        )

    if not records_to_insert:
        return

    all_keys = list(records_to_insert[0].keys())
    keys = ", ".join(all_keys)
    values_placeholders = ", ".join([f"${i + 1}" for i in range(len(all_keys))])
    query = f"INSERT INTO site_production_line_orders ({keys}) VALUES ({values_placeholders}) ON CONFLICT (orderid) DO NOTHING;"
    
    order_tuples = [tuple(rec.get(k) for k in all_keys) for rec in records_to_insert]
    await con.executemany(query, order_tuples)


async def process_production_templates(con, site_id: str, recipes_with_factors: List[Dict[str, Any]]):
    if not recipes_with_factors:
        return

    all_input_factors = list(chain.from_iterable(r.get("input_factors", []) for r in recipes_with_factors if isinstance(r, dict)))
    all_output_factors = list(chain.from_iterable(r.get("output_factors", []) for r in recipes_with_factors if isinstance(r, dict)))

    recipes_data_for_db = [
        {k: v for k, v in r.items() if k not in ["input_factors", "output_factors"]} 
        for r in recipes_with_factors if isinstance(r, dict)
    ]

    if not recipes_data_for_db:
        logger.warning(f"[WARNING] Site {site_id}: Received template recipes array, but all entries failed dict parsing!")
        return

    recipe_keys = list(recipes_data_for_db[0].keys())
    recipe_columns = ", ".join(recipe_keys)
    recipe_placeholders = ", ".join([f"${i + 1}" for i in range(len(recipe_keys))])
    update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in recipe_keys if col != "productiontemplateid"])
    recipes_tuples = [tuple(r.get(k) for k in recipe_keys) for r in recipes_data_for_db]

    SQL_UPSERT_RECIPES = f"""
        INSERT INTO production_recipes ({recipe_columns}) 
        VALUES ({recipe_placeholders})
        ON CONFLICT (productiontemplateid, productionlineid) DO UPDATE 
        SET {update_clause};
    """

    await con.executemany(SQL_UPSERT_RECIPES, recipes_tuples)

    # Process nested input/output factors
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
            input_factors_tuples = [tuple(f.get(k) for k in factor_keys) for f in all_input_factors]
            SQL_UPSERT_INPUT_FACTORS = f"""
                INSERT INTO production_recipe_input_factors ({factor_columns})
                VALUES ({factor_placeholders})
                ON CONFLICT (productiontemplateid, materialid, productionlineid) DO UPDATE 
                SET {factor_update_clause};
            """
            await con.executemany(SQL_UPSERT_INPUT_FACTORS, input_factors_tuples)

        if all_output_factors:
            output_factors_tuples = [tuple(f.get(k) for k in factor_keys) for f in all_output_factors]
            SQL_UPSERT_OUTPUT_FACTORS = f"""
                INSERT INTO production_recipe_output_factors ({factor_columns})
                VALUES ({factor_placeholders})
                ON CONFLICT (productiontemplateid, materialid, productionlineid) DO UPDATE 
                SET {factor_update_clause};
            """
            await con.executemany(SQL_UPSERT_OUTPUT_FACTORS, output_factors_tuples)