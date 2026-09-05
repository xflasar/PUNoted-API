import logging
import time
from typing import Any, Dict

from helpers.corrupted_data_cleaner import clean_corrupted_record
from managers.global_ws_manager import global_ws_manager
from services.internal.corp_site_delta_service import compute_and_broadcast_site_delta

logger = logging.getLogger(__name__)


async def handle_production_line_order_add_message(db, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles a message for a single new or updated production line order.
    Performs an UPSERT on the main order table and a DELETE-then-INSERT
    on the nested materials (inputs/outputs).
    """
    start_time = time.perf_counter()
    logger.debug("Starting processing production line order added/updated data.")

    converted_record = raw_payload.get("data")

    if not converted_record or not converted_record.get("orderid"):
        logger.warning("Missing or invalid 'data' or 'id' in payload.")
        return {"success": False, "message": "Missing required order ID in payload."}

    if not converted_record:
        logger.warning(f"Conversion failed for order ID: {converted_record.get('orderid')}")
        return {"success": False, "message": "Failed to convert order data."}

    cleaned_order = converted_record
    order_id = cleaned_order["orderid"]

    # Separate main order fields from nested material lists
    order_data_to_upsert = cleaned_order.copy()
    inputs_data = order_data_to_upsert.pop("inputs", [])
    outputs_data = order_data_to_upsert.pop("outputs", [])

    # Ensure a single transaction for atomicity
    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                # Ensure parent production line exists (avoid race condition ForeignKeyViolationError)
                if order_data_to_upsert.get("productionlineid"):
                    await con.execute(
                        "INSERT INTO site_production_lines (productionlineid) VALUES ($1) ON CONFLICT (productionlineid) DO NOTHING;",
                        order_data_to_upsert["productionlineid"]
                    )

                # --- A. UPSERT Main Order Record ---

                # 1. Prepare keys and placeholders
                order_keys = list(order_data_to_upsert.keys())
                order_columns = ", ".join(order_keys)
                order_placeholders = ", ".join([f"${i + 1}" for i in range(len(order_keys))])

                # 2. Prepare UPDATE SET clause for all non-primary key columns
                # The primary key for UPSERT is assumed to be 'orderid'
                update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in order_keys if col != "orderid"])

                # 3. Define and execute UPSERT query
                SQL_UPSERT_ORDER = f"""
                    INSERT INTO site_production_line_orders ({order_columns}) 
                    VALUES ({order_placeholders})
                    ON CONFLICT (orderid) DO UPDATE 
                    SET {update_clause};
                """

                order_data_to_upsert = clean_corrupted_record(order_data_to_upsert)

                await con.execute(SQL_UPSERT_ORDER, *order_data_to_upsert.values())

                logger.debug(f"UPSERT successful for main order record ID: {order_id}")

    except Exception as e:
        end_time = time.perf_counter()
        elapsed_time = (end_time - start_time) * 1000
        logger.error(
            f"Failed to process order update/add for order ID: {order_id}. Error: {e} | Took: {elapsed_time:.2f}ms",
            exc_info=True,
        )
        raise

    # --- Trigger Real-Time Site Production WebSocket Update ---
    user_id = raw_payload.get("userId") or raw_payload.get("data", {}).get("userid")
    production_line_id = order_data_to_upsert.get("productionlineid")
    if user_id and production_line_id:
        try:
            async with db.pool.acquire() as conn:
                site_row = await conn.fetchrow(
                    "SELECT siteid::text FROM site_production_lines WHERE productionlineid = $1;",
                    production_line_id
                )
                if site_row and site_row["siteid"]:
                    await compute_and_broadcast_site_delta(db, global_ws_manager, site_row["siteid"], user_id)
        except Exception as ws_err:
            logger.error(f"Failed to send site production delta on order added: {ws_err}")

    end_time = time.perf_counter()
    elapsed_time = (end_time - start_time) * 1000

    logger.debug(
        f"Successfully processed order ID: {order_id}. "
        f"Inputs: {len(inputs_data)}, Outputs: {len(outputs_data)}. | Took: {elapsed_time:.2f}ms"
    )

    return {"success": True, "orderid": order_id}
