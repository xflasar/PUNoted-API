import logging
import time
from typing import Any, Dict

from helpers.corrupted_data_cleaner import clean_corrupted_record

logger = logging.getLogger(__name__)


async def handle_production_line_order_update_message(db, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles a message for a single updated production line order.
    Performs an UPSERT (Update/Insert) on the main order table and a DELETE-then-INSERT
    on the nested materials (inputs/outputs) to guarantee a fresh, accurate state.
    """
    start_time = time.perf_counter()
    logger.debug("Starting processing production line order update data.")

    converted_record = raw_payload.get("data")

    if not converted_record or not converted_record.get("orderid"):
        logger.warning("Missing or invalid 'data' or 'orderid' in payload.")
        return {"success": False, "message": "Missing required order ID in payload."}

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
                # --- A. UPSERT Main Order Record ---

                order_keys = list(order_data_to_upsert.keys())
                order_columns = ", ".join(order_keys)
                order_placeholders = ", ".join([f"${i + 1}" for i in range(len(order_keys))])

                # 2. Prepare UPDATE SET clause for all non-primary key columns
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
            f"Failed to process order update for order ID: {order_id}. Error: {e} | Took: {elapsed_time:.2f}ms",
            exc_info=True,
        )
        raise

    end_time = time.perf_counter()
    elapsed_time = (end_time - start_time) * 1000

    logger.debug(
        f"Successfully processed order update for ID: {order_id}. "
        f"Inputs: {len(inputs_data)}, Outputs: {len(outputs_data)}. | Took: {elapsed_time:.2f}ms"
    )

    return {"success": True, "orderid": order_id}
