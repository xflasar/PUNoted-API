import logging
import time
from typing import Any, Dict
from managers.global_ws_manager import global_ws_manager
from services.internal.corp_site_delta_service import compute_and_broadcast_site_delta

logger = logging.getLogger(__name__)


async def handle_production_line_order_remove_message(db, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.debug("Starting processing production line order removed data.")

    converted_data = raw_payload.get("data", {}) if isinstance(raw_payload, dict) else {}
    if not isinstance(converted_data, dict):
        converted_data = {}

    order_id = (
        converted_data.get("orderid")
        or converted_data.get("orderId")
        or converted_data.get("id")
    )
    production_line_id = (
        converted_data.get("productionlineid")
        or converted_data.get("productionLineId")
        or converted_data.get("lineId")
    )

    # Basic input validation: order_id is the globally unique primary key in site_production_line_orders
    if not order_id:
        logger.warning(f"Missing required order ID in payload: data={converted_data}")
        return {"error": "Missing required orderid"}

    rows_deleted = 0

    try:
        async with db.pool.acquire() as conn:
            # If production_line_id is known, delete by orderid and line, or fallback to orderid alone
            if production_line_id:
                query = """
                    DELETE FROM site_production_line_orders 
                    WHERE orderid = $1 AND productionlineid = $2;
                """
                status = await conn.execute(query, order_id, production_line_id)
            else:
                query = """
                    DELETE FROM site_production_line_orders 
                    WHERE orderid = $1;
                """
                status = await conn.execute(query, order_id)

            # Extract the number of deleted rows from the status string
            if status.startswith("DELETE"):
                try:
                    rows_deleted = int(status.split()[-1])
                except ValueError:
                    rows_deleted = 0

            # Fallback: if line filter resulted in 0 deleted rows, try orderid directly
            if rows_deleted == 0 and production_line_id:
                fallback_status = await conn.execute(
                    "DELETE FROM site_production_line_orders WHERE orderid = $1;",
                    order_id,
                )
                if fallback_status.startswith("DELETE"):
                    try:
                        rows_deleted = int(fallback_status.split()[-1])
                    except ValueError:
                        rows_deleted = 0

    except Exception as e:
        end_time = time.perf_counter()
        elapsed_time = (end_time - start_time) * 1000
        logger.error(
            f"Failed to remove order {order_id} (line: {production_line_id}). "
            f"Error: {e} | Took: {elapsed_time:.2f}ms",
            exc_info=True,
        )
        raise

    end_time = time.perf_counter()
    elapsed_time = (end_time - start_time) * 1000

    if rows_deleted > 0:
        logger.debug(
            f"Successfully removed {rows_deleted} order(s) for orderid={order_id} "
            f"from productionlineid={production_line_id}. | Took: {elapsed_time:.2f}ms"
        )
    else:
        logger.warning(
            f"Attempted to remove order {order_id} from line {production_line_id}, "
            f"but 0 rows were deleted. It may have already been processed. | Took: {elapsed_time:.2f}ms"
        )

    # --- Trigger Real-Time Site Production WebSocket Update ---
    user_id = raw_payload.get("userId") or raw_payload.get("data", {}).get("userid")
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
            logger.error(f"Failed to send site production delta on order removed: {ws_err}")

    return {}
