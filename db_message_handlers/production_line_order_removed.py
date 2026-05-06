import logging
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)


async def handle_production_line_order_remove_message(db, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.debug("Starting processing production line order removed data.")

    converted_data = raw_payload.get("data", {})
    production_line_id = converted_data.get("productionlineid")
    order_id = converted_data.get("orderid")

    # Basic input validation
    if not production_line_id or not order_id:
        logger.warning(f"Missing required IDs in payload: productionlineid={production_line_id}, orderid={order_id}")
        return {"error": "Missing required productionlineid or orderid"}

    rows_deleted = 0

    try:
        async with db.pool.acquire() as conn:
            query = """
                DELETE FROM site_production_line_orders 
                WHERE orderid = $1 AND productionlineid = $2;
            """

            status = await conn.execute(query, order_id, production_line_id)

            # Extract the number of deleted rows from the status string
            if status.startswith("DELETE"):
                try:
                    rows_deleted = int(status.split()[-1])
                except ValueError:
                    rows_deleted = 0

    except Exception as e:
        end_time = time.perf_counter()
        elapsed_time = (end_time - start_time) * 1000
        logger.error(
            f"Failed to remove order {order_id} from line {production_line_id}. "
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

    return {}
