import logging
import time
from typing import Any, Dict, List
from managers.global_ws_manager import global_ws_manager
from services.internal.corp_site_delta_service import compute_and_broadcast_site_delta
from db_message_handlers.production_lines_data import process_orders, process_production_templates

logger = logging.getLogger(__name__)


async def handle_single_production_line_updated_message(db, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles PRODUCTION_PRODUCTION_LINE_UPDATED message for a single production line.
    Performs UPSERT of the single line, updates its orders and production templates,
    and broadcasts a delta update via WebSockets.
    """
    start_time = time.perf_counter()
    converted_data = raw_payload.get("data", {}) if isinstance(raw_payload, dict) else {}

    site_id = converted_data.get("siteid")
    line = converted_data.get("production_line", {})

    if not site_id:
        logger.warning(f"[WARNING] Missing 'siteid' in PRODUCTION_PRODUCTION_LINE_UPDATED payload: {list(converted_data.keys())}")
        return {"success": False, "message": "Missing siteid"}

    if not line or not line.get("productionlineid"):
        logger.warning(f"[WARNING] Missing 'production_line' or 'productionlineid' for site {site_id}")
        return {"success": False, "message": "Missing productionlineid"}

    p_id = line.get("productionlineid")
    orders = line.get("orders") or []
    production_templates = line.get("production_templates") or []

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                # Lock site row to serialize with full site production lines updates
                await con.execute("SELECT siteid FROM sites WHERE siteid = $1 FOR UPDATE;", site_id)

                line_record = {
                    "productionlineid": p_id,
                    "siteid": site_id,
                    "type": line.get("type"),
                    "capacity": line.get("capacity"),
                    "slots": line.get("slots"),
                    "efficiency": line.get("efficiency"),
                    "condition": line.get("condition"),
                }

                line_keys = list(line_record.keys())
                keys_sql = ", ".join(line_keys)
                placeholders = ", ".join([f"${i + 1}" for i in range(len(line_keys))])
                update_clause = ", ".join([f"{k} = EXCLUDED.{k}" for k in line_keys if k != "productionlineid"])

                upsert_query = f"""
                    INSERT INTO site_production_lines ({keys_sql})
                    VALUES ({placeholders})
                    ON CONFLICT (productionlineid) DO UPDATE
                    SET {update_clause};
                """
                await con.execute(upsert_query, *[line_record[k] for k in line_keys])

                # Process child orders & templates strictly for this line
                await process_orders(con, site_id, orders, {p_id})
                await process_production_templates(con, site_id, production_templates)

        elapsed = time.perf_counter() - start_time
        if elapsed > 1.0:
            logger.warning(f"[WARNING] Slow line update! Site {site_id} line {p_id} took {elapsed:.2f}s")

        # Broadcast real-time WebSocket event
        user_id = raw_payload.get("userId") or raw_payload.get("data", {}).get("userid")
        if user_id and site_id:
            try:
                await global_ws_manager.send_personal_message(user_id, {"type": "REFRESH_DASHBOARD", "siteid": site_id})
                await compute_and_broadcast_site_delta(db, global_ws_manager, site_id, user_id)
            except Exception as ws_err:
                logger.error(f"[WEBSOCKET ERROR] Failed to send single production line delta update: {ws_err}")

        return {"success": True, "message": f"Updated production line {p_id}"}

    except Exception as e:
        logger.error(f"[ERROR] Failed to handle single production line update {p_id} for site {site_id}: {e}", exc_info=True)
        raise
