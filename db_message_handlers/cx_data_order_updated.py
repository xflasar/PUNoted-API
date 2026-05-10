import logging
import time
from typing import Any, Dict

from db import Database
from helpers.db import _upsert_records
from managers.global_ws_manager import global_ws_manager

logger = logging.getLogger(__name__)


async def handle_comex_order_updated_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.debug("Starting processing comex order updated data.")

    record = raw_payload.get("data")
    if not record:
        logger.debug("No comex order updated record in payload. Exiting.")
        return {"success": True, "message": "No comex order record to process."}

    try:
        user_response = await db.fetch_one(
            "SELECT accountid, userdataid FROM users WHERE accountid = $1;",
            raw_payload["userId"],
        )
        if user_response and user_response.get("userdataid") is not None:
            userid = user_response.get("userdataid")
        elif user_response:
            userid = user_response.get("accountid")
        else:
            return {"success": False, "message": "User not found."}

        # --- Step 1: Collect and transform the single record ---
        comex_orders_to_upsert = []
        comex_orders_trades_to_upsert = []

        order_id = record.get("orderid")

        if order_id:
            record["userid"] = userid

            if "trades" in record:
                trades = record.pop("trades")
                if isinstance(trades, list):
                    for trade in trades:
                        trade["orderid"] = order_id
                        comex_orders_trades_to_upsert.append(trade)

            comex_orders_to_upsert.append(record)
        else:
            logger.warning("Received comex order without orderid.")
            return {"success": False, "message": "Record missing orderid."}

        # --- Step 2: Perform all upserts in a single transaction ---
        async with db.pool.acquire() as con:
            async with con.transaction():
                if comex_orders_to_upsert:
                    await _upsert_records(con, "comex_trade_orders", comex_orders_to_upsert, ["orderid"])

                if comex_orders_trades_to_upsert:
                    await _upsert_records(
                        con,
                        "comex_trade_orders_trades",
                        comex_orders_trades_to_upsert,
                        ["tradeid"],
                    )
    except Exception as e:
        logger.error(f"Error processing comex orders data: {e}", exc_info=True)
        raise

    # --- Step 3: Trigger WebSocket Update ---
    try:
        await global_ws_manager.send_personal_message(raw_payload["userId"], {"type": "REFRESH_DASHBOARD"})
        logger.debug(f"Triggered dashboard update for user {userid}")
    except Exception as e:
        logger.error(f"Failed to trigger dashboard update: {e}")

    end_time = time.perf_counter()
    logger.debug(f"Processing comex order record took {end_time - start_time:.4f} seconds")

    return {"success": True, "message": "Processed comex order record successfully."}
