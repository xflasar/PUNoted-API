import datetime
import logging
import time
from typing import Any, Dict, List

import asyncpg

from db import Database
from managers.global_ws_manager import global_ws_manager

logger = logging.getLogger(__name__)


async def handle_comex_orders_data_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.debug("Starting processing comex orders data.")

    converted_data = raw_payload.get("data")
    if not converted_data:
        logger.debug("No comex orders records in payload. Exiting.")
        return {"success": True, "message": "No comex orders records to process."}

    try:
        # Get User details
        user_response = await db.fetch_one(
            "SELECT accountid, userdataid FROM users WHERE accountid = $1;",
            raw_payload["userId"],
        )

        # Determine internal ID for DB storage
        if user_response and user_response.get("userdataid") is not None:
            db_userid = user_response.get("userdataid")
        elif user_response:
            db_userid = user_response.get("accountid")
        else:
            return {"success": False, "message": "User not found."}

        # --- Step 1: Collect and transform all records ---
        comex_orders_to_upsert = []
        comex_orders_trades_to_upsert = []

        for record in converted_data:
            order_id = record.get("orderid")
            if not order_id:
                continue

            record["userid"] = db_userid  # Use internal ID for DB

            if "trades" in record:
                trades = record.pop("trades")
                for trade in trades:
                    trade["orderid"] = order_id
                    comex_orders_trades_to_upsert.append(trade)

            comex_orders_to_upsert.append(record)

        # --- Step 2: Perform all upserts in a single transaction ---
        incoming_order_ids = [r.get("orderid") for r in comex_orders_to_upsert if r.get("orderid")]
        just_fulfilled_ids = []

        async with db.pool.acquire() as con:
            existing_rows = await con.fetch(
                "SELECT orderid, status, amount FROM comex_trade_orders WHERE orderid = ANY($1);",
                incoming_order_ids
            )
            existing_map = {r["orderid"]: r for r in existing_rows}

            for r in comex_orders_to_upsert:
                oid = r.get("orderid")
                if not oid:
                    continue
                new_status = r.get("status")
                new_amount = r.get("amount")
                is_fulfilled = (new_status == "FULFILLED" or new_amount == 0)

                if is_fulfilled:
                    if oid in existing_map:
                        prev = existing_map[oid]
                        prev_status = prev.get("status")
                        prev_amount = prev.get("amount")
                        if prev_status != "FULFILLED" and (prev_amount is None or prev_amount > 0):
                            just_fulfilled_ids.append(oid)
                    else:
                        created_ts = r.get("created")
                        if created_ts and hasattr(created_ts, "timestamp"):
                            now_ts = datetime.datetime.now(datetime.timezone.utc).timestamp()
                            c_ts = created_ts.timestamp() if created_ts.tzinfo else created_ts.replace(tzinfo=datetime.timezone.utc).timestamp()
                            if (now_ts - c_ts) < 900:
                                just_fulfilled_ids.append(oid)

            async with con.transaction():
                # Upsert the main comex orders table
                await _upsert_records(con, "comex_trade_orders", comex_orders_to_upsert, ["orderid"])

                # Upsert child table
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
        # We use raw_payload["userId"] (Account ID) because that matches the WS connection ID
        target_user_account = raw_payload["userId"]

        await global_ws_manager.send_personal_message(target_user_account, {"type": "REFRESH_DASHBOARD"})
        logger.debug(f"Triggered dashboard update for user {target_user_account}")

    except Exception as e:
        # Don't fail the whole request if WS notification fails
        logger.error(f"Failed to trigger dashboard update: {e}")

    # --- Step 4: Evaluate Notifications ---
    try:
        from services.notification_evaluator import evaluate_user_telemetry_notifications
        await evaluate_user_telemetry_notifications(db.pool, raw_payload["userId"], target_order_ids=just_fulfilled_ids)
    except Exception as e:
        logger.error(f"Failed triggering notifications for comex orders batch: {e}")

    end_time = time.perf_counter()
    logger.debug(f"Processing comex orders records took {end_time - start_time:.4f} seconds")

    return {
        "success": True,
        "message": f"Processed {len(converted_data)} comex orders records successfully.",
    }


async def _upsert_records(
    con: asyncpg.Connection,
    table_name: str,
    records: List[Dict[str, Any]],
    unique_fields: List[str],
    chunk_size: int = 100,
):
    """
    A generic helper function to perform bulk upserts using INSERT ... ON CONFLICT DO UPDATE.
    """
    if not records:
        return

    columns = records[0].keys()
    columns_str = ", ".join(columns)
    unique_columns_str = ", ".join(unique_fields)

    set_clauses = [f"{col} = EXCLUDED.{col}" for col in columns if col not in unique_fields]
    set_clause_str = ", ".join(set_clauses)

    values_placeholders = ", ".join([f"${i + 1}" for i in range(len(columns))])

    if set_clause_str:
        on_conflict_clause = f"ON CONFLICT ({unique_columns_str}) DO UPDATE SET {set_clause_str}"
    else:
        on_conflict_clause = f"ON CONFLICT ({unique_columns_str}) DO NOTHING"

    query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_placeholders}) {on_conflict_clause};"

    for i in range(0, len(records), chunk_size):
        chunk = records[i : i + chunk_size]
        values_to_insert = [list(rec.values()) for rec in chunk]
        try:
            await con.executemany(query, values_to_insert)
        except Exception as e:
            logger.error(f"Database error during UPSERT: {e}", exc_info=True)
            raise
