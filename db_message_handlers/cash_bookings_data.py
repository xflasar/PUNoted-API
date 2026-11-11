import logging
import time
from typing import Any, Dict, List
from db import Database

logger = logging.getLogger(__name__)


async def handle_cash_bookings_data_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles incoming ACCOUNTING_CASH_BOOKINGS data messages.
    Performs append-only INSERT into user_cash_transactions table (ignoring duplicates via unique constraint).
    """
    start_time = time.perf_counter()
    logger.debug("Starting processing accounting cash bookings data.")

    converted_records = raw_payload.get("data")
    if not converted_records:
        return {"success": True, "message": "No cash bookings records to process."}

    user_account_id = raw_payload.get("userId")
    if not user_account_id:
        return {"success": False, "message": "Missing userId in payload."}

    try:
        user_response = await db.fetch_one(
            "SELECT accountid, userdataid FROM users WHERE accountid = $1;",
            user_account_id,
        )
        if user_response and user_response.get("userdataid") is not None:
            userid = str(user_response.get("userdataid"))
        elif user_response:
            userid = str(user_response.get("accountid"))
        else:
            return {"success": False, "message": "User not found."}

        # Prepare records for insertion
        records_to_insert = []
        for rec in converted_records:
            if rec.get("period") is not None and rec.get("number") is not None:
                records_to_insert.append(
                    (
                        userid,
                        rec.get("period"),
                        rec.get("number"),
                        rec.get("item", 0),
                        rec.get("type"),
                        rec.get("account_category"),
                        rec.get("account_type"),
                        rec.get("account"),
                        rec.get("debit", True),
                        rec.get("amount", 0.0),
                        rec.get("currency", "ICA"),
                        rec.get("book_balance"),
                        rec.get("timestamp"),
                    )
                )

        if not records_to_insert:
            return {"success": True, "message": "No valid cash bookings records to insert."}

        insert_query = """
            INSERT INTO user_cash_transactions (
                userid, period, number, item, type, account_category,
                account_type, account, debit, amount, currency, book_balance, timestamp
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (userid, period, number, item) DO UPDATE
            SET
                book_balance = EXCLUDED.book_balance,
                amount = EXCLUDED.amount;
        """

        async with db.pool.acquire() as con:
            async with con.transaction():
                await con.executemany(insert_query, records_to_insert)

        elapsed = time.perf_counter() - start_time
        logger.info(f"Processed {len(records_to_insert)} cash transactions for user {userid} in {elapsed:.4f}s")
        return {"success": True, "message": f"Processed {len(records_to_insert)} cash transactions."}

    except Exception as e:
        logger.error(f"Error processing cash bookings data: {e}", exc_info=True)
        raise
