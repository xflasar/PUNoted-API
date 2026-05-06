import logging
from typing import Any, Dict

from helpers.db import _upsert_records

logger = logging.getLogger(__name__)


async def handle_workforce_data_message(db: Any, raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles incoming 'workforces' data messages, converts them, and synchronizes
    them with the database.
    """
    logger.debug("Starting processing batch of workforce data.")

    payload = raw_data.get("data", [])
    user_account_id = raw_data.get("userId")

    # 1. Validate User ID
    if not user_account_id:
        logger.error("Payload is missing 'userId'.")
        return {"success": False, "message": "Missing user ID in payload."}

    # 2. Find User ID
    try:
        user_response = await db.fetch_one(
            "SELECT accountid, userdataid FROM users WHERE accountid = $1;",
            user_account_id,
        )

        userid: Any = None
        if user_response and user_response.get("userdataid") is not None:
            # Use 'userdataid' if it exists
            userid = user_response.get("userdataid")
        elif user_response:
            # Otherwise, fallback to 'accountid'
            userid = user_response.get("accountid")
        else:
            logger.warning(f"User not found for account ID: {user_account_id}")
            return {"success": False, "message": "User not found."}

        logger.debug(f"Processing data for user ID: {userid}")

    except Exception as e:
        logger.error(f"Error finding user for account ID {user_account_id}: {e}", exc_info=True)
        raise

    needs = []

    # 3. Prepare Records for Upsert
    for record in payload:
        record["userid"] = userid
        needs.extend(record.get("needs", []))
        del record["needs"]

    # 4. Perform Bulk Upsert within a Transaction
    processed_count = 0
    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                await _upsert_records(
                    con=con,
                    table_name="workforces",
                    records=payload,
                    unique_fields=["workforceid"],
                )

                await _upsert_records(
                    con=con,
                    table_name="workforce_needs",
                    records=needs,
                    unique_fields=["workforceneedid"],
                )
                processed_count = len(payload)

    except Exception as e:
        logger.error(f"Error processing workforce data: {e}", exc_info=True)
        raise

    logger.debug(f"Successfully processed {processed_count} workforce records.")
    return {"success": True, "processed_count": processed_count}
