import logging
import time
from typing import Any, Dict

from db import Database

logger = logging.getLogger(__name__)

USER_DATA_CORP_CLEAR_QUERY = """
    UPDATE users_data
    SET corporationid = NULL
    WHERE userid = $1;
"""

USER_DATA_CORP_UPDATE_QUERY = """
    UPDATE users_data
    SET corporationid = $1
    WHERE userid = $2;
"""


async def handle_corporation_shareholder_holdings_data_message(
    db: Database, raw_payload: Dict[str, Any]
) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.debug("Starting processing corporation data.")

    converted_data = raw_payload.get("data")

    if not converted_data:
        logger.debug("No corporation records in payload. Exiting.")
        return {"success": True, "message": "No corporation records to process."}

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

        if converted_data is None:
            async with db.pool.acquire() as con:
                # 1. Delete all shareholder records associated with this user
                await con.execute("DELETE FROM corporation_shareholders WHERE companyid = $1", userid)
                logger.debug(f"Deleted old shareholder records for user {userid} (no corp data).")

                # 2. Update the user's main data record to clear the corporationid
                await con.execute(USER_DATA_CORP_CLEAR_QUERY, userid)
                logger.debug(f"Cleared corporation ID for user {userid} in users_data.")

            return {
                "success": True,
                "message": f"User {userid} has no corporation data; cleaned up records.",
            }

        async with db.pool.acquire() as con:
            if converted_data["corporationid"]:
                await con.execute(USER_DATA_CORP_UPDATE_QUERY, converted_data["corporationid"], userid)
                logger.debug(f"Updated user {userid} to corporation ID {converted_data['corporationid']}.")

    except Exception:
        raise

    end_time = time.perf_counter()
    logger.debug(f"Processing corporation records took {end_time - start_time:.4f} seconds")

    return {"success": True, "message": "Processed user corporation successfully."}
