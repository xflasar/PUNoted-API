import datetime
import logging
from typing import Any, Dict

from db import Database

logger = logging.getLogger(__name__)

async def handle_ship_flight_ended_message(db: Database, converted_data: Dict[str, Any]) -> Dict[str, Any]:
    logger.debug("Processing ship flight ended message.")
    try:
        flight_ended_record = converted_data.get("data")
        if not flight_ended_record:
            return {"success": False, "message": "No flight ended data found."}

        user_account_id = converted_data.get("userId")
        if not user_account_id:
            return {"success": False, "message": "User ID missing in payload."}

        async with db.pool.acquire() as con:
            async with con.transaction():
                await con.execute("""
                                  INSERT INTO notifications (accountid, type, message, created_at)
                                  VALUES ($1, $2, $3, $4);
                                  """, user_account_id, "flight_ended", f"Flight {flight_ended_record.get('id')} has ended.", datetime.datetime.utcnow())

    except Exception as e:
        logger.error(f"Error processing ship flight ended message: {e}", exc_info=True)
        raise

    return {"success": True, "message": "Flight ended record processed successfully."}
