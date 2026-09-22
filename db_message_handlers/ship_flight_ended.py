from managers.global_ws_manager import global_ws_manager
import datetime
import logging
from typing import Any, Dict

from db import Database

logger = logging.getLogger(__name__)

async def handle_ship_flight_ended_message(db: Database, converted_data: Dict[str, Any], testing: bool) -> Dict[str, Any]:
    logger.debug("Processing ship flight ended message.")
    try:
        flight_ended_record = converted_data.get("data")
        if not flight_ended_record:
            return {"success": False, "message": "No flight ended data found."}

        user_response = await db.fetch_one(
            "SELECT accountid, userdataid FROM users WHERE accountid = $1;",
            converted_data.get("userId"),
        )
        if user_response and user_response.get("userdataid") is not None:
            userid = user_response.get("userdataid")
        elif user_response:
            userid = user_response.get("accountid")
        else:
            return {"success": False, "message": "User not found."}

        # Will need to go thru db and replace accountid with userdataid
        if not testing:
            async with db.pool.acquire() as con:
                async with con.transaction():
                    await con.execute("""
                                      INSERT INTO notifications (accountid, type, message, created_at)
                                      VALUES ($1, $2, $3, $4);
                                      """, userid, "flight_ended", f"Flight {flight_ended_record.get('id')} has ended.", datetime.datetime.utcnow())
        try:
            ws_update = {
                flight_ended_record["id"],
                flight_ended_record["shipId"]
            }
            await global_ws_manager.send_personal_message(ws_update, {"type": "FLIGHT_ENDED"}) # Maybe change it into FLIGHT_UPDATE
            logger.debug("Triggered flight ended update for user {userid}")
        except Exception as e:
            logger.error(f"Failed to trigger flight ended update: {e}")

            await global_ws_manager.send_personal_message()
    except Exception as e:
        logger.error(f"Error processing ship flight ended message: {e}", exc_info=True)
        raise

    return {"success": True, "message": "Flight ended record processed successfully."}
