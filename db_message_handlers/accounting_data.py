import logging
import time
from typing import Any, Dict, List, Tuple

from db import Database

logger = logging.getLogger(__name__)

async def handle_accounting_data_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.info("Starting processing accounting data.")

    converted_data = raw_payload.get("data")
    if not converted_data:
        logger.info("No accounting records in payload. Exiting.")
        return {"success": True, "message": "No accounting records to process."}
    
    userid = None
    try:
        user_response = await db.fetch_one(
            "SELECT xata_id, userdataid FROM users WHERE xata_id = $1;",
            raw_payload["userId"]
        )
        if user_response and user_response.get('userdataid') is not None:
            userid = user_response.get('userdataid')
        elif user_response:
            userid = user_response.get('xata_id')
        else:
            return {"success": False, "message": "User not found."}
    except Exception as e:
        logger.error(f"Error processing accounting data: {e}", exc_info=True)
        raise

    # --- Prepare Data for Bulk UPSERT ---
    records_for_upsert: List[Tuple] = []
    
    for record in converted_data:
        try:
            records_for_upsert.append((
                userid,
                record.get('number'),
                record.get('bookbalanceamount'),
                record.get('balanceamount')
            ))
        except AttributeError as ae:
            logger.error(f"Data structure mismatch in accounting record: {record}. Error: {ae}", exc_info=True)
            continue
        except Exception as ex:
            logger.error(f"Unexpected error preparing accounting record for UPSERT: {record}. Error: {ex}", exc_info=True)
            continue

    if not records_for_upsert:
        logger.info("No valid accounting records prepared for UPSERT. Exiting.")
        return {"success": True, "message": "No valid accounting records to process."}

    # The update query
    update_query = """
    UPDATE user_currency_accounts
    SET
        bookbalanceamount = $3,
        balanceamount = $4
    WHERE
        userid = $1 AND number = $2;
    """

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                await con.executemany(update_query, records_for_upsert)

                logger.info(f"Attempted to UPDATE {len(records_for_upsert)} accounting balance records.")
    
    except Exception as e:
        logger.error(f"Database error during UPDATE: {e}", exc_info=True)
        raise

    end_time = time.perf_counter()
    logger.info(f"Processing accounting currency balance records took {end_time - start_time:.4f} seconds")
    
    return {
        "success": True, 
        "message": f"Processed {len(records_for_upsert)} accounting balance records successfully."
    }