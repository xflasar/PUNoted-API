import logging
import time
from typing import Any, Dict, List, Tuple

from db import Database

logger = logging.getLogger(__name__)


async def handle_accounting_data_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.debug("Starting processing accounting data.")

    converted_data = raw_payload.get("data")
    if not converted_data:
        logger.debug("No accounting records in payload. Exiting.")
        return {"success": True, "message": "No accounting records to process."}

    userid = None
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
    except Exception as e:
        logger.error(f"Error processing accounting data: {e}", exc_info=True)
        raise

    # --- Check Extension Context / Entity / Address Target ---
    msg_context = raw_payload.get("context")
    if msg_context and str(msg_context).upper() in ("GOVERNMENT", "ADMINCENTER"):
        logger.info(f"Skipping government/admincenter context accounting message for user {raw_payload.get('userId')}")
        return {"success": True, "message": "Ignored government/admincenter accounting record."}

    user_company = await db.fetch_one(
        "SELECT companyid FROM company_data WHERE userdataid = $1;",
        userid,
    )
    user_company_id = user_company.get("companyid") if user_company else None

    first_record = converted_data[0] if converted_data else {}
    record_address = first_record.get("address")
    if record_address and isinstance(record_address, dict):
        lines = record_address.get("lines", [])
        if lines:
            first_line = lines[0] if isinstance(lines[0], dict) else {}
            entity = first_line.get("entity", {})
            entity_type = entity.get("type")
            entity_id = entity.get("id")

            if entity_type == "GOVERNMENT":
                logger.info(f"Skipping government accounting message for user {raw_payload.get('userId')}")
                return {"success": True, "message": "Ignored government accounting record."}

            if user_company_id and entity_id and entity_id != user_company_id:
                logger.info(f"Skipping non-user accounting message (entity {entity_id}) for user {raw_payload.get('userId')}")
                return {"success": True, "message": "Ignored non-user accounting record."}

    # --- Prepare Data for Bulk UPSERT ---
    records_for_upsert: List[Tuple] = []

    for record in converted_data:
        try:
            records_for_upsert.append(
                (
                    userid,
                    record.get("number"),
                    record.get("bookbalanceamount"),
                    record.get("balanceamount"),
                )
            )
        except AttributeError as ae:
            logger.error(
                f"Data structure mismatch in accounting record: {record}. Error: {ae}",
                exc_info=True,
            )
            continue
        except Exception as ex:
            logger.error(
                f"Unexpected error preparing accounting record for UPSERT: {record}. Error: {ex}",
                exc_info=True,
            )
            continue

    if not records_for_upsert:
        logger.debug("No valid accounting records prepared for UPSERT. Exiting.")
        return {"success": True, "message": "No valid accounting records to process."}

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
                try:
                    await con.executemany(update_query, records_for_upsert)
                    logger.debug(f"Attempted to UPDATE {len(records_for_upsert)} accounting balance records.")
                except Exception as e:
                    logger.error(f"Database error during UPSERT: {e}", exc_info=True)
                    raise

    except Exception as e:
        logger.error(f"Database error during UPDATE: {e}", exc_info=True)
        raise

    end_time = time.perf_counter()
    logger.debug(f"Processing accounting currency balance records took {end_time - start_time:.4f} seconds")

    return {
        "success": True,
        "message": f"Processed {len(records_for_upsert)} accounting balance records successfully.",
    }
