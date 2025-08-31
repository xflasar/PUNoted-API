import logging
import time
from typing import Any, Dict, List, Tuple


logger = logging.getLogger(__name__)

async def handle_accounting_currency_balance_data_message(db: Any, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.info("Starting processing accounting currency balance data.")

    # Extract 'data' which should contain the list of accounting records
    converted_data = raw_payload.get("data")
    if not converted_data:
        logger.info("No accounting records in payload for processing. Exiting.")
        return {"success": True, "message": "No accounting records to process."}

    # --- User ID Resolution ---
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
            logger.warning(f"User with xata_id {raw_payload['userId']} not found for accounting data processing.")
            return {"success": False, "message": "User not found."}
            
    except Exception as e:
        logger.error(f"Error resolving user ID for accounting data: {e}", exc_info=True)
        raise # Re-raise the exception to indicate a failure

    # --- Prepare Data for Bulk UPSERT ---
    # This list will hold tuples of data ready for the database operation
    records_for_upsert: List[Tuple] = []

    for record in converted_data:
        # Assign the resolved userid to the record if it's missing
        # This ensures all balance entries are linked to a user
        if not record.get('userid'): # Check if 'userid' field exists and is not null/empty
            record['userid'] = userid

        # Extract data according to the new schema and prepare for UPSERT
        # This needs to get reworked (data_converter.py should do this and prepare the data for insertion)
        try:
            # Extract nested 'bookBalance' and 'currencyBalance' fields
            records_for_upsert.append((
                record.get('userid'),
                record.get('category'),
                record.get('type'),
                record.get('number'),
                record.get('bookbalanceamount'),
                record.get('bookbalancecurrencycode'),
                record.get('balanceamount'),
                record.get('balancecurrencycode')
            ))
        except AttributeError as ae:
            # Log specific error if nested keys are missing or structure is unexpected
            logger.error(f"Data structure mismatch in accounting record: {record}. Error: {ae}", exc_info=True)
            continue 
        except Exception as ex:
            logger.error(f"Unexpected error preparing accounting record for UPSERT: {record}. Error: {ex}", exc_info=True)
            continue


    if not records_for_upsert:
        logger.info("No valid accounting records prepared for UPSERT. Exiting.")
        return {"success": True, "message": "No valid accounting records to process."}

    # --- Perform Bulk UPSERT Operation ---
    upsert_query = """
    INSERT INTO user_currency_accounts (
        userid,
        category,
        type,
        number,
        bookbalanceamount,
        bookbalancecurrencycode,
        balanceamount,
        balancecurrencycode
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8
    )
    ON CONFLICT (userid, category, type, number, balancecurrencycode) DO UPDATE SET
        bookbalanceamount = EXCLUDED.bookbalanceamount,
        bookbalancecurrencycode = EXCLUDED.bookbalancecurrencycode,
        balanceamount = EXCLUDED.balanceamount,
        balancecurrencycode = EXCLUDED.balancecurrencycode;
    """

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                try:
                    await con.executemany(upsert_query, records_for_upsert)
                    logger.info(f"Successfully UPSERTed {len(records_for_upsert)} accounting balance records.")
                except Exception as e:
                      # This will catch and log the specific database error
                    logger.error(f"Database error during UPSERT: {e}", exc_info=True)
                    # Re-raise the exception to trigger the transaction rollback
                    raise
        
        logger.info(f"Successfully UPSERTed {len(records_for_upsert)} accounting balance records.")

    except Exception as e:
        logger.error(f"Error during bulk UPSERT of accounting balance data: {e}", exc_info=True)
        raise

    end_time = time.perf_counter()
    logger.info(f"Processing accounting currency balance records took {end_time - start_time:.4f} seconds")
    
    return {
        "success": True, 
        "message": f"Processed {len(records_for_upsert)} accounting balance records successfully."
    }