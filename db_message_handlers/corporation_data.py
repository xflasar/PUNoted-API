import logging
import time
from typing import Any, Dict, List

import asyncpg

from db import Database

logger = logging.getLogger(__name__)

# --- CONSTANTS ---

# Columns for the corporations table
CORP_KEYS = [
    "id",
    "name",
    "code",
    "countryid",
    "currencycode",
    "foundedtimestamp",
    "totalshares",
]

# UPSERT query for corporations (Conflict on primary key 'id')
CORP_UPSERT_QUERY = """
    INSERT INTO corporations (id, name, code, countryid, currencycode, foundedtimestamp, totalshares)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (id) DO UPDATE SET
        name = EXCLUDED.name,
        code = EXCLUDED.code,
        countryid = EXCLUDED.countryid,
        currencycode = EXCLUDED.currencycode,
        foundedtimestamp = EXCLUDED.foundedtimestamp,
        totalshares = EXCLUDED.totalshares;
"""

# SHAREHOLDER_KEYS must include 'userid' as the first element for the prepared statement
SHAREHOLDER_KEYS = [
    "userid",
    "corporationid",
    "companyid",
    "companycode",
    "companyname",
    "relativeshare",
    "shares",
]

# UPSERT query for corporation_shareholders (Columns expanded to include userid)
SHAREHOLDER_UPSERT_QUERY = """
    INSERT INTO corporation_shareholders (userid, corporationid, companyid, companycode, companyname, relativeshare, shares)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
    ON CONFLICT (corporationid, companyid) DO UPDATE SET
        userid = EXCLUDED.userid,
        companycode = EXCLUDED.companycode,
        companyname = EXCLUDED.companyname,
        relativeshare = EXCLUDED.relativeshare,
        shares = EXCLUDED.shares;
"""

# SQL to retrieve the company ID to user ID map
COMPANY_USER_MAP_QUERY = """
    SELECT companyid, userdataid
    FROM company_data
    WHERE userdataid IS NOT NULL;
"""

# NEW: Query to update the user's current corporationid in users_data
USER_DATA_CORP_UPDATE_QUERY = """
    UPDATE users_data
    SET corporationid = $1
    WHERE userid = $2;
"""

async def cleanup_old_shareholders(
    con: asyncpg.connection.Connection,
    corporation_id: str,
    latest_shareholder_records: List[Dict[str, Any]],
):
    """
    Deletes old shareholder records for a given corporation that were not present in the latest fetch.
    """

    # 1. Collect all valid company IDs (shareholders) from the current fetch to KEEP
    # These are the records that were just UPSERTED.
    valid_company_ids_to_keep = [
        rec.get("companyid") for rec in latest_shareholder_records if rec.get("companyid") is not None
    ]

    # 2. Dynamic query to delete records where companyid is NOT in the list of valid company IDs
    # This deletes records that belong to the corporation but were not in the new payload.
    DELETE_QUERY = """
        DELETE FROM corporation_shareholders
        WHERE 
            corporationid = $1  -- Scope deletion to the processed corporation
        AND 
            companyid NOT IN (SELECT UNNEST($2::text[]))
    """

    try:
        deleted_count = await con.execute(DELETE_QUERY, corporation_id, valid_company_ids_to_keep)
        count = int(deleted_count.split()[-1])
        logger.debug(f"Successfully deleted {count} old shareholder records for corporation {corporation_id}.")
    except Exception as e:
        logger.error(
            f"Error during shareholder cleanup for corp {corporation_id}: {e}",
            exc_info=True,
        )


async def handle_corporation_data_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.debug("Starting processing corporation data.")

    converted_data = raw_payload.get("data")

    if not converted_data:
        logger.debug("No corporation records in payload. Exiting.")
        return {"success": True, "message": "No corporation records to process."}

    # Data structure to hold all shareholder records from all corporations
    all_shareholder_records: List[Dict[str, Any]] = []
    corp_tuples: List[tuple] = []

    # Track the processed corp ID and the USER ID of the submitting user
    processed_corporation_id: str | None = None
    submitting_user_id: str | None = None

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                # --- 1. Retrieve the company ID -> User ID mapping from DB ---
                user_map_db_records = await con.fetch(COMPANY_USER_MAP_QUERY)
                company_to_user_map = {record["companyid"]: record["userdataid"] for record in user_map_db_records}
                logger.debug(f"Retrieved {len(company_to_user_map)} company-to-user mappings.")

                # --- 2. Prepare Corporation data and collect/enrich Shareholder data ---
                record = converted_data  # single corporation object

                # a. Prepare the tuple for the corporations table
                corp_tuple = tuple(record.get(key) for key in CORP_KEYS)
                corp_tuples.append(corp_tuple)

                # Store the ID for later cleanup
                processed_corporation_id = record.get("id")

                # b. Collect and enrich shareholder data
                shareholders = record.get("shareholders", [])

                for shareholder_record in shareholders:
                    # Add the corporation's ID
                    shareholder_record["corporationid"] = processed_corporation_id

                    shareholder_company_id = shareholder_record.get("companyid")

                    # --- LINKAGE STEP: Use map to set 'userid' ---
                    user_id = company_to_user_map.get(shareholder_company_id)
                    shareholder_record["userid"] = user_id

                    # Assume the user who submitted the data is the first one found in the map
                    if submitting_user_id is None and user_id is not None:
                        submitting_user_id = user_id

                    all_shareholder_records.append(shareholder_record)

                # --- 3. Prepare final tuples for Shareholder UPSERT ---
                shareholder_tuples: List[tuple] = [
                    tuple(rec.get(key) for key in SHAREHOLDER_KEYS) for rec in all_shareholder_records
                ]

                # --- 4. Perform Bulk UPSERT Operations ---

                # UPSERT Corporations
                if corp_tuples:
                    await con.executemany(CORP_UPSERT_QUERY, corp_tuples)
                    logger.debug(f"Successfully UPSERTED {len(corp_tuples)} corporations.")

                # UPSERT Corporation Shareholders
                if shareholder_tuples:
                    await con.executemany(SHAREHOLDER_UPSERT_QUERY, shareholder_tuples)
                    logger.debug(f"Successfully UPSERTED {len(shareholder_tuples)} corporation shareholders.")

                # a. Cleanup: Delete old shareholder records (if a corporation was processed)
                if processed_corporation_id:
                    await cleanup_old_shareholders(con, processed_corporation_id, all_shareholder_records)

                # b. Update the user's primary corporation ID (if we identified the user)
                if submitting_user_id and processed_corporation_id:
                    await con.execute(
                        USER_DATA_CORP_UPDATE_QUERY,
                        processed_corporation_id,
                        submitting_user_id,
                    )
                    logger.debug(
                        f"Updated user {submitting_user_id} with new corporation ID {processed_corporation_id}."
                    )

    except Exception as e:
        logger.error(f"Error processing corporation data: {e}", exc_info=True)
        raise

    end_time = time.perf_counter()
    logger.debug(f"Processing corporation records took {end_time - start_time:.4f} seconds")

    return {
        "success": True,
        "message": f"Processed 1 corporation and {len(all_shareholder_records)} shareholders successfully.",
    }
