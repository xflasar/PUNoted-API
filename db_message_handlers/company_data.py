import logging
import time
import uuid
from typing import Any, Dict, List

import asyncpg

logger = logging.getLogger(__name__)


def convert_uuids_to_strings(data: Dict[str, Any]) -> Dict[str, Any]:
    """Converts uuid.UUID objects in a dictionary to their string representation."""
    cleaned_data = {}
    for k, v in data.items():
        if isinstance(v, uuid.UUID):
            cleaned_data[k] = str(v)
        else:
            cleaned_data[k] = v
    return cleaned_data


"""
    Needs rewriting to use transaction
"""


async def handle_company_data_message(conn, converted_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Asynchronously handles inserting/updating company data and its nested components
    using a single transaction and bulk operations.
    """

    start_time = time.perf_counter()
    company_data_table = "company_data"
    userid = converted_data["userId"]
    payload_data = converted_data["data"]

    # Extract data for each table from the converted payload
    company_data_record = payload_data.get("company_data", {})
    representation_record = payload_data.get("representation", {})
    representation_contributors = payload_data.get("representationContributors", [])
    rating_report_record = payload_data.get("ratingReport", {})
    headquarters_record = payload_data.get("headquarters", {})
    headquarters_upgrade_items = payload_data.get("headquartersUpgradeItems", [])
    headquarters_efficiency_gains = payload_data.get("headquarters_efficiency_gains", [])
    headquarters_efficiency_gains_next_level = payload_data.get("headquarters_efficiency_gains_next_level", [])

    company_id = company_data_record.get("companyid")
    if not company_id:
        return {"success": False, "message": "Company ID is missing from payload."}

    userdataid_record = await conn.fetch_one("SELECT userdataid FROM users WHERE accountid = $1;", userid)
    userdataid = str(userdataid_record["userdataid"])

    update_shareholder_query = """
        UPDATE corporation_shareholders
        SET userid = $1
        WHERE companyid = $2 
          -- Optional condition: only update if userid is null or incorrect
          -- AND (userid IS NULL OR userid != $1); 
    """
    try:
        await conn.execute(update_shareholder_query, userdataid, company_id)
        logger.debug(
            f"Automated update: corporation_shareholders set for company {company_id} with userdataid {userdataid}."
        )
    except Exception as e:
        logger.error(f"Failed to automate corporation_shareholders update for company {company_id}: {e}")

    # Use a single transaction for all operations for this company (Insert needs to have it too)
    try:
        headquarters_id_to_link = None
        representation_id_to_link = None
        rating_report_id_to_link = None

        # Check if the main company_data record already exists
        existing_company_record = await conn.fetch_one(
            f"SELECT companyid, headquartersid, representationid, ratingreportid FROM {company_data_table} WHERE companyid = $1;",
            company_id,
        )

        if existing_company_record:
            # --- UPDATE EXISTING COMPANY ---
            logger.debug(f"Company '{company_id}' already exists. Updating.")
            await handle_company_update_transactional(
                conn,
                existing_company_record,
                company_data_record,
                company_id,
                company_data_table,
                headquarters_record,
                representation_record,
                rating_report_record,
                headquarters_upgrade_items,
                headquarters_efficiency_gains,
                headquarters_efficiency_gains_next_level,
                representation_contributors,
            )
        else:
            # --- INSERT NEW COMPANY ---
            logger.debug(f"Company '{company_id}' does not exist. Inserting new company and associated data.")

            linked_ids = await insert_new_company(
                conn,
                representation_record,
                representation_contributors,
                rating_report_record,
                headquarters_record,
                headquarters_upgrade_items,
                headquarters_efficiency_gains,
                headquarters_efficiency_gains_next_level,
            )

            headquarters_id_to_link = linked_ids["headquartersid"]
            representation_id_to_link = linked_ids["representationid"]
            rating_report_id_to_link = linked_ids["ratingreportid"]

            # Now insert the main company_data record, linking to the newly created IDs
            company_data_record["headquartersid"] = headquarters_id_to_link
            company_data_record["representationid"] = representation_id_to_link
            company_data_record["ratingreportid"] = rating_report_id_to_link
            company_data_record["userdataid"] = userdataid  # Link to user

            company_keys = ", ".join(company_data_record.keys())
            company_values = ", ".join([f"${i + 1}" for i in range(len(company_data_record))])
            try:
                await conn.execute(
                    f"INSERT INTO {company_data_table} ({company_keys}) VALUES ({company_values});",
                    *company_data_record.values(),
                )
            except Exception as e:
                logger.error(f"Database error during UPSERT: {e}", exc_info=True)
                raise

        end_time = time.perf_counter()
        logger.debug(f"Finished processing company data in {end_time - start_time:.2f} seconds.")
        return {
            "success": True,
            "message": f"Transaction for company '{company_id}' completed.",
        }

    except Exception as e:
        logger.error(f"Transaction failed for company '{company_id}': {e}", exc_info=True)
        raise


async def safe_update_record(conn, table_name, update_data, key_field, key_value):
    """
    Safely updates a single record using parameterized queries.
    """
    if not update_data:
        return

    update_data = convert_uuids_to_strings(update_data)

    set_clauses = [f"{key} = ${i + 2}" for i, key in enumerate(update_data.keys())]
    set_clause = ", ".join(set_clauses)

    update_query = f"UPDATE {table_name} SET {set_clause} WHERE {key_field} = $1;"

    await conn.execute(update_query, key_value, *update_data.values())


async def handle_company_update_transactional(
    conn,
    existing_company_record,
    company_data_record,
    company_id,
    company_data_table,
    headquarters_record,
    representation_record,
    rating_report_record,
    headquarters_upgrade_items,
    headquarters_efficiency_gains,
    headquarters_efficiency_gains_next_level,
    representation_contributors,
):
    try:
        async with conn.pool.acquire() as con:
            async with con.transaction():
                headquarters_id_to_link = None
                representation_id_to_link = None
                rating_report_id_to_link = None

                if existing_company_record:
                    # --- UPDATE EXISTING COMPANY ---
                    logger.debug(f"Company '{company_id}' already exists. Updating.")

                    headquarters_id_to_link = existing_company_record["headquartersid"]
                    representation_id_to_link = existing_company_record["representationid"]
                    rating_report_id_to_link = existing_company_record["ratingreportid"]

                    # Update main linked records (safe_update_record handles UUID conversion)
                    if headquarters_record and headquarters_id_to_link:
                        await safe_update_record(
                            conn,
                            "headquarters",
                            headquarters_record,
                            "headquartersid",
                            headquarters_id_to_link,
                        )

                    if representation_record and representation_id_to_link:
                        await safe_update_record(
                            conn,
                            "representation",
                            representation_record,
                            "representationid",
                            representation_id_to_link,
                        )

                    if rating_report_record and rating_report_id_to_link:
                        await safe_update_record(
                            conn,
                            "rating_reports",
                            rating_report_record,
                            "ratingreportid",
                            rating_report_id_to_link,
                        )

                    # Delete existing nested items
                    tables_to_clear_by_hq = [
                        "headquarters_upgrade_items",
                        "efficiency_gains",
                        "efficiency_gains_next_level",
                    ]
                    for table in tables_to_clear_by_hq:
                        try:
                            await conn.execute(
                                f"DELETE FROM {table} WHERE headquartersid = $1;",
                                headquarters_id_to_link,
                            )
                        except Exception as e:
                            logger.error(f"Database error during UPSERT: {e}", exc_info=True)
                            raise
                    try:
                        await conn.execute(
                            "DELETE FROM representation_contributors WHERE representationid = $1;",
                            representation_id_to_link,
                        )
                    except Exception as e:
                        logger.error(f"Database error during UPSERT: {e}", exc_info=True)
                        raise
                    logger.debug("Deleted old nested items in preparation for new ones.")

                    # Insert new nested items (bulk insert)
                    if headquarters_upgrade_items:
                        items_to_insert = [
                            convert_uuids_to_strings({**item, "headquartersid": headquarters_id_to_link})
                            for item in headquarters_upgrade_items
                        ]
                        if items_to_insert:
                            keys = ", ".join(items_to_insert[0].keys())
                            values_placeholders = ", ".join([f"${i + 1}" for i in range(len(items_to_insert[0]))])
                            try:
                                await conn.executemany(
                                    f"INSERT INTO headquarters_upgrade_items ({keys}) VALUES ({values_placeholders});",
                                    [list(rec.values()) for rec in items_to_insert],
                                )
                            except Exception as e:
                                logger.error(f"Database error during UPSERT: {e}", exc_info=True)
                                raise
                            logger.debug(f"Bulk inserted {len(items_to_insert)} headquarters_upgrade_items.")

                    if headquarters_efficiency_gains:
                        gains_to_insert = [
                            convert_uuids_to_strings({**gain, "headquartersid": headquarters_id_to_link})
                            for gain in headquarters_efficiency_gains
                        ]
                        if gains_to_insert:
                            keys = ", ".join(gains_to_insert[0].keys())
                            values_placeholders = ", ".join([f"${i + 1}" for i in range(len(gains_to_insert[0]))])
                            try:
                                await conn.executemany(
                                    f"INSERT INTO efficiency_gains ({keys}) VALUES ({values_placeholders});",
                                    [list(rec.values()) for rec in gains_to_insert],
                                )
                            except Exception as e:
                                logger.error(f"Database error during UPSERT: {e}", exc_info=True)
                                raise
                            logger.debug(f"Bulk inserted {len(gains_to_insert)} efficiency_gains.")

                    if headquarters_efficiency_gains_next_level:
                        next_level_gains_to_insert = [
                            convert_uuids_to_strings({**gain, "headquartersid": headquarters_id_to_link})
                            for gain in headquarters_efficiency_gains_next_level
                        ]
                        if next_level_gains_to_insert:
                            keys = ", ".join(next_level_gains_to_insert[0].keys())
                            values_placeholders = ", ".join(
                                [f"${i + 1}" for i in range(len(next_level_gains_to_insert[0]))]
                            )
                            try:
                                await conn.executemany(
                                    f"INSERT INTO efficiency_gains_next_level ({keys}) VALUES ({values_placeholders});",
                                    [list(rec.values()) for rec in next_level_gains_to_insert],
                                )
                            except Exception as e:
                                logger.error(f"Database error during UPSERT: {e}", exc_info=True)
                                raise
                            logger.debug(f"Bulk inserted {len(next_level_gains_to_insert)} efficiency_gains_next_level.")

                    if representation_contributors:
                        contribs_to_insert = [
                            convert_uuids_to_strings(
                                {
                                    **contrib,
                                    "representationid": representation_id_to_link,
                                }
                            )
                            for contrib in representation_contributors
                        ]
                        if contribs_to_insert:
                            keys = ", ".join(contribs_to_insert[0].keys())
                            values_placeholders = ", ".join([f"${i + 1}" for i in range(len(contribs_to_insert[0]))])
                            try:
                                await conn.executemany(
                                    f"INSERT INTO representation_contributors ({keys}) VALUES ({values_placeholders});",
                                    [list(rec.values()) for rec in contribs_to_insert],
                                )
                            except Exception as e:
                                logger.error(f"Database error during UPSERT: {e}", exc_info=True)
                                raise
                            logger.debug(f"Bulk inserted {len(contribs_to_insert)} representation_contributors.")

                    # Update the main company_data record (safe_update_record handles UUID conversion)
                    company_data_record["headquartersid"] = headquarters_id_to_link
                    company_data_record["representationid"] = representation_id_to_link
                    company_data_record["ratingreportid"] = rating_report_id_to_link

                    company_data_record = convert_uuids_to_strings(company_data_record)

                    update_fields = ", ".join([f"{key} = ${i + 2}" for i, key in enumerate(company_data_record.keys())])
                    update_query = f"UPDATE {company_data_table} SET {update_fields} WHERE companyid = $1;"
                    try:
                        await conn.execute(
                            update_query,
                            existing_company_record["companyid"],
                            *company_data_record.values(),
                        )
                    except Exception as e:
                        logger.error(f"Database error during UPSERT: {e}", exc_info=True)
                        raise
                    logger.debug(f"Updated main company record for '{company_id}'.")

                else:
                    logger.warning(f"Company '{company_id}' not found. No update performed.")
    except Exception as e:
        logger.error(f"Transaction failed for company '{company_id}': {e}", exc_info=True)
        raise


async def insert_new_company(
    conn: asyncpg.Connection,
    representation_record: Dict[str, Any],
    representation_contributors: List[Dict[str, Any]],
    rating_report_record: Dict[str, Any],
    headquarters_record: Dict[str, Any],
    headquarters_upgrade_items: List[Dict[str, Any]],
    headquarters_efficiency_gains: List[Dict[str, Any]],
    headquarters_efficiency_gains_next_level: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Asynchronously inserts a new company and its associated nested data within an ongoing transaction.
    Returns the IDs of the main linked records (HQ, Representation, Rating Report).
    """

    # Phase 1: Insert lowest-level main records (Representation, Headquarters, Rating Report)

    representation_record = convert_uuids_to_strings(representation_record)
    headquarters_record = convert_uuids_to_strings(headquarters_record)
    rating_report_record = convert_uuids_to_strings(rating_report_record)

    # Representation
    rep_keys = ", ".join(representation_record.keys())
    rep_values = ", ".join([f"${i + 1}" for i in range(len(representation_record))])
    representation_id_result = await conn.fetch_one(
        f"INSERT INTO representation ({rep_keys}) VALUES ({rep_values}) RETURNING representationid;",
        *representation_record.values(),
    )
    representation_id = str(representation_id_result["representationid"])

    # Headquarters
    hq_keys = ", ".join(headquarters_record.keys())
    hq_values = ", ".join([f"${i + 1}" for i in range(len(headquarters_record))])
    headquarters_id_result = await conn.fetch_one(
        f"INSERT INTO headquarters ({hq_keys}) VALUES ({hq_values}) RETURNING headquartersid;",
        *headquarters_record.values(),
    )

    headquarters_id = str(headquarters_id_result["headquartersid"])

    # Rating Report
    rr_keys = ", ".join(rating_report_record.keys())
    rr_values = ", ".join([f"${i + 1}" for i in range(len(rating_report_record))])
    rating_report_id_result = await conn.fetch_one(
        f"INSERT INTO rating_reports ({rr_keys}) VALUES ({rr_values}) RETURNING ratingreportid;",
        *rating_report_record.values(),
    )
    rating_report_id = str(rating_report_id_result["ratingreportid"])

    if not all([representation_id, headquarters_id, rating_report_id]):
        raise Exception("Failed to insert core company linked records.")

    # Phase 2: Insert mid-level linked tables (contributors, upgrade items, efficiency gains)

    # Representation contributors
    if representation_contributors:
        contrib_records_to_insert = []
        for contributor in representation_contributors:
            contributor_copy = convert_uuids_to_strings(contributor.copy())
            contributor_copy["representationid"] = str(representation_id)  # Link to parent
            contrib_records_to_insert.append(contributor_copy)

        if contrib_records_to_insert:
            contrib_keys = ", ".join(contrib_records_to_insert[0].keys())
            contrib_values = ", ".join([f"${i + 1}" for i in range(len(contrib_records_to_insert[0]))])
            contrib_query = f"INSERT INTO representation_contributors ({contrib_keys}) VALUES ({contrib_values});"
            try:
                await conn.executemany(
                    contrib_query,
                    [list(rec.values()) for rec in contrib_records_to_insert],
                )
                logger.debug(f"Attempted to UPDATE {len(contrib_records_to_insert)} records.")
            except Exception as e:
                logger.error(f"Database error during UPSERT: {e}", exc_info=True)
                raise

    # Headquarters items
    if headquarters_upgrade_items:
        items_to_insert = []
        for item in headquarters_upgrade_items:
            item_copy = convert_uuids_to_strings(item.copy())
            item_copy["headquartersid"] = str(headquarters_id)  # Link to parent
            items_to_insert.append(item_copy)

        if items_to_insert:
            item_keys = ", ".join(items_to_insert[0].keys())
            item_values = ", ".join([f"${i + 1}" for i in range(len(items_to_insert[0]))])
            item_query = f"INSERT INTO headquarters_upgrade_items ({item_keys}) VALUES ({item_values});"
            try:
                await conn.executemany(item_query, [list(rec.values()) for rec in items_to_insert])
                logger.debug(f"Attempted to UPDATE {len(items_to_insert)} records.")
            except Exception as e:
                logger.error(f"Database error during UPSERT: {e}", exc_info=True)
                raise

    # Headquarters efficiency gains
    if headquarters_efficiency_gains:
        gains_to_insert = []
        for gain in headquarters_efficiency_gains:
            gain_copy = convert_uuids_to_strings(gain.copy())
            gain_copy["headquartersid"] = str(headquarters_id)  # Link to parent
            gains_to_insert.append(gain_copy)

        if gains_to_insert:
            gain_keys = ", ".join(gains_to_insert[0].keys())
            gain_values = ", ".join([f"${i + 1}" for i in range(len(gains_to_insert[0]))])
            gain_query = f"INSERT INTO efficiency_gains ({gain_keys}) VALUES ({gain_values});"
            try:
                await conn.executemany(gain_query, [list(rec.values()) for rec in gains_to_insert])
                logger.debug(f"Attempted to UPDATE {len(gains_to_insert)} records.")
            except Exception as e:
                logger.error(f"Database error during UPSERT: {e}", exc_info=True)
                raise

    # Headquarters efficiency gains next level
    if headquarters_efficiency_gains_next_level:
        next_level_gains_to_insert = []
        for gain in headquarters_efficiency_gains_next_level:
            gain_copy = convert_uuids_to_strings(gain.copy())
            gain_copy["headquartersid"] = str(headquarters_id)  # Link to parent
            next_level_gains_to_insert.append(gain_copy)

        if next_level_gains_to_insert:
            next_level_keys = ", ".join(next_level_gains_to_insert[0].keys())
            next_level_values = ", ".join([f"${i + 1}" for i in range(len(next_level_gains_to_insert[0]))])
            next_level_query = (
                f"INSERT INTO efficiency_gains_next_level ({next_level_keys}) VALUES ({next_level_values});"
            )
            try:
                await conn.executemany(
                    next_level_query,
                    [list(rec.values()) for rec in next_level_gains_to_insert],
                )
                logger.debug(f"Attempted to UPDATE {len(next_level_gains_to_insert)} records.")
            except Exception as e:
                logger.error(f"Database error during UPSERT: {e}", exc_info=True)
                raise

    return {
        "representationid": representation_id,
        "headquartersid": headquarters_id,
        "ratingreportid": rating_report_id,
    }
