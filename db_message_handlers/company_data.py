import logging
import time
from typing import Any, Dict, List
import asyncpg

logger = logging.getLogger(__name__)

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
    payload_data = converted_data['data']

    # Extract data for each table from the converted payload
    company_data_record = payload_data.get('company_data', {})
    representation_record = payload_data.get('representation', {})
    representation_contributors = payload_data.get('representationContributors', [])
    rating_report_record = payload_data.get('ratingReport', {})
    headquarters_record = payload_data.get('headquarters', {})
    headquarters_upgrade_items = payload_data.get('headquartersUpgradeItems', [])
    headquarters_efficiency_gains = payload_data.get('headquarters_efficiency_gains', [])
    headquarters_efficiency_gains_next_level = payload_data.get('headquarters_efficiency_gains_next_level', [])

    company_id = company_data_record.get('companyid')
    if not company_id:
        return {"success": False, "message": "Company ID is missing from payload."}
    
    userdataid_record = await conn.fetch_one(f"SELECT userdataid FROM users WHERE xata_id = $1;", userid)
    userdataid = userdataid_record['userdataid']

    # Use a single transaction for all operations for this company (Insert needs to have it too)
    try:
        headquarters_id_to_link = None
        representation_id_to_link = None
        rating_report_id_to_link = None
        
        # Check if the main company_data record already exists
        existing_company_record = await conn.fetch_one(
            f"SELECT xata_id, headquartersid, representationid, ratingreportid FROM {company_data_table} WHERE companyid = $1;",
            company_id
        )

        if existing_company_record:
            # --- UPDATE EXISTING COMPANY ---
            logger.info(f"Company '{company_id}' already exists. Updating.")
            await handle_company_update_transactional(conn, existing_company_record, company_data_record, company_id, company_data_table, headquarters_record, representation_record, rating_report_record, headquarters_upgrade_items, headquarters_efficiency_gains, headquarters_efficiency_gains_next_level, representation_contributors)
        else:
            # --- INSERT NEW COMPANY ---
            logger.info(f"Company '{company_id}' does not exist. Inserting new company and associated data.")
            
            linked_ids = await insert_new_company(
                conn,
                representation_record,
                representation_contributors,
                rating_report_record,
                headquarters_record,
                headquarters_upgrade_items,
                headquarters_efficiency_gains,
                headquarters_efficiency_gains_next_level
            )
            
            headquarters_id_to_link = linked_ids["headquartersid"]
            representation_id_to_link = linked_ids["representationid"]
            rating_report_id_to_link = linked_ids["ratingreportid"]

            # Now insert the main company_data record, linking to the newly created IDs
            company_data_record['headquartersid'] = headquarters_id_to_link
            company_data_record['representationid'] = representation_id_to_link
            company_data_record['ratingreportid'] = rating_report_id_to_link
            company_data_record['userdataid'] = userdataid # Link to user

            company_keys = ', '.join(company_data_record.keys())
            company_values = ', '.join([f'${i+1}' for i in range(len(company_data_record))])
            await conn.execute(
                f"INSERT INTO {company_data_table} ({company_keys}) VALUES ({company_values});",
                *company_data_record.values()
            )

        end_time = time.perf_counter()
        logger.info(f"Finished processing company data in {end_time - start_time:.2f} seconds.")
        return {"success": True, "message": f"Transaction for company '{company_id}' completed."}

    except Exception as e:
        logger.error(f"Transaction failed for company '{company_id}': {e}", exc_info=True)
        raise

async def safe_update_record(conn, table_name, update_data, key_field, key_value):
    """
    Safely updates a single record using parameterized queries.
    """
    if not update_data:
        return

    set_clauses = [f"{key} = ${i+2}" for i, key in enumerate(update_data.keys())]
    set_clause = ", ".join(set_clauses)
    
    update_query = f"UPDATE {table_name} SET {set_clause} WHERE {key_field} = $1;"
    
    await conn.execute(update_query, key_value, *update_data.values())

async def handle_company_update_transactional(conn, existing_company_record, company_data_record, company_id, company_data_table, headquarters_record, representation_record, rating_report_record, headquarters_upgrade_items, headquarters_efficiency_gains, headquarters_efficiency_gains_next_level, representation_contributors):
    try:
        async with conn.pool.acquire() as con:
            async with con.transaction():
                headquarters_id_to_link = None
                representation_id_to_link = None
                rating_report_id_to_link = None

                if existing_company_record:
                    # --- UPDATE EXISTING COMPANY ---
                    logger.info(f"Company '{company_id}' already exists. Updating.")

                    headquarters_id_to_link = existing_company_record['headquartersid']
                    representation_id_to_link = existing_company_record['representationid']
                    rating_report_id_to_link = existing_company_record['ratingreportid']

                    # Update main linked records
                    if headquarters_record and headquarters_id_to_link:
                        await safe_update_record(conn, "headquarters", headquarters_record, "xata_id", headquarters_id_to_link)

                    if representation_record and representation_id_to_link:
                        await safe_update_record(conn, "representation", representation_record, "xata_id", representation_id_to_link)

                    if rating_report_record and rating_report_id_to_link:
                        await safe_update_record(conn, "rating_reports", rating_report_record, "xata_id", rating_report_id_to_link)

                    # Delete existing nested items
                    tables_to_clear_by_hq = ["headquarters_upgrade_items", "efficiency_gains", "efficiency_gains_next_level"]
                    for table in tables_to_clear_by_hq:
                        await conn.execute(f"DELETE FROM {table} WHERE headquartersid = $1;", headquarters_id_to_link)

                    await conn.execute("DELETE FROM representation_contributors WHERE representationid = $1;", representation_id_to_link)
                    logger.info("Deleted old nested items in preparation for new ones.")

                    # Insert new nested items (bulk insert)
                    if headquarters_upgrade_items:
                        items_to_insert = [{**item, 'headquartersid': headquarters_id_to_link} for item in headquarters_upgrade_items]
                        if items_to_insert:
                            keys = ', '.join(items_to_insert[0].keys())
                            values_placeholders = ', '.join([f'${i+1}' for i in range(len(items_to_insert[0]))])
                            await conn.executemany(f"INSERT INTO headquarters_upgrade_items ({keys}) VALUES ({values_placeholders});", [list(rec.values()) for rec in items_to_insert])
                            logger.info(f"Bulk inserted {len(items_to_insert)} headquarters_upgrade_items.")

                    if headquarters_efficiency_gains:
                        gains_to_insert = [{**gain, 'headquartersid': headquarters_id_to_link} for gain in headquarters_efficiency_gains]
                        if gains_to_insert:
                            keys = ', '.join(gains_to_insert[0].keys())
                            values_placeholders = ', '.join([f'${i+1}' for i in range(len(gains_to_insert[0]))])
                            await conn.executemany(f"INSERT INTO efficiency_gains ({keys}) VALUES ({values_placeholders});", [list(rec.values()) for rec in gains_to_insert])
                            logger.info(f"Bulk inserted {len(gains_to_insert)} efficiency_gains.")

                    if headquarters_efficiency_gains_next_level:
                        next_level_gains_to_insert = [{**gain, 'headquartersid': headquarters_id_to_link} for gain in headquarters_efficiency_gains_next_level]
                        if next_level_gains_to_insert:
                            keys = ', '.join(next_level_gains_to_insert[0].keys())
                            values_placeholders = ', '.join([f'${i+1}' for i in range(len(next_level_gains_to_insert[0]))])
                            await conn.executemany(f"INSERT INTO efficiency_gains_next_level ({keys}) VALUES ({values_placeholders});", [list(rec.values()) for rec in next_level_gains_to_insert])
                            logger.info(f"Bulk inserted {len(next_level_gains_to_insert)} efficiency_gains_next_level.")

                    if representation_contributors:
                        contribs_to_insert = [{**contrib, 'representationid': representation_id_to_link} for contrib in representation_contributors]
                        if contribs_to_insert:
                            keys = ', '.join(contribs_to_insert[0].keys())
                            values_placeholders = ', '.join([f'${i+1}' for i in range(len(contribs_to_insert[0]))])
                            await conn.executemany(f"INSERT INTO representation_contributors ({keys}) VALUES ({values_placeholders});", [list(rec.values()) for rec in contribs_to_insert])
                            logger.info(f"Bulk inserted {len(contribs_to_insert)} representation_contributors.")

                    # Update the main company_data record
                    company_data_record['headquartersid'] = headquarters_id_to_link
                    company_data_record['representationid'] = representation_id_to_link
                    company_data_record['ratingreportid'] = rating_report_id_to_link

                    update_fields = ", ".join([f"{key} = ${i+2}" for i, key in enumerate(company_data_record.keys())])
                    update_query = f"UPDATE {company_data_table} SET {update_fields} WHERE xata_id = $1;"
                    await conn.execute(update_query, existing_company_record['xata_id'], *company_data_record.values())
                    logger.info(f"Updated main company record for '{company_id}'.")

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
    headquarters_efficiency_gains_next_level: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Asynchronously inserts a new company and its associated nested data within an ongoing transaction.
    Returns the IDs of the main linked records (HQ, Representation, Rating Report).
    """
    
    # Phase 1: Insert lowest-level main records (Representation, Headquarters, Rating Report)
    # and retrieve their IDs.

    # Representation
    rep_keys = ', '.join(representation_record.keys())
    rep_values = ', '.join([f'${i+1}' for i in range(len(representation_record))])
    representation_id_result = await conn.fetch_one(
        f"INSERT INTO representation ({rep_keys}) VALUES ({rep_values}) RETURNING xata_id;",
        *representation_record.values()
    )
    representation_id = representation_id_result['xata_id']

    # Headquarters
    hq_keys = ', '.join(headquarters_record.keys())
    hq_values = ', '.join([f'${i+1}' for i in range(len(headquarters_record))])
    headquarters_id_result = await conn.fetch_one(
        f"INSERT INTO headquarters ({hq_keys}) VALUES ({hq_values}) RETURNING xata_id;",
        *headquarters_record.values()
    )

    headquarters_id = headquarters_id_result['xata_id']

    # Rating Report
    rr_keys = ', '.join(rating_report_record.keys())
    rr_values = ', '.join([f'${i+1}' for i in range(len(rating_report_record))])
    rating_report_id_result = await conn.fetch_one(
        f"INSERT INTO rating_reports ({rr_keys}) VALUES ({rr_values}) RETURNING xata_id;",
        *rating_report_record.values()
    )
    rating_report_id = rating_report_id_result['xata_id']

    if not all([representation_id, headquarters_id, rating_report_id]):
        raise Exception("Failed to insert core company linked records.")

    # Phase 2: Insert mid-level linked tables (contributors, upgrade items, efficiency gains)
    
    # Representation contributors
    if representation_contributors:
        contrib_records_to_insert = []
        for contributor in representation_contributors:
            contributor_copy = contributor.copy()
            contributor_copy["representationid"] = representation_id # Link to parent
            contrib_records_to_insert.append(contributor_copy)
        
        if contrib_records_to_insert:
            contrib_keys = ', '.join(contrib_records_to_insert[0].keys())
            contrib_values = ', '.join([f'${i+1}' for i in range(len(contrib_records_to_insert[0]))])
            contrib_query = f"INSERT INTO representation_contributors ({contrib_keys}) VALUES ({contrib_values});"
            await conn.executemany(contrib_query, [list(rec.values()) for rec in contrib_records_to_insert])

    # Headquarters items
    if headquarters_upgrade_items:
        items_to_insert = []
        for item in headquarters_upgrade_items:
            item_copy = item.copy()
            item_copy["headquartersid"] = headquarters_id # Link to parent
            items_to_insert.append(item_copy)

        if items_to_insert:
            item_keys = ', '.join(items_to_insert[0].keys())
            item_values = ', '.join([f'${i+1}' for i in range(len(items_to_insert[0]))])
            item_query = f"INSERT INTO headquarters_upgrade_items ({item_keys}) VALUES ({item_values});"
            await conn.executemany(item_query, [list(rec.values()) for rec in items_to_insert])

    # Headquarters efficiency gains
    if headquarters_efficiency_gains:
        gains_to_insert = []
        for gain in headquarters_efficiency_gains:
            gain_copy = gain.copy()
            gain_copy["headquartersid"] = headquarters_id # Link to parent
            gains_to_insert.append(gain_copy)
        
        if gains_to_insert:
            gain_keys = ', '.join(gains_to_insert[0].keys())
            gain_values = ', '.join([f'${i+1}' for i in range(len(gains_to_insert[0]))])
            gain_query = f"INSERT INTO efficiency_gains ({gain_keys}) VALUES ({gain_values});"
            await conn.executemany(gain_query, [list(rec.values()) for rec in gains_to_insert])

    # Headquarters efficiency gains next level
    if headquarters_efficiency_gains_next_level:
        next_level_gains_to_insert = []
        for gain in headquarters_efficiency_gains_next_level:
            gain_copy = gain.copy()
            gain_copy["headquartersid"] = headquarters_id # Link to parent
            next_level_gains_to_insert.append(gain_copy)
        
        if next_level_gains_to_insert:
            next_level_keys = ', '.join(next_level_gains_to_insert[0].keys())
            next_level_values = ', '.join([f'${i+1}' for i in range(len(next_level_gains_to_insert[0]))])
            next_level_query = f"INSERT INTO efficiency_gains_next_level ({next_level_keys}) VALUES ({next_level_values});"
            await conn.executemany(next_level_query, [list(rec.values()) for rec in next_level_gains_to_insert])

    return {
        "representationid": representation_id,
        "headquartersid": headquarters_id,
        "ratingreportid": rating_report_id
    }