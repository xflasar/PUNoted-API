import asyncio
import logging
import time
from typing import Any, Dict, List

import asyncpg

logger = logging.getLogger(__name__)


def get_changed_fields(new_data: Dict[str, Any], existing_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compares two dictionaries and returns a new dictionary with only the keys
    that have changed values.
    """
    changed_fields = {}
    for key, new_value in new_data.items():
        if new_value != existing_data.get(key):
            changed_fields[key] = new_value
    return changed_fields


async def handle_site_data_message(conn, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Asynchronously handles incoming site data messages using a single transaction from a connection pool.
    It performs selective updates on existing records and inserts new ones.
    """
    try:
        async with conn.pool.acquire() as con:
            async with con.transaction():
                return await process_all_site_data(conn, raw_payload)
    except Exception as e:
        logger.error(f"Error processing sites data: {e}", exc_info=True)
        raise


async def process_all_site_data(con, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Processes a single site payload (data must be a dict, not list).
    Includes insert, update, and nested processing.
    """
    logger.debug("Starting processing site data, including deletion of stale sites.")

    # --- STEP 1: Validate and load payload ---
    site_data = raw_payload.get("data")

    if not site_data:
        return {"success": True, "message": "No site data provided."}

    if not isinstance(site_data, dict):
        return {
            "success": False,
            "message": "Invalid data format: expected object/dict, not list",
        }

    # --- STEP 2: Resolve user ID ---
    user_response = await con.fetch_one(
        "SELECT accountid, userdataid FROM users WHERE accountid = $1;",
        raw_payload.get("userId"),
    )

    if user_response and user_response.get("userdataid") is not None:
        userid = user_response.get("userdataid")
    elif user_response:
        userid = user_response.get("accountid")
    else:
        logger.warning(f"User not found for accountid: {raw_payload.get('userId')}")
        return {"success": False, "message": "User not found."}

    # --- STEP 3: Validate ID and prepare base record ---
    site_id = site_data.get("siteid")
    if not site_id:
        return {"success": False, "message": "Missing siteid."}

    site_data["userid"] = userid

    # record for insert/update
    record_to_process = site_data.copy()
    record_to_process.pop("building_options", None)
    record_to_process.pop("platforms", None)

    # --- STEP 4: Check existing record ---
    existing_record = await con.fetch_one(
        "SELECT * FROM sites WHERE siteid = $1 AND userid = $2;",
        site_id,
        userid,
    )

    results = {}

    if existing_record:
        existing_dict = dict(existing_record)
        changed_fields = get_changed_fields(record_to_process, existing_dict)

        if changed_fields:
            update_fields = ", ".join([f"{key} = ${i + 2}" for i, key in enumerate(changed_fields.keys())])
            update_query = f"UPDATE sites SET {update_fields} WHERE siteid = $1;"
            await con.execute(update_query, site_id, *changed_fields.values())
            results["status"] = "updated"
        else:
            results["status"] = "unchanged"

    else:
        keys = ", ".join(record_to_process.keys())
        values_placeholders = ", ".join([f"${i + 1}" for i in range(len(record_to_process))])
        insert_query = f"INSERT INTO sites ({keys}) VALUES ({values_placeholders});"
        await con.execute(insert_query, *record_to_process.values())
        results["status"] = "inserted"

    # --- STEP 5: Handle nested records ---
    await _handle_all_nested_data(con, site_data)

    return {
        "success": True,
        "message": "Site processed successfully",
        "siteid": site_id,
        "result": results,
    }


async def _handle_all_nested_data(con, site_data: Dict[str, Any]):
    """
    Processes all nested data, including buildings, platforms, and their
    associated materials and workforce capacities.
    """
    site_id = site_data.get("siteid")

    buildings_task = None
    platforms_task = None

    buildings_options_materials_task = []
    buildings_options_workforce_task = []

    platforms_reclaimables_task = []
    platforms_repairs_task = []

    platform_ids = []

    ## All of this needs rework 

    # 1. Prepare and insert 'building_options' records
    #building_records_to_insert = []
    #for option in site_data.get("building_options", []):
    #    clean_record = option.copy()
    #    clean_record.pop("materials", None)
    #    clean_record.pop("workforcecapacities", None)
    #    building_records_to_insert.append(clean_record)

    #if building_records_to_insert:
    #    keys = ", ".join(building_records_to_insert[0].keys())
    #    values_placeholders = ", ".join([f"${i + 1}" for i in range(len(building_records_to_insert[0]))])
    #    insert_query = f"INSERT INTO buildings ({keys}) VALUES ({values_placeholders}) ON CONFLICT DO NOTHING;"

    #    values_to_insert = [list(rec.values()) for rec in building_records_to_insert]
    #    buildings_task = con.executemany(insert_query, values_to_insert)

    # 2. Process nested 'materials' and 'workforce_capacities'
    #for option in site_data.get("building_options", []):
    #    building_id = option.get("buildingid")
    #    if not building_id:
    #        continue

    #    materials_list = option.get("materials", [])
    #    if materials_list:
    #        buildings_options_materials_task.extend(materials_list)

    #    workforce_list = option.get("workforcecapacities", [])
    #    if workforce_list:
    #        buildings_options_workforce_task.extend(workforce_list)

    # 3. Prepare and insert 'site_platforms' records (UPSERT)
    platform_records_to_insert = []
    for platform in site_data.get("platforms", []):
        platform_ids.append(platform.get("platformid"))
        clean_record = platform.copy()
        clean_record.pop("reclaimable_materials", None)
        clean_record.pop("repair_materials", None)
        platform_records_to_insert.append(clean_record)

    if platform_records_to_insert:
        keys = platform_records_to_insert[0].keys()
        keys_str = ", ".join(keys)
        values_placeholders = ", ".join([f"${i + 1}" for i in range(len(keys))])
        update_set_clause = ", ".join([f"{key} = EXCLUDED.{key}" for key in keys])

        insert_query = f"""
            INSERT INTO site_platforms ({keys_str})
            VALUES ({values_placeholders})
            ON CONFLICT (platformid) DO UPDATE
            SET {update_set_clause};
        """
        values_to_insert = [list(rec.values()) for rec in platform_records_to_insert]
        await con.executemany(insert_query, values_to_insert)

    # 4. Process nested 'reclaimable_materials' and 'repair_materials'
    for platform in site_data.get("platforms", []):
        platform_id = platform.get("platformid")
        if not platform_id:
            continue

        reclaimable_list = platform.get("reclaimable_materials", [])
        if reclaimable_list:
            platforms_reclaimables_task.extend(reclaimable_list)

        repair_list = platform.get("repair_materials", [])
        if repair_list:
            platforms_repairs_task.extend(repair_list)

    buildings_time_process_start = time.perf_counter()

    # --- STEP 5: DELETE REMOVED PLATFORM RECORDS (within the current site) ---
    if site_id and platform_ids:
        # Deletes any platform for this site whose ID is NOT in the current platform_ids list.
        delete_platforms_query = """
            DELETE FROM site_platforms
            WHERE siteid = $1
            AND platformid <> ALL($2::text[]);
        """
        try:
            # If platform_ids is empty, this correctly deletes ALL platforms for the site.
            await con.execute(delete_platforms_query, site_id, platform_ids)
            logger.debug(f"Deleted platforms from site_platforms for site {site_id} that were not in the payload.")
        except Exception as e:
            logger.error(
                f"Database error during site_platforms deletion for site {site_id}: {e}",
                exc_info=True,
            )
            raise

    # --- STEP 6: Upsert all nested records ---
    await asyncio.gather(
        #_upsert_records(
        #    con,
        #    "building_build_materials",
        #    buildings_options_materials_task,
        #    ["buildingid", "materialid"],
        #),
        #_upsert_records(
        #    con,
        #    "building_workforce_capacities",
        #    buildings_options_workforce_task,
        #    ["buildingid", "workforcelevel"],
        #),
        _upsert_records(
            con,
            "platform_materials",
            platforms_reclaimables_task,
            ["platformid", "materialid", "materialtype"],
        ),
        _upsert_records(
            con,
            "platform_materials",
            platforms_repairs_task,
            ["platformid", "materialid", "materialtype"],
        ),
    )

    # 7. Clean up old platform materials for the remaining platforms
    all_platform_materials = platforms_reclaimables_task + platforms_repairs_task
    await _handle_deletions_by_platforms(
        con,
        "platform_materials",
        ["platformid", "materialid", "materialtype"],
        all_platform_materials,
        platform_ids,
    )
    buildings_time_process_end = time.perf_counter()
    logger.debug(
        f"Processed nested site_platforms_options data for siteid {site_id} in {buildings_time_process_end - buildings_time_process_start:.4f} seconds."
    )


async def _upsert_records(
    con: asyncpg.Connection,
    table_name: str,
    records: List[Dict[str, Any]],
    unique_fields: List[str],
    chunk_size: int = 5000,
    timeout: float = None,
):
    """
    A generic helper function to perform bulk upserts using INSERT ... ON CONFLICT DO UPDATE.
    It processes records in chunks to prevent large transaction overhead.
    """
    if not records:
        return

    # Extract columns from the first record to build the query dynamically
    columns = records[0].keys()
    columns_str = ", ".join(columns)
    unique_columns_str = ", ".join(unique_fields)

    # Construct the SET clause for ON CONFLICT DO UPDATE
    set_clauses = []
    for col in columns:
        if col not in unique_fields:
            if col == "capacity":
                set_clauses.append(
                    f"capacity = EXCLUDED.capacity WHERE {table_name}.capacity IS DISTINCT FROM EXCLUDED.capacity"
                )
            elif col == "amount":
                set_clauses.append(
                    f"amount = EXCLUDED.amount WHERE {table_name}.amount IS DISTINCT FROM EXCLUDED.amount"
                )
            else:
                set_clauses.append(f"{col} = EXCLUDED.{col}")

    set_clause_str = ",\n".join(set_clauses)

    values_placeholders = ", ".join([f"${i + 1}" for i in range(len(columns))])

    if set_clause_str:
        on_conflict_clause = f"ON CONFLICT ({unique_columns_str}) DO UPDATE SET {set_clause_str}"
    else:
        on_conflict_clause = f"ON CONFLICT ({unique_columns_str}) DO NOTHING"

    query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_placeholders}) {on_conflict_clause};"

    # Process records in chunks
    for i in range(0, len(records), chunk_size):
        chunk = records[i : i + chunk_size]
        values_to_insert = [list(rec.values()) for rec in chunk]
        try:
            await con.executemany(query, values_to_insert, timeout=timeout)
        except Exception as e:
            logger.error(f"Database error during UPSERT: {e}", exc_info=True)
            raise
    logger.debug("Finished inserting data in upsert_records.")


async def _handle_deletions_by_platforms(
    con,
    table_name: str,
    unique_fields: List[str],
    current_records: List[Dict[str, Any]],
    platform_ids_in_payload: List[str],
):
    """
    Deletes records from a child table (e.g., platform_materials)
    that are not present in the new payload, for the platforms that still exist.
    """
    if not platform_ids_in_payload:
        return

    # 1. Get a list of all existing unique keys for these platforms from the database
    keys_query = f"""
        SELECT {", ".join(unique_fields)} FROM {table_name}
        WHERE platformid = ANY($1::text[]);
    """
    existing_records = await con.fetch_rows(keys_query, platform_ids_in_payload)
    existing_keys_in_db = set(tuple(rec.values()) for rec in existing_records)

    # 2. Get a set of unique keys from the new payload
    payload_keys = set(tuple(rec[field] for field in unique_fields) for rec in current_records)

    # 3. Find the keys that are in the database but not in the new payload
    keys_to_delete = existing_keys_in_db - payload_keys

    if not keys_to_delete:
        return

    # 4. Perform the bulk deletion with a single query
    if len(unique_fields) == 3:
        delete_query = f"""
            DELETE FROM {table_name}
            WHERE ({", ".join(unique_fields)}) IN (
                SELECT UNNEST($1::text[]), UNNEST($2::text[]), UNNEST($3::text[])
            );
        """
        values_to_delete = list(zip(*keys_to_delete))

        try:
            await con.execute(delete_query, *values_to_delete)
        except Exception as e:
            logger.error(f"Database error during deletion in {table_name}: {e}", exc_info=True)
            raise
    else:
        logger.warning(
            f"Skipping deletion for {table_name}: Generic deletion logic not implemented for {len(unique_fields)} unique fields."
        )
