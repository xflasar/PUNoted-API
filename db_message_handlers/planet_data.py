import logging
import time
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


# This helper function is needed for the logic below.
def get_changed_fields(new_data: Dict[str, Any], existing_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compares two dictionaries and returns a new dictionary with only the keys
    that have changed values.
    """
    changed_fields = {}
    for key, new_value in new_data.items():
        # Compare vs existing. Note: existing_data keys usually match DB columns (lowercase)
        if new_value != existing_data.get(key):
            changed_fields[key] = new_value
    return changed_fields


async def handle_planet_data_message(conn, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Asynchronously handles inserting/updating all planet-related data.
    Compatible with 'planets' being a LIST (bulk) or a DICT (single).
    """
    start_time = time.perf_counter()
    logger.debug("Starting processing planet data.")
    
    # Expects raw_payload["data"] to be the result of your conversion function
    converted_data = raw_payload["data"]
    overall_results = {}

    # 1. Normalize 'planets' to always be a list
    planets_input = converted_data.get("planets")
    
    if isinstance(planets_input, list):
        planets_list = planets_input
    elif isinstance(planets_input, dict):
        planets_list = [planets_input]
    else:
        return {
            "success": False, 
            "message": "Invalid or missing planet data in payload."
        }

    # Filter out empty entries if any
    planets_list = [p for p in planets_list if p.get("planetid")]

    if not planets_list:
         return {
            "success": False, 
            "message": "No valid planet IDs found in data."
        }

    # Extract all IDs for batch querying
    all_planet_ids = [p["planetid"] for p in planets_list]

    table_configs = {
        "planet_resources": converted_data.get("planet_resources", []),
        "planet_build_options": converted_data.get("planet_build_options", []),
        "planet_projects": converted_data.get("planet_projects", []),
        "planet_orbit": converted_data.get("planet_orbit", []),
        "planet_celestial_bodies": converted_data.get("planet_celestial_bodies", []),
        "planet_production_fees": converted_data.get("planet_production_fees", []),
        "planet_physical_data": converted_data.get("planet_physical_data", [])
    }

    try:
        async with conn.pool.acquire() as con:
            async with con.transaction():
                
                # --- Step 1: Handle main 'planets' table (Bulk Upsert) ---
                if planets_list:
                    # Use keys from the first planet to build the query
                    keys = list(planets_list[0].keys())
                    columns = ", ".join(keys)
                    placeholders = ", ".join([f"${i + 1}" for i in range(len(keys))])

                    # Build UPDATE clause (exclude 'planetid')
                    update_assignments = ", ".join([
                        f"{k} = EXCLUDED.{k}" for k in keys if k != "planetid"
                    ])

                    upsert_query = f"""
                        INSERT INTO planets ({columns}) 
                        VALUES ({placeholders})
                        ON CONFLICT (planetid) 
                        DO UPDATE SET {update_assignments}
                    """

                    try:
                        # Convert list of dicts to list of value-lists for executemany
                        values_list = [list(p.values()) for p in planets_list]
                        
                        await con.executemany(upsert_query, values_list)

                        overall_results["planets"] = {
                            "success": True,
                            "message": f"Processed {len(planets_list)} planet records.",
                        }
                    except Exception as e:
                        logger.error(f"Database error during planets UPSERT: {e}", exc_info=True)
                        raise

                # --- Step 2: Handle nested tables (Bulk Nested) ---
                for table_name, records in table_configs.items():
                    if not records:
                        continue

                    # Define composite keys
                    if table_name == "planet_resources":
                        key_fields = ["planetid", "materialid"]
                    elif table_name == "planet_build_options":
                        key_fields = ["planetid", "sitetype"]
                    elif table_name == "planet_projects":
                        key_fields = ["planetid", "type", "entityid"] # Added entityid
                    elif table_name == "planet_orbit":
                        key_fields = ["planetid"]
                    elif table_name == "planet_celestial_bodies":
                        key_fields = ["planetid", "id"]
                    elif table_name == "planet_production_fees":
                        key_fields = ["planetid", "category", "workforcelevel"]
                    elif table_name == "planet_physical_data":
                        key_fields = ["planetid"]
                    else:
                        continue

                    # Bulk Fetch: Get existing records for ALL planets in this batch
                    fetch_query = f"SELECT * FROM {table_name} WHERE planetid = ANY($1::text[])"
                    
                    existing_rows = await con.fetch(fetch_query, all_planet_ids)
                    
                    existing_records_dict = {
                        tuple(rec[k] for k in key_fields): dict(rec)
                        for rec in existing_rows
                    }

                    records_to_insert = []
                    records_to_update = []

                    for record_data in records:
                        record_key = tuple(record_data.get(k) for k in key_fields)
                        if record_key in existing_records_dict:
                            existing_record = existing_records_dict[record_key]
                            changed_fields = get_changed_fields(record_data, existing_record)
                            if changed_fields:
                                records_to_update.append({
                                    "key_values": [record_data.get(k) for k in key_fields],
                                    "changed_data": changed_fields,
                                })
                        else:
                            records_to_insert.append(record_data)

                    # Bulk Insert
                    if records_to_insert:
                        first_rec = records_to_insert[0]
                        
                        # Define which tables use Text/GUID IDs that MUST be inserted manually
                        TABLES_WITH_MANUAL_IDS = {"planet_celestial_bodies", "planet_projects"}

                        if table_name in TABLES_WITH_MANUAL_IDS:
                            keys_to_insert = list(first_rec.keys())
                        else:
                            # Exclude 'id' only for tables with auto-incrementing integers
                            keys_to_insert = [k for k in first_rec.keys() if k != 'id']
                        
                        keys_str = ", ".join(keys_to_insert)
                        values_placeholders = ", ".join([f"${i + 1}" for i in range(len(keys_to_insert))])
                        
                        insert_query = f"INSERT INTO {table_name} ({keys_str}) VALUES ({values_placeholders});"
                        
                        # Prepare values strictly matching keys_to_insert order
                        values_for_insert = [
                            [rec[k] for k in keys_to_insert] 
                            for rec in records_to_insert
                        ]

                        try:
                            await con.executemany(insert_query, values_for_insert)
                        except Exception as e:
                            logger.error(f"Database error during {table_name} INSERT: {e}", exc_info=True)
                            raise

                    # Bulk Update (via loop)
                    if records_to_update:
                        for update_rec in records_to_update:
                            changed_data = update_rec["changed_data"]
                            key_values = update_rec["key_values"]

                            update_fields = ", ".join(
                                [f"{key} = ${i + len(key_fields) + 1}" for i, key in enumerate(changed_data.keys())]
                            )

                            where_clause = " AND ".join(
                                [f"{key_fields[i]} = ${i + 1}" for i in range(len(key_fields))]
                            )

                            update_query = f"UPDATE {table_name} SET {update_fields} WHERE {where_clause};"
                            
                            try:
                                set_clause_len = len(changed_data)
                                update_fields = ", ".join(
                                    [f"{key} = ${i + 1}" for i, key in enumerate(changed_data.keys())]
                                )
                                where_clause = " AND ".join(
                                    [f"{key_fields[i]} = ${i + 1 + set_clause_len}" for i in range(len(key_fields))]
                                )
                                
                                update_query = f"UPDATE {table_name} SET {update_fields} WHERE {where_clause};"
                                
                                await con.execute(update_query, *changed_data.values(), *key_values)
                            except Exception as e:
                                logger.error(f"Database error during {table_name} UPDATE: {e}", exc_info=True)
                                raise

                    overall_results[table_name] = {
                        "success": True,
                        "message": f"Processed {len(records_to_insert)} inserts and {len(records_to_update)} selective updates.",
                    }

        end_time = time.perf_counter()
        logger.debug(f"Finished processing planet data in {end_time - start_time:.2f} seconds.")
        return {
            "success": True,
            "message": "Processed planet data.",
            "results": overall_results,
        }
    except Exception as e:
        logger.error(f"Error processing planet data: {e}", exc_info=True)
        raise