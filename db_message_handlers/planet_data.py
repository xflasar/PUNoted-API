import logging
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)

# This helper function is needed for the logic below.
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

async def handle_planet_data_message(conn, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Asynchronously handles inserting/updating all planet-related data from a single payload,
    using a single transaction and bulk operations with selective updates.
    """
    start_time = time.perf_counter()
    logger.info("Starting processing planet data.")
    converted_data = raw_payload["data"]
    overall_results = {}

    planet = converted_data.get("planets")
    if not planet or not planet.get("planetid"):
        return {"success": False, "message": "Invalid or missing planet data in payload."}

    table_configs = {
        "planet_resources": converted_data.get("resources", []),
        "planet_build_options": converted_data.get("build_options", []),
        "planet_projects": converted_data.get("projects", []),
    }
    
    try:
        async with conn.pool.acquire() as con:
            async with con.transaction():
                # Step 1: Handle the main 'planets' table record
                existing_planet = await conn.fetch_one(
                    "SELECT * FROM planets WHERE planetid = $1;",
                    planet.get("planetid")
                )
                
                if existing_planet:
                    changed_fields = get_changed_fields(planet, dict(existing_planet))
                    
                    if changed_fields:
                        update_fields = ", ".join([f"{key} = ${i+2}" for i, key in enumerate(changed_fields.keys())])
                        update_query = f"UPDATE planets SET {update_fields} WHERE planetid = $1;"
                        await conn.execute(update_query, planet.get("planetid"), *changed_fields.values())
                        overall_results["planets"] = {"success": True, "message": f"Record '{planet.get("planetid")}' updated. Changed fields: {list(changed_fields.keys())}"}
                    else:
                        overall_results["planets"] = {"success": True, "message": f"Record '{planet.get("planetid")}' is unchanged."}
                else:
                    keys = ", ".join(planet.keys())
                    values_placeholders = ", ".join([f'${i+1}' for i in range(len(planet))])
                    insert_query = f"INSERT INTO planets ({keys}) VALUES ({values_placeholders});"
                    await conn.execute(insert_query, *planet.values())
                    overall_results["planets"] = {"success": True, "message": f"Record '{planet.get("planetid")}' inserted."}

                # Step 2: Handle nested tables with selective updates
                for table_name, records in table_configs.items():
                    if not records:
                        continue

                    if table_name == "planet_resources":
                        key_fields = ["planetid", "materialid"]
                    elif table_name == "planet_build_options":
                        key_fields = ["planetid", "sitetype"]
                    elif table_name == "planet_projects":
                        key_fields = ["planetid", "type"]
                    else:
                        continue

                    existing_records_dict = {
                        tuple(rec[k] for k in key_fields): dict(rec)
                        for rec in await conn.fetch_rows(f"SELECT * FROM {table_name} WHERE planetid = $1;", planet.get("planetid"))
                    }
                    
                    records_to_insert = []
                    records_to_update = []

                    for record_data in records:
                        record_key = tuple(record_data.get(k) for k in key_fields)
                        if record_key in existing_records_dict:
                            existing_record = existing_records_dict[record_key]
                            changed_fields = get_changed_fields(record_data, existing_record)
                            if changed_fields:
                                records_to_update.append({"key_values": [record_data.get(k) for k in key_fields], "changed_data": changed_fields})
                        else:
                            records_to_insert.append(record_data)

                    if records_to_insert:
                        keys = ', '.join(records_to_insert[0].keys())
                        values_placeholders = ', '.join([f'${i+1}' for i in range(len(records_to_insert[0]))])
                        insert_query = f"INSERT INTO {table_name} ({keys}) VALUES ({values_placeholders});"
                        await conn.executemany(insert_query, [list(rec.values()) for rec in records_to_insert])
                    
                    if records_to_update:
                        for update_rec in records_to_update:
                            changed_data = update_rec["changed_data"]
                            key_values = update_rec["key_values"]
                            
                            update_fields = ", ".join([f"{key} = ${i+len(key_fields)+1}" for i, key in enumerate(changed_data.keys())])
                            
                            update_query = f"UPDATE {table_name} SET {update_fields} WHERE {' AND '.join([f'{key_fields[i]} = ${i+1}' for i in range(len(key_fields))])};"
                            await conn.execute(update_query, *key_values, *changed_data.values())

                    overall_results[table_name] = {"success": True, "message": f"Processed {len(records_to_insert)} inserts and {len(records_to_update)} selective updates."}
        
        end_time = time.perf_counter()
        logger.info(f"Finished processing planet data in {end_time - start_time:.2f} seconds.")
        return {
            "success": True, 
            "message": f"Processed planet data.",
            "results": overall_results
        }
    except Exception as e:
        logger.error(f"Error processing planet data: {e}", exc_info=True)
        raise