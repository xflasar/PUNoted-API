import logging
import time
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

"""
    Needs rewriting to use transaction
"""

async def _upsert_batch(
    db: Any,
    table_name: str,
    records: List[Dict[str, Any]],
    unique_key_columns: List[str],
    updatable_columns: List[str]
) -> None:
    """
    Performs a bulk UPSERT (INSERT ON CONFLICT DO UPDATE) for a list of records.
    Assumes all records in the list have the same keys.
    """
    if not records:
        return

    all_record_keys = list(records[0].keys())
    insert_columns = ', '.join(f'"{k}"' for k in all_record_keys)
    insert_value_placeholders = ', '. join(f'${i+1}' for i in range(len(all_record_keys)))

    update_set_clauses = [f'"{col}" = EXCLUDED."{col}"' for col in updatable_columns]
    update_set_str = ', '.join(update_set_clauses)
    
    conflict_columns_str = ', '.join(f'"{k}"' for k in unique_key_columns)

    if not update_set_str:
        query = f'INSERT INTO "{table_name}" ({insert_columns}) VALUES ({insert_value_placeholders}) ON CONFLICT ({conflict_columns_str}) DO NOTHING;'
    else:
        query = f'INSERT INTO "{table_name}" ({insert_columns}) VALUES ({insert_value_placeholders}) ON CONFLICT ({conflict_columns_str}) DO UPDATE SET {update_set_str};'

    values_to_insert = [tuple(record.values()) for record in records]
    
    try:
        await db.executemany(query, values_to_insert)
    except Exception as e:
        logger.error(f"Database error during UPSERT: {e}", exc_info=True)
        raise
    logger.debug(f"Upserted {len(records)} records into {table_name}.")

async def handle_site_available_reserve_population_data_message(db: Any, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.info("Starting processing site reserve pop data.")

    population_data_records = raw_payload.get("data", [])
    table_name = "site_available_reserve_populations"

    try:
        if not population_data_records:
            logger.info("No reserve population data to process.")
            return {"success": True, "message": "No reserve population data to process."}
        
        planetid = await db.fetch_one(f"SELECT addressplanetid FROM sites WHERE siteid = '{population_data_records.get('siteid')}'")

        population_data_records['planetid'] = planetid['addressplanetid']

        unique_columns = ["planetid", "siteid"]
        
        updatable_columns = ["pioneer", "settler", "engineer", "scientist", "technician"]
        async with db.pool.acquire() as con:
            async with con.transaction():
                await _upsert_batch(
                    con,
                    table_name,
                    [population_data_records],
                    unique_columns,
                    updatable_columns
                )

        end_time = time.perf_counter()
        duration = (end_time - start_time) * 1000
        logger.info(f"Finished processing site reserve pop data for {len(population_data_records)} records in {duration:.2f} ms.")
        return {"success": True, "message": f"Successfully processed {len(population_data_records)} reserve population records."}

    except Exception as e:
        logger.error(f"Error processing site reserve pop data: {e}", exc_info=True)
        raise