import logging
import time
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

async def handle_stations_data_message(db, data: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.info("Starting processing station data.")
    converted_data = data["data"]

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                await _upsert_batch(con, 'stations', converted_data, ['stationid'], [])

        end_time = time.perf_counter()
        logger.info(f"Total processing for the entire batch took {end_time - start_time:.4f} seconds.")
        return {"success": True, "message": "Station data processed successfully."}
    except Exception as e:
        logger.error(f"we errored {e}")
        raise
    
async def _upsert_batch(
    con: Any,
    table_name: str,
    record: Dict[str, Any],
    unique_key_columns: List[str],
    updatable_columns: List[str]
) -> None:
    """
    Performs a bulk UPSERT (INSERT ON CONFLICT DO UPDATE) for a list of records.
    Assumes all records in the list have the same keys.
    """
    if not record:
        return

    all_record_keys = list(record.keys())
    insert_columns = ', '.join(f'"{k}"' for k in all_record_keys)
    insert_value_placeholders = ', '.join(f'${i+1}' for i in range(len(all_record_keys)))

    update_set_clauses = [f'"{col}" = EXCLUDED."{col}"' for col in updatable_columns]
    update_set_str = ', '.join(update_set_clauses)
    
    conflict_columns_str = ', '.join(f'"{k}"' for k in unique_key_columns)

    if not update_set_str:
        query = f'INSERT INTO "{table_name}" ({insert_columns}) VALUES ({insert_value_placeholders}) ON CONFLICT ({conflict_columns_str}) DO NOTHING;'
    else:
        query = f'INSERT INTO "{table_name}" ({insert_columns}) VALUES ({insert_value_placeholders}) ON CONFLICT ({conflict_columns_str}) DO UPDATE SET {update_set_str};'

    values_to_insert = tuple(record.values())
    
    try:
        await con.execute(query, *values_to_insert)
    except Exception as e:
        logger.error(e)
        raise
    logger.debug(f"Upserted record into {table_name}.")