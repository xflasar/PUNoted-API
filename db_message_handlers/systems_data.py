import logging
import time
from typing import Dict, Any, List


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
    insert_value_placeholders = ', '.join(f'${i+1}' for i in range(len(all_record_keys)))

    update_set_clauses = [f'"{col}" = EXCLUDED."{col}"' for col in updatable_columns]
    update_set_str = ', '.join(update_set_clauses)
    
    conflict_columns_str = ', '.join(f'"{k}"' for k in unique_key_columns)

    if not update_set_str:
        query = f'INSERT INTO "{table_name}" ({insert_columns}) VALUES ({insert_value_placeholders}) ON CONFLICT ({conflict_columns_str}) DO NOTHING;'
    else:
        query = f'INSERT INTO "{table_name}" ({insert_columns}) VALUES ({insert_value_placeholders}) ON CONFLICT ({conflict_columns_str}) DO UPDATE SET {update_set_str};'

    values_to_insert = [tuple(record.values()) for record in records]
    
    await db.executemany(query, values_to_insert)
    logger.debug(f"Upserted {len(records)} records into {table_name}.")

# --- Main Handler Function ---
async def handle_systems_data(db: Any, raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles incoming 'systems' data messages, converts them, and synchronizes
    them with the database.
    """
    start_time = time.perf_counter()
    logger.info("Starting processing batch of systems data.")
    try:
        converted_data = raw_data['data']
        systems_records = converted_data['systems']
        connections_records = converted_data['systems_connections']

        # --- Synchronize Systems ---
        systems_updatable_cols = [
            'name', 'naturalid', 'type', 'positionx', 'positiony', 'positionz',
            'sectorid', 'subsectorid'
        ]
        await _upsert_batch(db, 'systems', systems_records, ['systemid'], systems_updatable_cols)
        
        # --- Synchronize Systems Connections ---
        connections_unique_cols = ['systemidorigin', 'systemiddestination']
        connections_updatable_cols = []
        await _upsert_batch(db, 'system_connections', connections_records, 
              connections_unique_cols, connections_updatable_cols)
        
        end_time = time.perf_counter()
        logger.info(f"Total processing for the entire batch took {end_time - start_time:.4f} seconds.")
        logger.info(f"Successfully processed systems data, Systems: {len(systems_records)}, Connections: {len(connections_records)}")
        return {"success": True, "message": "Systems data processed successfully."}

    except Exception as e:
        logger.error(f"Error handling systems data: {e}", exc_info=True)
        raise