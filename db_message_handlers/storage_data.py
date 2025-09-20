import logging
import time
from typing import Any, Dict, List, Optional

import asyncpg
from db import Database

logger = logging.getLogger(__name__)

async def handle_storage_data_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Processes storage data, performing bulk inserts and updates in a single transaction.
    """
    start_time = time.perf_counter()
    logger.info("Starting processing storage data.")

    converted_data = raw_payload.get("data")
    storage_records = converted_data.get("storages", [])
    
    if not storage_records:
        logger.info("No storage records to process.")
        return {"success": True, "message": "No storage records to process."}
    
    try:
        user_response = await db.fetch_one(
            "SELECT xata_id, userdataid FROM users WHERE xata_id = $1;",
            raw_payload.get("userId")
        )
        if user_response and user_response.get('userdataid') is not None:
            userid = user_response.get('userdataid')
        elif user_response:
            userid = user_response.get('xata_id')
        else:
            return {"success": False, "message": "User not found."}

        async with db.pool.acquire() as con:
            async with con.transaction():
                # Step 1: Process all nested storage items in a single pass before the main records
                all_incoming_items = [
                    item for record in storage_records for item in record.get('storage_items', [])
                ]
                await sync_all_storage_items(con, all_incoming_items)

                # Step 2: Perform the UPSERT for the main 'storages' table
                await upsert_storage_records(con, "storages", storage_records, userid)
                
        end_time = time.perf_counter()
        logger.info(f"Finished processing storage data in {end_time - start_time:.2f} seconds.")
        return {
            "success": True, 
            "message": f"Processed {len(storage_records)} storages. Items synced.",
            "sync_results": {"sync_completed": True}
        }
    except Exception as e:
        logger.error(f"Error processing storage data: {e}", exc_info=True)
        raise

# Helper Functions
# ----------------------------------------------------------------------------------------------------------------------

async def upsert_storage_records(con: asyncpg.Connection, table_name: str, records: List[Dict[str, Any]], userid: str):
    """
    Performs a bulk UPSERT (INSERT or UPDATE) on the main 'storages' table.
    """
    if not records:
        return
    
    upsert_records = []
    for record in records:
        temp_record = record.copy()
        temp_record.pop('storage_items', None)
        temp_record['userid'] = userid
        upsert_records.append(temp_record)

    keys = list(upsert_records[0].keys())
    keys_str = ', '.join(keys)
    values_placeholders = ', '.join([f'${i+1}' for i in range(len(keys))])
    set_clause = ", ".join([f"{key} = EXCLUDED.{key}" for key in keys])
    
    query = f"""
    INSERT INTO {table_name} ({keys_str})
    VALUES ({values_placeholders})
    ON CONFLICT (storageid) DO UPDATE SET
        {set_clause};
    """
    
    records_as_tuples = [tuple(rec.get(key) for key in keys) for rec in upsert_records]
    
    try:
        await con.executemany(query, records_as_tuples)
        logger.info(f"UPSERT for {len(records_as_tuples)} storages records completed successfully.")
    except Exception as e:
        logger.error(f"Database error during storages UPSERT: {e}", exc_info=True)
        raise

async def sync_all_storage_items(con: asyncpg.Connection, incoming_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Handles all sync operations (UPSERT, DELETE) for nested storage items in a single pass.
    """
    TABLE_NAME = "storage_items"
    
    if not incoming_items:
        return {"message": "No storage items to sync."}
    
    # Generate composite keys and check for duplicates
    incoming_items_map = {}
    for item in incoming_items:
        storage_id = item.get('storageid')
        material_id = item.get('materialid')
        if storage_id and material_id:
            composite_key = f"{storage_id}-{material_id}"
            item["compositekey"] = composite_key
            incoming_items_map[composite_key] = item
    
    # Get all existing items for the incoming keys
    incoming_composite_keys = list(incoming_items_map.keys())
    storage_ids = [key.split('-')[0] for key in incoming_composite_keys]

    existing_items_data = await con.fetch(
        "SELECT xata_id, compositekey FROM storage_items WHERE storageid = ANY($1::text[]);",
        storage_ids
    )
    existing_keys_map = {record['compositekey']: record['xata_id'] for record in existing_items_data}
    
    # Classify records for UPSERT and DELETE
    upsert_records = []
    keys_to_delete = [key for key in existing_keys_map.keys() if key not in incoming_composite_keys]
    
    for key, record in incoming_items_map.items():
        if key in existing_keys_map:
            record['xata_id'] = existing_keys_map[key]
        upsert_records.append(record)

    # Perform the UPSERT operation
    await upsert_storage_items(con, TABLE_NAME, upsert_records)
    
    # Perform the DELETE operation for removed items
    if keys_to_delete:
        await bulk_delete_storage_items(con, TABLE_NAME, keys_to_delete)

    return {"success": True, "message": "Storage items synced successfully."}

async def upsert_storage_items(con: asyncpg.Connection, table_name: str, records: List[Dict[str, Any]]):
    """
    Performs a bulk UPSERT (INSERT or UPDATE) on the 'storage_items' table.
    """
    if not records:
        return
    
    keys = list(records[0].keys())
    keys_str = ', '.join(keys)
    values_placeholders = ', '.join([f'${i+1}' for i in range(len(keys))])
    set_clause = ", ".join([f"{key} = EXCLUDED.{key}" for key in keys if key != 'compositekey'])
    
    query = f"""
    INSERT INTO {table_name} ({keys_str})
    VALUES ({values_placeholders})
    ON CONFLICT (compositekey) DO UPDATE SET
        {set_clause};
    """
    
    records_as_tuples = [tuple(rec.get(key) for key in keys) for rec in records]
    
    try:
        await con.executemany(query, records_as_tuples)
        logger.info(f"UPSERT for {len(records_as_tuples)} storage items completed successfully.")
    except Exception as e:
        logger.error(f"Database error during storage items UPSERT: {e}", exc_info=True)
        raise

async def bulk_delete_storage_items(con: asyncpg.Connection, table_name: str, composite_keys: List[str]):
    """
    Performs a bulk DELETE operation for a list of composite keys.
    """
    logger.info(f'Deleting {len(composite_keys)} items!!!!')
    if not composite_keys:
        return
        
    query = f"DELETE FROM {table_name} WHERE compositekey = ANY($1::text[]);"
    
    try:
        await con.execute(query, list(composite_keys))
        logger.info(f"Bulk DELETE for {len(composite_keys)} storage items completed successfully.")
    except Exception as e:
        logger.error(f"Database error during bulk DELETE: {e}", exc_info=True)
        raise