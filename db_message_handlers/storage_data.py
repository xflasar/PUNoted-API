import logging
import time
from typing import Any, Dict, List
from db import Database

logger = logging.getLogger(__name__)

"""
    Damn it all needs rewriting Takes too long to process data
    Needs rewriting to use transaction
"""

async def handle_storage_data_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Asynchronously processes storage data, performing bulk inserts and updates
    in a single transaction.
    """
    start_time = time.perf_counter()
    logger.info("Starting processing storage data.")

    converted_data = raw_payload["data"]
    table_name = "storages"
    storage_records = converted_data.get("storages")
    
    if not storage_records:
        logger.info("No storage records to process.")
        return {"success": True, "message": "No storage records to process."}
    
    try:
        # Get user ID and userdataid
        user_response = await db.fetch_one(
            "SELECT xata_id, userdataid FROM users WHERE xata_id = $1;",
            raw_payload["userId"]
        )
        if user_response and user_response.get('userdataid') is not None:
            userid = user_response.get('userdataid')
        elif user_response:
            userid = user_response.get('xata_id')
        else:
            return {"success": False, "message": "User not found."}

        incoming_storage_ids = [record.get('storageid') for record in storage_records if record.get('storageid')]
        if not incoming_storage_ids:
            return {"success": False, "message": "No valid storage IDs found."}

        # Query for existing storage records in bulk
        query_response = await db.fetch_rows(
            f"SELECT storageid, xata_id FROM {table_name} WHERE storageid = ANY($1::text[]);",
            incoming_storage_ids
        )
        
        existing_ids_map = {record['storageid']: record['xata_id'] for record in query_response}
        existing_storage_ids = set(existing_ids_map.keys())

        records_to_insert = []
        records_to_update = []

        for record in storage_records:
            storage_id = record.get('storageid')
            if not storage_id:
                continue

            temp_record = record.copy()
            if 'storage_items' in temp_record:
                del temp_record['storage_items']
            #if 'userid' not in temp_record:
            temp_record['userid'] = userid

            if storage_id not in existing_storage_ids:
                records_to_insert.append(temp_record)
            else:
                temp_record['storageid'] = existing_ids_map[storage_id]
                records_to_update.append(temp_record)
        
        # Perform bulk inserts
        if records_to_insert:
            logger.info(f"Found {len(records_to_insert)} new storage records. Performing bulk insert.")
            keys = ', '.join(records_to_insert[0].keys())
            values_placeholders = ', '.join([f'${i+1}' for i in range(len(records_to_insert[0]))])
            query = f"INSERT INTO {table_name} ({keys}) VALUES ({values_placeholders}) ON CONFLICT DO NOTHING;"
            for rec_values in [list(rec.values()) for rec in records_to_insert]:
                await db.execute(query, *rec_values)

        # Perform bulk updates
        if records_to_update:
            logger.info(f"Found {len(records_to_update)} existing storage records. Performing bulk update.")
            
            for record_to_update in records_to_update:
                update_data = record_to_update.copy()
                record_id = update_data.pop('storageid')
                update_fields = ", ".join([f"{key} = ${i+2}" for i, key in enumerate(update_data.keys())])
                query = f"UPDATE {table_name} SET {update_fields} WHERE xata_id = $1;"
                await db.execute(query, record_id, *update_data.values())

        # Handle nested storage items
        sync_results = {}
        for record in storage_records:
            storage_id = record.get('storageid')
            if storage_id:
                incoming_items = record.get('storage_items', [])
                sync_results[storage_id] = await sync_storage_items(db, storage_id, incoming_items)
        
        end_time = time.perf_counter()
        logger.info(f"Finished processing storage data in {end_time - start_time:.2f} seconds.")
        return {
            "success": True, 
            "message": f"Processed {len(storage_records)} storages. Items synced.",
            "sync_results": sync_results
        }

    except Exception as e:
        logger.error(f"Error processing storage data: {e}", exc_info=True)
        # Re-raise to let the Celery task handle the failure and potential transaction rollback
        raise

async def sync_storage_items(db: Database, storage_id: str, incoming_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Asynchronously synchronizes a storage's items by identifying and performing
    bulk inserts, updates, and deletes, handling duplicates gracefully.
    """
    TABLE_NAME = "storage_items"
    
    incoming_items_map = {}
    for item in incoming_items:
        material_id = item.get('materialid')
        if material_id:
            composite_key = f"{storage_id}-{material_id}"
            item["compositekey"] = composite_key
            item["storageid"] = storage_id
            incoming_items_map[composite_key] = item
    
    incoming_composite_keys = set(incoming_items_map.keys())

    all_existing_items = await db.fetch_rows(
        f"SELECT xata_id, compositekey FROM {TABLE_NAME} WHERE storageid = $1;",
        storage_id
    )
    all_existing_keys = {record['compositekey'] for record in all_existing_items}
    existing_records_map = {record['compositekey']: record['xata_id'] for record in all_existing_items}

    to_insert_keys = incoming_composite_keys - all_existing_keys
    to_update_keys = incoming_composite_keys.intersection(all_existing_keys)
    to_delete_keys = all_existing_keys - incoming_composite_keys
    
    records_to_insert = [incoming_items_map[key] for key in to_insert_keys]
    records_to_update = [incoming_items_map[key] for key in to_update_keys]
    records_to_delete_ids = [existing_records_map[key] for key in to_delete_keys]

    try:
        # 1. Perform bulk inserts using ON CONFLICT DO NOTHING
        if records_to_insert:
            valid_keys = records_to_insert[0].keys()
            keys_str = ', '.join(valid_keys)
            values_placeholders_str = ', '.join([f'${i+1}' for i in range(len(valid_keys))])
            
            query = f"INSERT INTO {TABLE_NAME} ({keys_str}) VALUES ({values_placeholders_str}) ON CONFLICT (compositekey) DO NOTHING;"
            
            records_as_tuples = [tuple(rec.values()) for rec in records_to_insert]
            await db.executemany(query, records_as_tuples)


        # 2. Perform bulk updates
        if records_to_update:
            for record_to_update in records_to_update:
                update_data = {
                    key: record_to_update[key] 
                    for key in record_to_update 
                    if key not in ['compositekey', 'storageid', 'xata_id']
                }
                update_fields = ", ".join([f"{key} = ${i+1}" for i, key in enumerate(update_data.keys())])
                
                # Dynamically build the list of values to match the placeholders
                values = list(update_data.values())
                values.append(record_to_update['compositekey'])
                
                query = f"UPDATE {TABLE_NAME} SET {update_fields} WHERE compositekey = ${len(values)};"
                
                await db.execute(query, *values)

        # 3. Perform bulk deletes
        if records_to_delete_ids:
            query = f"DELETE FROM {TABLE_NAME} WHERE xata_id = ANY($1::text[]);"
            await db.execute(query, records_to_delete_ids)
        
        logger.info(f"Transaction for storage '{storage_id}' completed: {len(records_to_insert)} inserts, {len(records_to_update)} updates, {len(records_to_delete_ids)} deletes.")
        return {"success": True, "message": f"Transaction for storage '{storage_id}' completed."}
    
    except Exception as e:
        logger.error(f"Transaction failed for storage '{storage_id}': {e}", exc_info=True)
        raise