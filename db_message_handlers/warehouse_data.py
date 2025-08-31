import logging
import time
from typing import Any, Dict
from db import Database

logger = logging.getLogger(__name__)

"""
    Needs rewriting to use transaction
"""

async def handle_warehouse_data_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.info("Starting processing warehouse data.")

    converted_data = raw_payload["data"]
    table_name = "warehouses"

    if not converted_data:
        logger.info("No warehouse records to process.")
        return {"success": True, "message": "No warehouse records to process."}
    
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

        incoming_storage_ids = [record.get('warehouseid') for record in converted_data if record.get('warehouseid')]
        if not incoming_storage_ids:
            return {"success": False, "message": "No valid werehouse IDs found."}

        # Query for existing werehouse records in bulk
        query_response = await db.fetch_rows(
            f"SELECT warehouseid, xata_id FROM {table_name} WHERE warehouseid = ANY($1::text[]);",
            incoming_storage_ids
        )
        
        existing_ids_map = {record['warehouseid']: record['xata_id'] for record in query_response}
        existing_storage_ids = set(existing_ids_map.keys())

        records_to_insert = []
        records_to_update = []

        for record in converted_data:
            storage_id = record.get('warehouseid')
            record['userid'] = userid
            if not storage_id:
                continue

            temp_record = record.copy()

            if storage_id not in existing_storage_ids:
                records_to_insert.append(temp_record)
            else:
                temp_record['warehouseid'] = existing_ids_map[storage_id]
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
                record_id = update_data.pop('warehouseid')
                update_fields = ", ".join([f"{key} = ${i+2}" for i, key in enumerate(update_data.keys())])
                query = f"UPDATE {table_name} SET {update_fields} WHERE xata_id = $1;"
                # 🌟 FIX: Add await
                await db.execute(query, record_id, *update_data.values())
        end_time = time.perf_counter()
        logger.info(f"Processing warehouse records took {end_time - start_time:.4f} seconds")
        
        return {
            "success": True, 
            "message": f"Processed {len(converted_data)} . {len(records_to_insert)} items inserted | {len(records_to_update)} updated.",
        }
    except Exception as e:
        logger.error(f"Error processing warehouse data: {e}", exc_info=True)
        raise