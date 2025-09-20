import logging
import time
from typing import Any, Dict, List
from db import Database
import asyncpg

logger = logging.getLogger(__name__)

async def handle_cx_broker_data_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.info("Starting processing cx_broker data.")

    converted_data = raw_payload.get("data")
    if not converted_data:
        logger.info("No cx_broker records in payload. Exiting.")
        return {"success": True, "message": "No cx_broker records to process."}

    try:
        # --- Step 1: Collect and transform all records ---
        # The main broker record is a single dictionary
        cx_broker_data_to_upsert = converted_data
        
        # Buy and sell orders are nested lists
        buy_orders_to_insert = converted_data[0].pop('buy')
        sell_orders_to_insert = converted_data[0].pop('sell')
        
        # Add the parent brokermaterialid to each nested order record
        brokermaterialid = converted_data[0].get('brokermaterialid')
        for order in buy_orders_to_insert:
            order['brokermaterialid'] = brokermaterialid
            
        for order in sell_orders_to_insert:
            order['brokermaterialid'] = brokermaterialid

        # --- Step 2: Perform all upserts and deletions in a single transaction ---
        async with db.pool.acquire() as con:
            async with con.transaction():
                # Upsert the main cx_brokers table
                await _upsert_records(con, "cx_brokers", cx_broker_data_to_upsert, ['brokermaterialid'])

                # Handle deletions and re-insertion for buy orders
                await _delete_and_insert(con, "cx_brokers_buy_orders", buy_orders_to_insert, 'brokermaterialid', brokermaterialid)

                # Handle deletions and re-insertion for sell orders
                await _delete_and_insert(con, "cx_brokers_sell_orders", sell_orders_to_insert, 'brokermaterialid', brokermaterialid)
                
    except Exception as e:
        logger.error(f"Error processing cx_broker data: {e}", exc_info=True)
        raise

    end_time = time.perf_counter()
    logger.info(f"Processing cx_broker records took {end_time - start_time:.4f} seconds")
    
    return {
        "success": True, 
        "message": f"Processed cx_broker record successfully."
    }

async def _upsert_records(
    con: asyncpg.Connection,
    table_name: str,
    records: List[Dict[str, Any]],
    unique_fields: List[str],
    chunk_size: int = 100
):
    """
    A generic helper function to perform bulk upserts using INSERT ... ON CONFLICT DO UPDATE.
    It processes records in chunks to prevent large transaction overhead.
    """
    if not records:
        return

    columns = records[0].keys()
    columns_str = ', '.join(columns)
    unique_columns_str = ', '.join(unique_fields)
    
    set_clauses = [f"{col} = EXCLUDED.{col}" for col in columns if col not in unique_fields]
    set_clause_str = ', '.join(set_clauses)
    
    values_placeholders = ', '.join([f'${i+1}' for i in range(len(columns))])
    
    if set_clause_str:
        on_conflict_clause = f"ON CONFLICT ({unique_columns_str}) DO UPDATE SET {set_clause_str}"
    else:
        on_conflict_clause = f"ON CONFLICT ({unique_columns_str}) DO NOTHING"
        
    query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_placeholders}) {on_conflict_clause};"
    
    for i in range(0, len(records), chunk_size):
        chunk = records[i:i + chunk_size]
        values_to_insert = [list(rec.values()) for rec in chunk]
        try:
            await con.executemany(query, values_to_insert)
        except Exception as e:
            logger.error(f"Database error during UPSERT: {e}", exc_info=True)
            raise
    logger.info(f"Finished inserting data in upsert_records.")

async def _delete_and_insert(
    con: asyncpg.Connection,
    table_name: str,
    records: List[Dict[str, Any]],
    parent_id_field: str,
    parent_id: str
):
    """
    Deletes all records for a given parent ID and then inserts the new records.
    """
    # Delete existing records for the parent ID
    delete_query = f"DELETE FROM {table_name} WHERE {parent_id_field} = $1;"
    try:
        await con.execute(delete_query, parent_id)
    except Exception as e:
        logger.error(f"Database error during UPSERT: {e}", exc_info=True)
        raise
    # Insert the new records if the list is not empty
    if records:
        columns = records[0].keys()
        columns_str = ', '.join(columns)
        values_placeholders = ', '.join([f'${i+1}' for i in range(len(columns))])
        
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_placeholders});"
        
        # Use executemany for bulk insertion
        values_to_insert = [list(rec.values()) for rec in records]
        try:
            await con.executemany(insert_query, values_to_insert)
        except Exception as e:
            logger.error(f"Database error during UPSERT: {e}", exc_info=True)
            raise
    logger.info(f"Finished processing {len(records)} records for {table_name}.")