import logging
import time
from typing import Any, Dict, List
from db import Database
import asyncpg

logger = logging.getLogger(__name__)

async def handle_comex_orders_data_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.info("Starting processing comex orders data.")

    converted_data = raw_payload.get("data")
    if not converted_data:
        logger.info("No comex orders records in payload. Exiting.")
        return {"success": True, "message": "No comex orders records to process."}

    try:
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
        
        # --- Step 1: Collect and transform all records ---
        comex_orders_to_upsert = []
        comex_orders_trades_to_upsert = []
        
        for record in converted_data:
            order_id = record.get('orderid')
            if not order_id:
                continue

            record['userid'] = userid

            if 'trades' in record:
                trades = record.pop('trades')
                for trade in trades:
                    trade['orderid'] = order_id
                    comex_orders_trades_to_upsert.append(trade)

            comex_orders_to_upsert.append(record)

        # --- Step 2: Perform all upserts in a single transaction ---
        async with db.pool.acquire() as con:
            async with con.transaction():
                # Upsert the main comex orders table
                await _upsert_records(con, "comex_trade_orders", comex_orders_to_upsert, ['orderid'])

                # Upsert and handle deletions for the child table.
                if comex_orders_trades_to_upsert:
                    await _upsert_records(
                        con,
                        "comex_trade_orders_trades",
                        comex_orders_trades_to_upsert,
                        ['tradeid']
                    )
    except Exception as e:
        logger.error(f"Error processing comex orders data: {e}", exc_info=True)
        raise

    end_time = time.perf_counter()
    logger.info(f"Processing comex orders records took {end_time - start_time:.4f} seconds")
    
    return {
        "success": True, 
        "message": f"Processed {len(converted_data)} comex orders records successfully."
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