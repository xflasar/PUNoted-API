import logging
import time
from typing import Any, Dict, List
from db import Database
import asyncpg

logger = logging.getLogger(__name__)

async def handle_ship_data_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.info("Starting processing ship data.")

    converted_data = raw_payload.get("data")
    if not converted_data:
        logger.info("No ship records in payload. Exiting.")
        return {"success": True, "message": "No ship records to process."}

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
        ships_to_upsert = []
        ship_repair_materials_to_upsert = []
        
        for record in converted_data:
            ship_id = record.get('shipid')
            if not ship_id:
                continue

            record['userid'] = userid

            if 'repair_materials' in record:
                repair_materials = record.pop('repair_materials')
                for material in repair_materials:
                    material['shipid'] = ship_id
                    ship_repair_materials_to_upsert.append(material)

            ships_to_upsert.append(record)

        # --- Step 2: Perform all upserts in a single transaction ---
        async with db.pool.acquire() as con:
            async with con.transaction():
                # Upsert the main ships table
                await _upsert_records(con, "ships", ships_to_upsert, ['shipid'])

                # Upsert and handle deletions for the child table.
                if ship_repair_materials_to_upsert:
                    await _upsert_records(
                        con,
                        "ship_repair_materials",
                        ship_repair_materials_to_upsert,
                        ['shipid', 'materialid']
                    )

                # Targeted deletion for the repair materials
                for ship in ships_to_upsert:
                    # Collect only the materials for this specific ship
                    materials_for_this_ship = [
                        mat for mat in ship_repair_materials_to_upsert if mat['shipid'] == ship['shipid']
                    ]
                    await _handle_deletions_by_parent(
                        con,
                        "ship_repair_materials",
                        "shipid",
                        ship['shipid'],
                        ['shipid', 'materialid'],
                        materials_for_this_ship
                    )


    except Exception as e:
        logger.error(f"Error processing ship data: {e}", exc_info=True)
        raise

    end_time = time.perf_counter()
    logger.info(f"Processing ship records took {end_time - start_time:.4f} seconds")
    
    return {
        "success": True, 
        "message": f"Processed {len(converted_data)} ship records successfully."
    }

async def _upsert_records(
    con: asyncpg.Connection,
    table_name: str,
    records: List[Dict[str, Any]],
    unique_fields: List[str],
    chunk_size: int = 5000
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


async def _handle_deletions_by_parent(
    con: asyncpg.Connection,
    table_name: str,
    parent_id_field: str,
    parent_id: str,
    unique_fields: List[str],
    current_records: List[Dict[str, Any]]
):
    """
    Deletes records for a given parent ID (e.g., userid) that are not present
    in the new payload for that parent.
    """
    if not current_records:
        delete_query = f"DELETE FROM {table_name} WHERE {parent_id_field} = $1;"
        try:
            await con.execute(delete_query, parent_id)
        except Exception as e:
            logger.error(f"Database error during UPSERT: {e}", exc_info=True)
            raise
        return

    payload_keys = {
        tuple(rec[field] for field in unique_fields)
        for rec in current_records
    }
    
    keys_query = f"SELECT {', '.join(unique_fields)} FROM {table_name} WHERE {parent_id_field} = $1;"
    existing_records = await con.fetch(keys_query, parent_id)
    existing_keys_in_db = {tuple(rec.values()) for rec in existing_records}

    keys_to_delete = existing_keys_in_db - payload_keys

    if not keys_to_delete:
        return

    delete_query = f"""
        DELETE FROM {table_name}
        WHERE ({', '.join(unique_fields)}) IN (
            SELECT UNNEST($1)
        );
    """
    
    values_to_delete = [key[0] for key in keys_to_delete]

    try:
        await con.execute(delete_query, values_to_delete)
    except Exception as e:
        logger.error(f"Database error during UPSERT: {e}", exc_info=True)
        raise