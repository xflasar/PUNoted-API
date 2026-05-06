import logging
import time
from typing import Any, Dict, List

import asyncpg

from db import Database

logger = logging.getLogger(__name__)


async def upsert_record(conn: asyncpg.Connection, table_name: str, data: List[Dict[str, Any]]):
    """Performs a batched INSERT or UPDATE (UPSERT) for a given table."""
    if not data:
        return

    # Dynamically extract column names and prepare values for executemany
    columns = data[0].keys()
    column_names = ", ".join(columns)
    placeholders = ", ".join([f"${i + 1}" for i in range(len(columns))])
    values = [tuple(record.values()) for record in data]

    # --- Conflict Resolution Logic ---
    query = ""
    if table_name == "material_processes":
        # PK: process_id. If it exists, assume the definition is immutable.
        query = f"""
            INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})
            ON CONFLICT (processid) DO NOTHING;
        """
    elif table_name == "process_material_io":
        # PK: (process_id, material_id, io_type). Update amount on conflict.
        query = f"""
            INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})
            ON CONFLICT (processid, materialid, iotype) DO UPDATE
            SET amount = EXCLUDED.amount;
        """
    elif table_name == "recipes":
        # PK: material_id. Update the list of process IDs on conflict.
        query = f"""
            INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})
            ON CONFLICT (materialid) DO UPDATE
            SET 
                input_recipe_ids = EXCLUDED.input_recipe_ids, 
                output_recipe_ids = EXCLUDED.output_recipe_ids;
        """
    else:
        logger.warning(f"Attempted upsert for unknown table: {table_name}")
        return
    await conn.executemany(query, values)


# --- Main Handler Function ---


async def handle_material_recipe_message(db: Database, data: Dict[str, Any]):
    """
    Handles material recipe data, assuming 'data' is already converted to the
    flat records structure required for batch DB insertion.
    """
    start_time = time.perf_counter()

    flat_data = data.get("data")

    # 2. Database Insertion
    try:
        # Check structure before proceeding to prevent KeyError in transaction
        required_keys = ["recipes", "material_processes", "process_material_io"]
        if not all(k in flat_data for k in required_keys):
            raise ValueError(f"Incoming data is missing required keys: {required_keys}")

        async with db.pool.acquire() as conn:
            async with conn.transaction():
                # A. material_processes (Must be first, FK-dependency: None)
                await upsert_record(conn, "material_processes", flat_data["material_processes"])

                # B. process_material_io (References process_id from step A)
                await upsert_record(conn, "process_material_io", flat_data["process_material_io"])

                # C. recipes (References material_id and contains process_id arrays from step A)
                await upsert_record(conn, "recipes", flat_data["recipes"])

    except Exception as e:
        logger.error(f"Error processing material recipe data: {e}", exc_info=True)
        raise

    end_time = time.perf_counter()
    logger.debug(f"Processing material recipe records took {end_time - start_time:.4f} seconds")
    return {}
