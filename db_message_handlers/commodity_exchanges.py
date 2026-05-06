import logging
import time
from typing import Any, Dict, List

import asyncpg

from db import Database

logger = logging.getLogger(__name__)

async def handle_commodity_exchanges_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.debug("Starting processing commodity exchanges data.")

    converted_data = raw_payload.get("data")
    if not converted_data:
        logger.debug("No commodity exchanges records in payload. Exiting.")
        return {"success": True, "message": "No commodity exchanges records to process."}

    try:
        exchanges_to_upsert = converted_data
        async with db.pool.acquire() as con:
            async with con.transaction():
                await _upsert_records(
                    con, 
                    "commodity_exchanges", 
                    exchanges_to_upsert, 
                    ["id"] 
                )

    except Exception as e:
        logger.error(f"Error processing commodity exchanges data: {e}", exc_info=True)
        raise

    except Exception as e:
        logger.error(f"Failed to trigger dashboard update: {e}")

    end_time = time.perf_counter()
    logger.debug(f"Processing commodity exchanges records took {end_time - start_time:.4f} seconds")

    return {
        "success": True,
        "message": f"Processed {len(converted_data)} commodity exchanges successfully.",
    }

async def _upsert_records(
    con: asyncpg.Connection,
    table_name: str,
    records: List[Dict[str, Any]],
    unique_fields: List[str],
    chunk_size: int = 100,
):
    """
    A generic helper function to perform bulk upserts using INSERT ... ON CONFLICT DO UPDATE.
    """
    if not records:
        return

    columns = list(records[0].keys())
    columns_str = ", ".join(columns)
    unique_columns_str = ", ".join(unique_fields)

    # Create SET clause for update (exclude unique fields)
    set_clauses = [f"{col} = EXCLUDED.{col}" for col in columns if col not in unique_fields]
    set_clause_str = ", ".join(set_clauses)

    values_placeholders = ", ".join([f"${i + 1}" for i in range(len(columns))])

    if set_clause_str:
        on_conflict_clause = f"ON CONFLICT ({unique_columns_str}) DO UPDATE SET {set_clause_str}"
    else:
        on_conflict_clause = f"ON CONFLICT ({unique_columns_str}) DO NOTHING"

    query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_placeholders}) {on_conflict_clause};"

    for i in range(0, len(records), chunk_size):
        chunk = records[i : i + chunk_size]
        values_to_insert = [list(rec.values()) for rec in chunk]
        try:
            await con.executemany(query, values_to_insert)
        except Exception as e:
            logger.error(f"Database error during UPSERT on {table_name}: {e}", exc_info=True)
            raise