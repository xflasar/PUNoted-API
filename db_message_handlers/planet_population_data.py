import logging
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)


async def _prepare_and_execute_upsert(
    con, table_name: str, records: List[Dict[str, Any]], conflict_keys: List[str]
) -> int:
    """
    Prepares data and executes a bulk UPSERT (INSERT ... ON CONFLICT DO UPDATE)
    operation using asyncpg.executemany.

    :param con: asyncpg.Connection object.
    :param table_name: The target PostgreSQL table name.
    :param records: List of dictionary records to UPSERT.
    :param conflict_keys: List of column names forming the unique key for conflict resolution.
    :return: The number of rows processed by executemany.
    """
    if not records:
        return 0

    # Ensure all records have the same keys for correct tuple generation
    # Keys will be ordered as per the first record
    keys = list(records[0].keys())

    # Prepare list of tuples for executemany
    values_list: List[Tuple] = [tuple(rec.values()) for rec in records]

    # Create column and placeholder strings
    columns = ", ".join(keys)
    placeholders = ", ".join([f"${i + 1}" for i in range(len(keys))])

    # Create the UPDATE SET clause for all non-conflict key columns
    update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in keys if col not in conflict_keys])

    # Construct the UPSERT query
    query = f"""
        INSERT INTO {table_name} ({columns}) 
        VALUES ({placeholders})
        ON CONFLICT ({", ".join(conflict_keys)}) 
        DO UPDATE SET {update_clause};
    """

    # Execute the bulk UPSERT
    try:
        await con.executemany(query, values_list)
    except Exception as e:
        logger.error(f"Logged error in pop execute: {e}", exc_info=True)
        raise

async def handle_planet_population_data_message(db, data: Dict[str, Any]):
    """
    Converts raw population data and performs an atomic bulk UPSERT
    for infrastructures and reports using asyncpg.executemany.

    :param db: An asyncpg.Connection or asyncpg.Pool connection object (or wrapper).
    :param data: The raw message data dictionary.
    :return: Status dictionary.
    """

    converted_data = data["data"]

    infrastructure_records: List[Dict[str, Any]] = converted_data["infrastructures"]
    population_records: List[Dict[str, Any]] = converted_data["populations"]

    inserted_infra_count = 0
    inserted_pop_count = 0

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                # --- 1. Bulk UPSERT INFRASTRUCTURES ---
                # Conflict Key: (populationid, type, projectid)
                if infrastructure_records:
                    await _prepare_and_execute_upsert(
                        con,
                        table_name="planet_infrastructures",
                        records=infrastructure_records,
                        conflict_keys=["populationid", "type", "projectid"],
                    )
                    logger.debug(f"UPSERTED {len(infrastructure_records)} planet infrastructures.")

                # --- 2. Bulk UPSERT POPULATION REPORTS ---
                # Conflict Key: (populationid, time)

                # Filter out records missing a 'time' value as it's part of the primary key
                valid_population_records = [rec for rec in population_records if rec.get("time") is not None]

                if valid_population_records:
                    await _prepare_and_execute_upsert(
                        con,
                        table_name="planet_populations",
                        records=valid_population_records,
                        conflict_keys=["populationid", "time"],
                    )
                    logger.debug(f"UPSERTED {len(valid_population_records)} planet population reports.")

        return {
            "status": "success",
            "message": f"Successfully upserted {len(infrastructure_records)} infrastructures and {len(valid_population_records)} population reports.",
        }
    except Exception as e:
        logger.error(e)
        return {"status": "fail", "message": f"Rip {e}"}
