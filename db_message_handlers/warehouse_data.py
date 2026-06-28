import logging
import time
from typing import Any, Dict

from db import Database

logger = logging.getLogger(__name__)


async def handle_warehouse_data_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.debug("Starting processing warehouse data.")

    converted_data = raw_payload.get("data", [])
    table_name = "warehouses"

    if not converted_data:
        logger.debug("No warehouse records to process.")
        return {"success": True, "message": "No warehouse records to process."}

    try:
        user_response = await db.fetch_one(
            "SELECT accountid, userdataid FROM users WHERE accountid = $1;",
            raw_payload.get("userId"),
        )
        
        if user_response and user_response.get("userdataid") is not None:
            userid = str(user_response.get("userdataid"))
        elif user_response:
            userid = str(user_response.get("accountid"))
        else:
            return {"success": False, "message": "User not found."}

        valid_records = []
        for record in converted_data:
            if record.get("warehouseid") and record.get("storeid"):
                record["userid"] = userid
                valid_records.append(record)

        if not valid_records:
            return {"success": False, "message": "No valid warehouse records with warehouseid and storeid found."}

        keys = list(valid_records[0].keys())
        columns_str = ", ".join(keys)
        placeholders_str = ", ".join([f"${i + 1}" for i in range(len(keys))])
        update_keys = [k for k in keys if k not in ("warehouseid", "storeid")]
        update_set_str = ", ".join([f"{k} = EXCLUDED.{k}" for k in update_keys])
        
        query = f"""
            INSERT INTO {table_name} ({columns_str}) 
            VALUES ({placeholders_str})
            ON CONFLICT (warehouseid, storeid) 
            DO UPDATE SET {update_set_str};
        """
        
        values_list = [[rec[k] for k in keys] for rec in valid_records]

        async with db.pool.acquire() as con:
            async with con.transaction():
                await con.executemany(query, values_list)

        end_time = time.perf_counter()
        logger.debug(f"Processing warehouse records took {end_time - start_time:.4f} seconds")

        return {
            "success": True,
            "message": f"Processed {len(valid_records)} warehouse records successfully via bulk UPSERT.",
        }

    except Exception as e:
        logger.error(f"Error processing warehouse data: {e}", exc_info=True)
        raise