import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

async def handle_material_categories_message(db_wrapper, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Asynchronously handles inserting/updating material categories and materials.
    Uses a single transaction to ensure atomicity.
    """
    converted_data = raw_payload.get("data")
    if not converted_data:
         return {"success": False, "message": "No data found in payload."}

    category_records = converted_data.get("material_categories", [])
    material_records = converted_data.get("materials", [])

    try:
        # Acquire a connection from the pool
        async with db_wrapper.pool.acquire() as con:
            # Start transaction
            async with con.transaction():

                # --- Step 1: Material Categories ---
                if category_records:
                    logger.debug(f"Processing {len(category_records)} records for 'material_categories'...")

                    raw_keys = list(category_records[0].keys())

                    # MAPPING: JSON 'categoryid' -> DB 'id'
                    db_cols = []
                    for k in raw_keys:
                        if k == 'categoryid':
                            db_cols.append('id')
                        else:
                            db_cols.append(k)

                    cols_str = ", ".join(db_cols)
                    vals_str = ", ".join([f"${i + 1}" for i in range(len(db_cols))])

                    # Update clause (exclude PK 'id')
                    update_set = ", ".join([
                        f"{col} = EXCLUDED.{col}"
                        for col in db_cols if col != 'id'
                    ])

                    cat_query = f"""
                        INSERT INTO material_categories ({cols_str}) 
                        VALUES ({vals_str}) 
                        ON CONFLICT (id) 
                        DO UPDATE SET {update_set};
                    """

                    cat_values = [[rec[k] for k in raw_keys] for rec in category_records]

                    await con.executemany(cat_query, cat_values)
                    logger.debug("Bulk upsert completed for 'material_categories'.")
                else:
                    logger.debug("No 'material_categories' records to process.")


                # --- Step 2: Materials ---
                if material_records:
                    logger.debug(f"Processing {len(material_records)} records for 'materials'...")

                    # NO MAPPING NEEDED: DB column is 'materialid', matches JSON.
                    keys = list(material_records[0].keys())

                    cols_str = ", ".join(keys)
                    vals_str = ", ".join([f"${i + 1}" for i in range(len(keys))])

                    # Update clause (exclude PK 'materialid')
                    update_set = ", ".join([
                        f"{k} = EXCLUDED.{k}"
                        for k in keys if k != 'materialid'
                    ])

                    mat_query = f"""
                        INSERT INTO materials ({cols_str}) 
                        VALUES ({vals_str}) 
                        ON CONFLICT (materialid) 
                        DO UPDATE SET {update_set};
                    """

                    mat_values = [[rec[k] for k in keys] for rec in material_records]

                    await con.executemany(mat_query, mat_values)
                    logger.debug("Bulk upsert completed for 'materials'.")
                else:
                    logger.debug("No 'materials' records to process.")

        return {
            "success": True,
            "message": "Bulk upsert completed successfully.",
        }

    except Exception as e:
        logger.error(f"Error processing material data: {e}", exc_info=True)
        raise
