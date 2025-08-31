import logging
from typing import Any, Dict
import asyncpg

logger = logging.getLogger(__name__)

"""
    This is bad.. Rework completely this Currently not working
    Needs rewriting to use transaction
"""

def handle_material_categories_message(conn: asyncpg.Connection, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Asynchronously handles inserting/updating material categories and materials
    from a single payload using a single transaction?? and bulk upsert operations.
    """
    converted_data = raw_payload["data"]
    
    try:
        # Step 1: Process material categories with a bulk upsert
        category_records = converted_data.get("material_categories")
        if category_records:
            logger.info(f"Processing {len(category_records)} records for 'material_categories'...")
            
            # Get the keys and values from the first record to build the query
            keys = list(category_records[0].keys())
            keys_str = ', '.join(keys)
            values_placeholders = ', '.join([f'${i+1}' for i in range(len(keys))])
            
            # Assume 'categoryid' is the unique key for ON CONFLICT
            update_fields = ', '.join([f"{key} = EXCLUDED.{key}" for key in keys])
            
            # The ON CONFLICT DO UPDATE clause handles both inserts and updates efficiently
            upsert_query = f"""
                INSERT INTO material_categories ({keys_str}) 
                VALUES ({values_placeholders}) 
                ON CONFLICT (categoryid) DO UPDATE SET {update_fields};
            """
            
            # Execute the bulk upsert
            conn.executemany(upsert_query, [list(rec.values()) for rec in category_records])
            logger.info("Bulk upsert completed for 'material_categories'.")
        else:
            logger.info("No 'material_categories' records to process.")

        # Step 2: Process materials with a bulk upsert
        material_records = converted_data.get("materials")
        if material_records:
            logger.info(f"Processing {len(material_records)} records for 'materials'...")
            
            keys = list(material_records[0].keys())
            keys_str = ', '.join(keys)
            values_placeholders = ', '.join([f'${i+1}' for i in range(len(keys))])
            
            # Assume 'materialid' is the unique key for ON CONFLICT
            update_fields = ', '.join([f"{key} = EXCLUDED.{key}" for key in keys])
            
            upsert_query = f"""
                INSERT INTO materials ({keys_str}) 
                VALUES ({values_placeholders}) 
                ON CONFLICT (materialid) DO UPDATE SET {update_fields};
            """
            
            conn.executemany(upsert_query, [list(rec.values()) for rec in material_records])
            logger.info("Bulk upsert completed for 'materials'.")
        else:
            logger.info("No 'materials' records to process.")

        return {"success": True, "message": "Bulk upsert completed for material categories and materials."}

    except Exception as e:
        logger.error(f"Error processing material data: {e}", exc_info=True)
        raise