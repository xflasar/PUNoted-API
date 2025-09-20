import logging
import time
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

async def handle_production_lines_data_message(db, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.info("Starting processing production lines data.")
    converted_data = raw_payload["data"]

    site_id = converted_data['siteid']
    production_lines = converted_data['production_lines']
    if not production_lines or len(production_lines) == 0:
        return {"success": False, "message": "Invalid or missing production lines data in payload."}
    
    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                # check if we have production lines for siteid
                query = "SELECT xata_id, productionlineid FROM site_production_lines WHERE siteid=$1;"
                query_response = await db.fetch_rows(query, site_id )

                existing_production_lines = {record['productionlineid']: record['xata_id'] for record in query_response}
                existing_production_lines_ids = set(existing_production_lines.keys())

                records_to_insert = []
                records_to_update = []

                orders = []
                production_templates = []
                efficiency_factors = []
                workforces = []

                for record in production_lines:
                    production_line_id = record.get('productionlineid')
                    if not production_line_id:
                      continue

                    orders = record.get("orders", [])
                    production_templates = record.get("production_templates", [])
                    efficiency_factors = record.get("efficiency_factors", [])
                    workforces = record.get('workforces', [])

                    temp_record = record.copy()
                    if 'orders' in temp_record:
                        del temp_record['orders']
                    if 'production_templates' in temp_record:
                        del temp_record['production_templates']
                    if 'efficiency_factors' in temp_record:
                        del temp_record['efficiency_factors']
                    if 'workforces' in temp_record:
                        del temp_record['workforces']

                    if production_line_id not in existing_production_lines_ids:
                        records_to_insert.append(temp_record)
                    else:
                        temp_record['productionlineid'] = existing_production_lines[production_line_id]
                        records_to_update.append(temp_record)

                if records_to_insert:
                    logger.info(f"Found {len(records_to_insert)} new production lines. Performing bulk insert.")
                    keys = ', '.join(records_to_insert[0].keys())
                    values_placeholders = ', '.join([f'${i+1}' for i in range(len(records_to_insert[0]))])
                    query = f"INSERT INTO site_production_lines ({keys}) VALUES ({values_placeholders}) ON CONFLICT (productionlineid) DO NOTHING;"
                    for rec_values in [list(rec.values()) for rec in records_to_insert]:
                        try:
                          await con.execute(query, *rec_values)
                        except Exception as e:
                          logger.error(e)
                          raise

                if records_to_update:
                    logger.info(f"Found {len(records_to_update)} existing production lines. Performing bulk insert.")
                    for record_to_update in records_to_update:
                        update_data = record_to_update.copy()
                        record_id = update_data.pop('productionlineid')
                        update_fields = ", ".join([f"{key} = ${i+2}" for i, key in enumerate(update_data.keys())])
                        query = f"UPDATE site_production_lines SET {update_fields} WHERE productionlineid = $1;"
                        try:
                          await con.execute(query, record_id, *update_data.values())
                        except Exception as e:
                          logger.error(e)
                          raise
            
                # now we need to handle nested items
                await process_orders(con, db, orders)

        end_time = time.perf_counter()
        logger.info(f"Finished processing production lines data in {end_time - start_time:.2f} seconds.")
        return {
            "success": True, 
            "message": f"Processed production lines data."
        }
    except Exception as e:
        logger.error(f"Error processing planet data: {e}", exc_info=True)
        raise
    
async def process_production_templates(production_templates: List[Dict[str, Any]]):
    # this one will be run for some time then turned off since it's more like a static data
    return

async def process_orders(con, db, orders: List[Dict[str, Any]]):
    incoming_orders_ids = [record.get('orderid') for record in orders if record.get('orderid')]
    if not incoming_orders_ids:
        return {"success": False, "message": "No valid orders IDs found."}
    # Query for existing storage records in bulk
    query_response = await db.fetch_rows(
        f"SELECT orderid, xata_id FROM site_production_line_orders WHERE productionlineid = ANY($1::text[]);",
        incoming_orders_ids
    )
    
    existing_ids_map = {record['orderid']: record['xata_id'] for record in query_response}
    existing_orders_ids = set(existing_ids_map.keys())

    records_to_delete_ids = existing_ids_map.keys() - incoming_orders_ids

    records_to_insert = []
    records_to_update = []

    records_order_inputs = []
    records_order_outputs = []

    for record in orders:
        order_id = record.get('orderid')
        if not order_id:
            continue
        
        if 'inputs' in record:
            for rec in record.get('inputs'):
                records_order_inputs.append(rec)
            del record['inputs']
        if 'outputs' in record:
            for rec in record.get('outputs'):
                records_order_outputs.append(rec)
            del record['outputs']
        
        if order_id not in existing_orders_ids:
            records_to_insert.append(record)
        else:
            record['orderid'] = existing_ids_map[order_id]
            records_to_update.append(record)
    
    await process_bulk(con, "site_production_line_orders", records_to_insert, records_to_update, records_to_delete_ids)
    return

async def process_effficiency_factors(efficiency_factors: List[Dict[str, Any]]):
    return

async def process_workforce(workforce: List[Dict[str, Any]]):
    return

async def process_bulk(con, table_name, records_to_insert, records_to_update, records_to_delete_ids = None):
    # Perform bulk inserts
    if records_to_insert:
        logger.info(f"Found {len(records_to_insert)} new production line orders records. Performing bulk insert.")
        keys = ', '.join(records_to_insert[0].keys())
        values_placeholders = ', '.join([f'${i+1}' for i in range(len(records_to_insert[0]))])
        query = f"INSERT INTO {table_name} ({keys}) VALUES ({values_placeholders}) ON CONFLICT (orderid) DO NOTHING;"
        for rec_values in [list(rec.values()) for rec in records_to_insert]:
            try:
              await con.execute(query, *rec_values)
            except Exception as e:
                logger.error(e)
                raise

    # Perform bulk updates
    if records_to_update:
        logger.info(f"Found {len(records_to_update)} existing production line orders records. Performing bulk update.")
        
        for record_to_update in records_to_update:
            update_data = record_to_update.copy()
            record_id = update_data.pop('orderid')
            update_fields = ", ".join([f"{key} = ${i+2}" for i, key in enumerate(update_data.keys())])
            query = f"UPDATE {table_name} SET {update_fields} WHERE orderid = $1;"
            try:
              await con.execute(query, record_id, *update_data.values())
            except Exception as e:
                logger.error(e)
                raise

    if records_to_delete_ids:
        logger.info(f"Deleting {len(records_to_delete_ids)} from production line orders.")
        try:
          await con.execute(f"DELETE FROM {table_name} WHERE productionlineid = ANY($1::text[]);", records_to_delete_ids)
        except Exception as e:
                logger.error(e)
                raise