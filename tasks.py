# tasks.py
import asyncio
import json
import inspect
import logging
import time
from typing import Any, Dict, List

from cachetools import TTLCache

import data_converter
import db_message_handlers.user_data
import db_message_handlers.storage_data
import db_message_handlers.company_data
import db_message_handlers.material_categories
import db_message_handlers.planet_data
import db_message_handlers.sites_data
import db_message_handlers.warehouse_data
import db_message_handlers.ship_data
import db_message_handlers.cx_data_orders
import db_message_handlers.cx_broker_data
import db_message_handlers.accounting_currency_balance_data
import db_message_handlers.accounting_data
import db_message_handlers.sectors_data
import db_message_handlers.systems_data
import db_message_handlers.site_available_reserve_population_data

logger = logging.getLogger(__name__)

processed_message_ids_cache = TTLCache(maxsize=1000, ttl=60)

# Dictionary to map message types to the converter functions
CONVERTER_HANDLERS = {
    'USER_DATA': data_converter.convert_users_data_table,
    'WORLD_MATERIAL_CATEGORIES': data_converter.convert_world_materials_data,
    'PLANET_DATA': data_converter.convert_planets_data,
    'planets': data_converter.convert_planets_data,
    'STORAGE_STORAGES': data_converter.convert_storages_data,
    'STORAGE_CHANGE': data_converter.convert_storages_data,
    'COMPANY_DATA': data_converter.convert_company_data,
    'SITE_SITES': data_converter.convert_sites_data,
    'SITE_SITE': data_converter.convert_sites_data,
    'WAREHOUSE_STORAGES': data_converter.convert_warehouses_data,
    'SHIP_SHIPS': data_converter.convert_ships_data,
    'SHIP_DATA': data_converter.convert_ships_data,
    'COMEX_TRADER_ORDERS': data_converter.convert_comex_trade_orders_data,
    'COMEX_BROKER_DATA': data_converter.convert_comex_broker_data,
    'ACCOUNTING_CASH_BALANCES': data_converter.convert_user_currency_accounts_data,
    'ACCOUNTING_BOOKINGS': data_converter.convert_accounting_data,
    'WORLD_SECTORS': data_converter.convert_sectors_data,
    'SYSTEM_STARS_DATA': data_converter.convert_systems_data,
    'POPULATION_AVAILABLE_RESERVE_WORKFORCE': data_converter.convert_site_available_population_data
}

def converter_router(argument, data):
    """
    Routes an argument to the correct data converter function.
    """
    handler = CONVERTER_HANDLERS.get(argument)
    if handler:
        return handler(data)
    else:
        return []
    
MESSAGE_HANDLERS = {
    'USER_DATA': db_message_handlers.user_data.handle_user_data_message,
    'WORLD_MATERIAL_CATEGORIES': db_message_handlers.material_categories.handle_material_categories_message,
    'PLANET_DATA': db_message_handlers.planet_data.handle_planet_data_message,
    'planets': db_message_handlers.planet_data.handle_planet_data_message,
    'STORAGE_STORAGES': db_message_handlers.storage_data.handle_storage_data_message,
    'STORAGE_CHANGE': db_message_handlers.storage_data.handle_storage_data_message,
    'COMPANY_DATA': db_message_handlers.company_data.handle_company_data_message,
    'SITE_SITES': db_message_handlers.sites_data.handle_sites_data_message,
    'SITE_SITE': db_message_handlers.sites_data.handle_sites_data_message,
    'WAREHOUSE_STORAGES': db_message_handlers.warehouse_data.handle_warehouse_data_message,
    'SHIP_SHIPS': db_message_handlers.ship_data.handle_ship_data_message,
    'SHIP_DATA': db_message_handlers.ship_data.handle_ship_data_message,
    'COMEX_TRADER_ORDERS': db_message_handlers.cx_data_orders.handle_comex_orders_data_message,
    'COMEX_BROKER_DATA': db_message_handlers.cx_broker_data.handle_cx_broker_data_message,
    'ACCOUNTING_CASH_BALANCES': db_message_handlers.accounting_currency_balance_data.handle_accounting_currency_balance_data_message,
    'ACCOUNTING_BOOKINGS': db_message_handlers.accounting_data.handle_accounting_data_message,
    'WORLD_SECTORS': db_message_handlers.sectors_data.handle_sectors_message,
    'SYSTEM_STARS_DATA': db_message_handlers.systems_data.handle_systems_data,
    'POPULATION_AVAILABLE_RESERVE_WORKFORCE': db_message_handlers.site_available_reserve_population_data.handle_site_available_reserve_population_data_message
}

async def handle_message_data_router(db, messageType, payload) -> Any:
    """
    Routes an incoming message to the correct handler
    """
    handler = MESSAGE_HANDLERS.get(messageType)
    if handler:
        if inspect.iscoroutinefunction(handler):
            logger.info(f"Awaiting async handler for message type: {messageType}")
            response = await handler(db, payload)
        else:
            logger.info(f"Running synchronous handler in thread for message type: {messageType}")
            response = await asyncio.to_thread(handler, db, payload)
            
        if isinstance(response, tuple) and len(response) == 2:
            return response
        else:
            return response, 200
    else:
        return {"error": f"Unknown message type: {messageType}"}, 200

async def process_data_batch_task(items_to_process: List[Dict[str, Any]], user_id: str, db):
    """
    Synchronous task wrapper that runs the async logic.
    """
    task_start_time = time.perf_counter()
    

    for item in items_to_process:
        if "message" not in item or item["message"] is None:
            logger.warning(f"Item {item.get('id', 'N/A')}: 'message' key is missing or None. Skipping.")
            continue

        message_payload = item.get("message")

        if not isinstance(message_payload, dict):
            try:
                message_payload = json.loads(message_payload)
            except (json.JSONDecodeError, TypeError):
                logger.error(f"Invalid message payload format for item {item.get('id', 'N/A')}: {message_payload}. Skipping.", exc_info=True)
                continue

        message_id = item.get("id")
        if message_id in processed_message_ids_cache:
            logger.info(f"Message ID '{message_id}' already processed in this batch, skipping.")
            continue

        processed_message_ids_cache[message_id] = True

        try:
            message_type = message_payload.get("messageType")
            if message_type == "DATA_DATA":
                if message_payload.get("payload", {}).get("path", []) and \
                    message_payload["payload"]["path"][0] == "planets" and \
                    len(message_payload["payload"]["path"]) < 3:
                    message_type = message_payload["payload"]["path"][0]
                    message_payload["payload"] = message_payload["payload"].get("body")
                else:
                    logger.warning(f"Message {message_id}: messageType is None and not a recognized special case. Skipping.")
                    continue

            db_start_time = time.perf_counter()
            response = await handle_message_data_router(
                db,
                messageType=message_type,
                payload={"userId": user_id, "data": converter_router(message_type, message_payload)}
            )

            log_message = response[0].get('message', 'No message') if isinstance(response, tuple) and response and isinstance(response[0], dict) else 'No response info'
            logger.info(f"Processed message ID '{message_id}' for type '{message_type}' with response: {log_message}")
            db_end_time = time.perf_counter()
            logger.info(f"Processing request took {db_end_time - db_start_time:.4f} seconds.")
        except Exception as e:
            logger.error(f"Error processing message ID '{message_id}': {e}", exc_info=True)

    task_end_time = time.perf_counter()
    logger.info(f"Finished for user_id: '{user_id}' in {task_end_time - task_start_time:.2f} seconds.")
    return {"status": "completed", "total_processed": len(items_to_process)}