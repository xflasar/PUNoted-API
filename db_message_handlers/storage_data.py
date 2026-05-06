import logging
import time
from typing import Any, Dict, List, Optional, Set

import asyncpg

from db import Database
from helpers.shipments import fetch_active_shipments_structured
from managers.global_ws_manager import global_ws_manager as ws_manager
from repositories.storage import fetch_user_storages_by_id

logger = logging.getLogger(__name__)


# TODO: This handler is currently working for storage updates but if we receive partial update it will break the data integrity and delete all storages and insert only the one we have received This happens on Ship construction finished event.


async def handle_storage_removed_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.debug("Starting processing storage removed data.")

    storage_records = raw_payload.get("data", {})

    # 1. Quick Exit if empty
    if not storage_records:
        return {"success": True, "message": "No storage records to process."}

    try:
        # 2. Get User ID (Optimized lookup)
        user_response = await db.fetch_one(
            "SELECT accountid, userdataid FROM users WHERE accountid = $1;",
            raw_payload.get("userId"),
        )

        if not user_response:
            return {"success": False, "message": "User not found."}

        userid = str(user_response.get("userdataid") or user_response.get("accountid"))

        # 3. Extract IDs
        storage_ids = [rec["storageid"] for rec in storage_records]

        async with db.pool.acquire() as con:
            async with con.transaction():
                # STEP A: Delete Items First (Safest for Foreign Keys)
                # Removes items inside the target storages
                await con.execute(
                    """
                    DELETE FROM storage_items 
                    WHERE storageid = ANY($1::text[]);
                    """,
                    storage_ids,
                )

                # STEP B: Delete Warehouses
                # We do this BEFORE deleting storages so we can still look up the link.
                await con.execute(
                    """
                    DELETE FROM warehouses 
                    WHERE userid = $2
                      AND warehouseid IN (
                          SELECT addressableid 
                          FROM storages 
                          WHERE storageid = ANY($1::text[])
                      );
                    """,
                    storage_ids,
                    userid,
                )

                # STEP C: Delete Storages
                # Now it is safe to remove the storages themselves
                await con.execute(
                    """
                    DELETE FROM storages 
                    WHERE storageid = ANY($1::text[]) 
                      AND userid = $2;
                    """,
                    storage_ids,
                    userid,
                )

        end_time = time.perf_counter()
        logger.debug(f"Finished removal in {end_time - start_time:.2f} seconds.")

        return {
            "success": True,
            "message": f"Removed {len(storage_ids)} storages, related warehouses, and items.",
        }

    except Exception as e:
        logger.error(f"Error processing removal: {e}", exc_info=True)
        raise


async def handle_storage_data_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.debug("Starting processing storage data.")

    converted_data = raw_payload.get("data")
    full_refresh = converted_data.get("full_refresh", False)
    storage_records = converted_data.get("storages", [])

    # Initialize this early to prevent UnboundLocalError
    affected_account_ids = set()

    if not storage_records:
        logger.debug("No storage records to process.")
        return {"success": True, "message": "No storage records to process."}

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

        # ✔ GET ALL STORAGE IDs
        storage_ids = [rec["storageid"] for rec in storage_records]

        async with db.pool.acquire() as con:
            async with con.transaction():
                if full_refresh:
                    logger.debug(f"Performing full refresh for user {userid}")
                    # Full refresh: Wipe all existing storages for this user
                    if storage_ids:
                        # Standard sync: Keep these, delete others
                        delQuery = "DELETE FROM storages WHERE userid = $1 AND storageid != ALL($2::text[]);"
                        await con.execute(delQuery, userid, storage_ids)
                    else:
                        # Explicit wipe: The list is intentionally empty
                        logger.debug(f"Wiping all storages for user {userid}")
                        await con.execute("DELETE FROM storages WHERE userid = $1", userid)

                # Step 1: sync items
                all_incoming_items = [item for record in storage_records for item in record.get("storage_items", [])]

                await sync_all_storage_items(con, all_incoming_items, storage_ids)

                # Step 2: UPSERT storages
                await upsert_storage_records(con, "storages", storage_records, userid)

                storage_data = await fetch_user_storages_by_id(con, userid, storage_ids)

                await ws_manager.send_personal_message(
                    raw_payload.get("userId"),
                    {"type": "STORAGE_DATA_UPDATE", "data": storage_data},
                )

                # --- STEP 3: SMART SHIPMENT TRIGGER ---
                # Check if any "SHIPMENT" type items were involved in this update
                shipment_item_ids = [
                    item["materialid"] for item in all_incoming_items if item.get("type") == "SHIPMENT"
                ]

                # Only proceed if actual shipment crates were touched
                if shipment_item_ids:
                    logger.debug(f"Shipment items detected in storage update: {len(shipment_item_ids)}")

                    # Resolve which users (Client/Carrier) need a dashboard refresh
                    affected_account_ids = await _find_users_by_shipment_items(con, shipment_item_ids)

        # --- STEP 4: NOTIFICATIONS (Outside Transaction) ---
        if affected_account_ids:
            logger.debug(f"Notifying {len(affected_account_ids)} users about shipment data updates.")
            for account_id in affected_account_ids:
                try:
                    data = await fetch_active_shipments_structured(db, account_id)
                    await ws_manager.send_personal_message(account_id, {"type": "SHIPMENT_DATA_UPDATE", "data": data})
                except Exception as e:
                    logger.warning(f"Failed to push shipment update to {account_id}: {e}")

        end_time = time.perf_counter()
        logger.debug(f"Finished processing storage data in {end_time - start_time:.2f} seconds.")

        return {
            "success": True,
            "message": f"Processed {len(storage_records)} storages. Items synced.",
            "sync_results": {"sync_completed": True},
        }

    except Exception as e:
        logger.error(f"Error processing storage data: {e}", exc_info=True)
        raise


# --- HELPER FUNCTIONS ---


async def _find_users_by_shipment_items(con: asyncpg.Connection, item_ids: List[str]) -> Set[str]:
    """
    Finds all Account IDs (Carrier and Client) associated with the given shipment item IDs.
    """
    if not item_ids:
        return set()

    query = """
    SELECT 
        c.userid as client_user_id,
        cd.userdataid as carrier_user_id
    FROM contract_conditions cc
    JOIN contracts c ON cc.contractid = c.id
    LEFT JOIN company_data cd ON c.partnerid = cd.companyid
    WHERE cc.shipmentitemid = ANY($1::text[])
      AND cc.status = 'PENDING' -- Only care about active shipments
    """

    affected_ids = set()
    rows = await con.fetch(query, item_ids)

    for r in rows:
        # Resolve Client Account ID
        if r["client_user_id"]:
            client_acct = await _get_account_id(con, r["client_user_id"])
            if client_acct:
                affected_ids.add(client_acct)

        # Resolve Carrier Account ID
        if r["carrier_user_id"]:
            carrier_acct = await _get_account_id(con, r["carrier_user_id"])
            if carrier_acct:
                affected_ids.add(carrier_acct)

    return affected_ids


async def _get_account_id(con: asyncpg.Connection, userdataid: str) -> Optional[str]:
    val = await con.fetchval("SELECT accountid FROM users WHERE userdataid = $1", userdataid)
    return str(val) if val else None


async def upsert_storage_records(con: asyncpg.Connection, table_name: str, records: List[Dict[str, Any]], userid: str):
    """
    Performs a bulk UPSERT (INSERT or UPDATE) on the main 'storages' table.
    """
    if not records:
        return

    upsert_records = []
    for record in records:
        temp_record = record.copy()
        temp_record.pop("storage_items", None)
        temp_record["userid"] = userid
        upsert_records.append(temp_record)

    keys = list(upsert_records[0].keys())
    keys_str = ", ".join(keys)
    values_placeholders = ", ".join([f"${i + 1}" for i in range(len(keys))])
    set_clause = ", ".join([f"{key} = EXCLUDED.{key}" for key in keys])

    query = f"""
    INSERT INTO {table_name} ({keys_str})
    VALUES ({values_placeholders})
    ON CONFLICT (storageid) DO UPDATE SET
        {set_clause};
    """

    records_as_tuples = [tuple(rec.get(key) for key in keys) for rec in upsert_records]

    try:
        await con.executemany(query, records_as_tuples)
        logger.debug(f"UPSERT for {len(records_as_tuples)} storages records completed successfully.")
    except Exception as e:
        logger.error(f"Database error during storages UPSERT: {e}", exc_info=True)
        raise


async def sync_all_storage_items(
    con: asyncpg.Connection,
    incoming_items: List[Dict[str, Any]],
    storage_ids: List[str],
) -> Dict[str, Any]:
    """
    Syncs storage_items by upserting incoming items and deleting missing ones.
    """
    TABLE_NAME = "storage_items"
    incoming_items_map = {}

    for item in incoming_items:
        sid = item.get("storageid")
        mid = item.get("materialid")
        if sid and mid:
            composite_key = f"{sid}-{mid}"
            item["compositekey"] = composite_key
            incoming_items_map[composite_key] = item

    incoming_composite_keys = list(incoming_items_map.keys())

    # FETCH EXISTING
    existing_items_data = await con.fetch(
        "SELECT compositekey FROM storage_items WHERE storageid = ANY($1::text[]);",
        storage_ids,
    )
    existing_keys_map = [r["compositekey"] for r in existing_items_data]

    # DELETE
    keys_to_delete = [key for key in existing_keys_map if key not in incoming_composite_keys]

    # UPSERT
    upsert_records = list(incoming_items_map.items())

    if upsert_records:
        await upsert_storage_items(con, TABLE_NAME, upsert_records)

    if keys_to_delete:
        await bulk_delete_storage_items(con, TABLE_NAME, keys_to_delete)

    return {"success": True, "message": "Storage items synced successfully."}


async def upsert_storage_items(con: asyncpg.Connection, table_name: str, records: List[Dict[str, Any]]):
    if not records:
        return

    keys = list(records[0][1].keys())
    keys_str = ", ".join(keys)
    values_placeholders = ", ".join([f"${i + 1}" for i in range(len(keys))])
    set_clause = ", ".join([f"{key} = EXCLUDED.{key}" for key in keys if key != "compositekey"])

    query = f"""
    INSERT INTO {table_name} ({keys_str})
    VALUES ({values_placeholders})
    ON CONFLICT (compositekey) DO UPDATE SET
        {set_clause};
    """

    records_as_tuples = [tuple(rec[1].get(key) for key in keys) for rec in records]

    try:
        await con.executemany(query, records_as_tuples)
        logger.debug(f"UPSERT for {len(records_as_tuples)} storage items completed successfully.")
    except Exception as e:
        logger.error(f"Database error during storage items UPSERT: {e}", exc_info=True)
        raise


async def bulk_delete_storage_items(con: asyncpg.Connection, table_name: str, composite_keys: List[str]):
    if not composite_keys:
        return

    query = f"DELETE FROM {table_name} WHERE compositekey = ANY($1::text[]);"

    try:
        await con.execute(query, list(composite_keys))
        logger.debug(f"Bulk DELETE for {len(composite_keys)} storage items completed successfully.")
    except Exception as e:
        logger.error(f"Database error during bulk DELETE: {e}", exc_info=True)
        raise
