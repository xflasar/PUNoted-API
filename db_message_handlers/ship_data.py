import json
import logging
import time
from typing import Any, Dict, List

import asyncpg

from app.db.models.ships import Ship
from db import Database
from helpers.shipments import fetch_active_shipments_structured
from managers.global_ws_manager import global_ws_manager

logger = logging.getLogger(__name__)

def serialize_ws_payload(data: Any) -> str:
    from datetime import date, datetime
    from decimal import Decimal

    def default_serializer(obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return str(obj)

    return json.dumps(data, default=default_serializer)


async def fetch_updated_ships_batch(conn: asyncpg.Connection, updated_ships_ids: List[str]) -> List[dict]:
    sql_query = """
        SELECT 
            s.*, 
            ud.displayname AS displayname, 
            cd.companycode AS companycode,
            
            CASE 
                WHEN s.flightid IS NOT NULL THEN (
                    SELECT row_to_json(f)::jsonb || jsonb_build_object(
                        'segments', (
                            SELECT COALESCE(json_agg(row_to_json(seg) ORDER BY seg.segment_index ASC), '[]'::json)
                            FROM ship_flight_segments seg
                            WHERE seg.flightid = f.id
                        )
                    )
                    FROM flights f 
                    WHERE f.id = s.flightid
                )
                ELSE NULL 
            END AS plan,
            CASE
                WHEN st.volumecapacity >= 5000 AND st.weightcapacity >= 5000 THEN 'HCB'
                WHEN st.volumecapacity = 3000 AND st.weightcapacity = 1000 THEN 'VCB'
                WHEN st.volumecapacity = 1000 AND st.weightcapacity = 3000 THEN 'WCB'
                WHEN st.volumecapacity = 2000 AND st.weightcapacity = 2000 THEN 'LCB'
                WHEN st.volumecapacity = 500  AND st.weightcapacity = 500  THEN 'TINY'
                ELSE 'UNKNOWN'
            END AS ship_type

        FROM ships s
        JOIN storages st ON st.storageid = s.idshipstore
        JOIN users_data ud ON ud.userid = s.userid
        JOIN company_data cd ON cd.companyid = ud.companyid
        WHERE s.shipid = ANY($1);
    """
    ships_records = await conn.fetch(sql_query, updated_ships_ids)
    
    parsed_records = []
    for r in ships_records:
        r_dict = dict(r)
        
        if isinstance(r_dict.get('plan'), str):
            r_dict['plan'] = json.loads(r_dict['plan'])
            
        r_dict['is_owner'] = False
        parsed_records.append(r_dict)

    ships = [Ship(**rec) for rec in parsed_records]


async def handle_ship_data_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.debug("Starting processing ship data.")

    converted_data = raw_payload.get("data")
    if not converted_data:
        return {"success": True, "message": "No ship records to process."}

    try:
        # User lookup logic remains the same
        user_response = await db.fetch_one(
            "SELECT accountid, userdataid FROM users WHERE accountid = $1;",
            raw_payload["userId"],
        )
        if user_response and user_response.get("userdataid") is not None:
            userid = user_response.get("userdataid")
        elif user_response:
            userid = user_response.get("accountid")
        else:
            return {"success": False, "message": "User not found."}

        ships_to_upsert = []
        ship_repair_materials_to_upsert = []
        updated_ships_ids = set()

        for record in converted_data:
            ship_id = record.get("shipid")
            if not ship_id:
                continue
            updated_ships_ids.add(ship_id)
            record["userid"] = userid

            if "repair_materials" in record:
                repair_materials = record.pop("repair_materials")
                for material in repair_materials:
                    material["shipid"] = ship_id
                    ship_repair_materials_to_upsert.append(material)

            ships_to_upsert.append(record)

        # Database Upserts
        async with db.pool.acquire() as con:
            async with con.transaction():
                await _upsert_records(con, "ships", ships_to_upsert, ["shipid"])
                if ship_repair_materials_to_upsert:
                    await _upsert_records(
                        con,
                        "ship_repair_materials",
                        ship_repair_materials_to_upsert,
                        ["shipid", "materialid"],
                    )

                for ship in ships_to_upsert:
                    materials_for_this_ship = [
                        m for m in ship_repair_materials_to_upsert if m["shipid"] == ship["shipid"]
                    ]
                    await _handle_deletions_by_parent(
                        con,
                        "ship_repair_materials",
                        "shipid",
                        ship["shipid"],
                        ["shipid", "materialid"],
                        materials_for_this_ship,
                    )

    except Exception as e:
        logger.error(f"Error processing ship data: {e}", exc_info=True)
        raise

    try:
        if updated_ships_ids:
            await _process_notifications(db, global_ws_manager, raw_payload["userId"], updated_ships_ids)
            async with db.pool.acquire() as conn:
                updated_ships_payload = await fetch_updated_ships_batch(conn, updated_ships_ids)

                # Fetch Corp IDs
                corp_records = await conn.fetch(
                    "SELECT corporationid FROM corporation_shareholders WHERE userid = $1",
                    raw_payload["userId"],
                )
                user_corp_ids = [str(r["corporationid"]) for r in corp_records]

            if updated_ships_payload:
                # 1. Create the personal version (is_owner = True)
                personal_payload_data = [
                    {**ship, "is_owner": True} for ship in updated_ships_payload
                ]
                personal_message = {
                    "type": "SHIP_DATA_UPDATE",
                    "data": personal_payload_data,
                }

                # 2. Create the corporate version (is_owner = False)
                corp_payload_data = [
                    {**ship, "is_owner": False} for ship in updated_ships_payload
                ]
                corp_message = {
                    "type": "SHIP_DATA_UPDATE",
                    "data": corp_payload_data,
                }

                # 3. Broadcast to Personal Channel
                user_channel = f"map:user:{raw_payload['userId']}"
                await global_ws_manager.broadcast(user_channel, personal_message)

                # 4. Broadcast to Corp Channels
                for corp_id in user_corp_ids:
                    await global_ws_manager.broadcast(f"map:corp:{corp_id}", corp_message)

    except Exception as e:
        logger.error(f"Error broadcasting ship updates: {e}")

    return {
        "success": True,
        "message": f"Processed {len(converted_data)} ship records successfully.",
    }


async def _upsert_records(
    con: asyncpg.Connection,
    table_name: str,
    records: List[Dict[str, Any]],
    unique_fields: List[str],
    chunk_size: int = 5000,
):
    """
    A generic helper function to perform bulk upserts using INSERT ... ON CONFLICT DO UPDATE.
    It processes records in chunks to prevent large transaction overhead.
    """
    if not records:
        return

    columns = records[0].keys()
    columns_str = ", ".join(columns)
    unique_columns_str = ", ".join(unique_fields)

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
            logger.error(f"Database error during UPSERT: {e}", exc_info=True)
            raise
    logger.debug("Finished inserting data in upsert_records.")


async def _handle_deletions_by_parent(
    con: asyncpg.Connection,
    table_name: str,
    parent_id_field: str,
    parent_id: str,
    unique_fields: List[str],
    current_records: List[Dict[str, Any]],
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

    payload_keys = {tuple(rec[field] for field in unique_fields) for rec in current_records}

    keys_query = f"SELECT {', '.join(unique_fields)} FROM {table_name} WHERE {parent_id_field} = $1;"
    existing_records = await con.fetch(keys_query, parent_id)
    existing_keys_in_db = {tuple(rec.values()) for rec in existing_records}

    keys_to_delete = existing_keys_in_db - payload_keys

    if not keys_to_delete:
        return

    # Construct the DELETE query dynamically
    if len(unique_fields) == 1:
        delete_query = f"""
            DELETE FROM {table_name}
            WHERE {unique_fields[0]} IN (SELECT UNNEST($1::text[]));
        """
        values_to_delete = [key[0] for key in keys_to_delete]
        delete_values = [values_to_delete]  # Wrap in a list
    else:
        # Construct multiple UNNEST clauses for multiple unique fields
        unnest_clauses = ", ".join(f"UNNEST(${i + 1}::text[])" for i in range(len(unique_fields)))
        delete_query = f"""
            DELETE FROM {table_name}
            WHERE ({", ".join(unique_fields)}) IN (SELECT {unnest_clauses});
        """

        # Prepare values for the query. Transpose the list of tuples into a list of lists.
        # Example: [(1, 'a'), (2, 'b')] becomes [[1, 2], ['a', 'b']]
        values_to_delete = list(zip(*keys_to_delete))
        delete_values = list(values_to_delete)

    try:
        await con.execute(delete_query, *delete_values)
    except Exception as e:
        logger.error(f"Database error during UPSERT: {e}", exc_info=True)
        raise


async def _process_notifications(db, ws_manager, current_account_id: str, shipids: set):
    """
    Unified notification handler:
    1. Resolves all recipients (User + Partners).
    2. ALWAYS sends a 'CONTRACTS_DATA_UPDATE_SIGNAL' (for the general list).
    3. CONDITIONALLY sends 'SHIPMENT_DATA_UPDATE' (heavy payload) if shipment items exist.
    """
    try:
        # 1. Resolve Recipients (Current User + Any Partners found in contracts)
        recipients = {current_account_id}

        q = """
        SELECT u.accountid FROM ships s
        INNER JOIN storages st ON st.storageid = s.idshipstore
        INNER JOIN storage_items sti ON sti.storageid = st.storageid
        INNER JOIN users u ON u.userdataid = s.userid
        INNER JOIN contract_conditions cc ON cc.shipmentitemid = sti.materialid
        INNER JOIN contracts c ON c.id = cc.contractid
        WHERE sti.type = 'SHIPMENT' AND u.accountid = $2 AND s.shipid = ANY($1::text[]) 
        GROUP BY u.accountid
        """
        async with db.pool.acquire() as conn:
            rows = await conn.fetch(q, list(shipids), current_account_id)
            for r in rows:
                if r["accountid"] != current_account_id:
                    recipients.add(str(r["accountid"]))

        # 3. Broadcast to all recipients
        for account_id in recipients:
            try:
                # Fetch fresh structure specifically for this user context
                new_shipment_state = await fetch_active_shipments_structured(db, account_id)

                await ws_manager.send_personal_message(
                    account_id,
                    {"type": "SHIPMENT_DATA_UPDATE", "data": new_shipment_state},
                )
                logger.debug(f"Pushed fresh shipment data to {account_id}")
            except Exception as e:
                logger.error(f"Failed to push shipment data to {account_id}: {e}")

    except Exception as e:
        logger.error(f"Notification processing failed: {e}")
