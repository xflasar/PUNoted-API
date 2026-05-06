import logging
import time
from typing import Any, Dict, List

import asyncpg

from db import Database

# FIX: Import the new global manager
from helpers.shipments import lookup_shipment_channels
from managers.global_ws_manager import global_ws_manager

logger = logging.getLogger(__name__)


async def handle_ship_flights_data_message(db: Database, converted_data: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.debug("Starting processing ship flight data.")

    records_to_process = (
        [converted_data.get("data")]
        if isinstance(converted_data.get("data"), dict)
        else list(converted_data.get("data", []))
    )
    user_account_id = converted_data.get("userId")

    if not records_to_process or not user_account_id:
        return {"success": False, "message": "Invalid payload."}

    try:
        user_response = await db.fetch_one(
            "SELECT accountid, userdataid FROM users WHERE accountid = $1;",
            user_account_id,
        )
        userid = user_response.get("userdataid") or user_response.get("accountid") if user_response else None
        if not userid:
            return {"success": False, "message": "User not found."}

        segments = []
        for record in records_to_process:
            segments += record.get("segments", [])
            if "segments" in record:
                del record["segments"]
            record["userid"] = userid

        updated_flight_ids = [record["id"] for record in records_to_process]

        async with db.pool.acquire() as con:
            async with con.transaction():
                await _upsert_records(con, "ship_flights", records_to_process, ["id", "userid", "shipid"])
                await _upsert_records(
                    con,
                    "ship_flight_segments",
                    segments,
                    ["flight_id", "segment_index"],
                )

    except Exception as e:
        logger.error(f"Error processing ship flights data: {e}", exc_info=True)
        raise

    # --- REAL-TIME CLIENT BROADCAST ---
    try:
        if updated_flight_ids:
            # Keep connection open for the duration of the batch processing
            async with db.pool.acquire() as conn:
                updated_flights_payload = await fetch_updated_flight_plans_batch(conn, updated_flight_ids)

                if updated_flights_payload:
                    for flight_data in updated_flights_payload:
                        flight_id = flight_data.get("id")
                        if not flight_id:
                            continue

                        # 1. Identify Context (Shipment vs Regular) using the SQL function
                        channels_info = await lookup_shipment_channels(conn, flight_id)

                        # Extract Data
                        owner_data = channels_info.get("current_user", {})
                        owner_acc = owner_data.get("account_id")
                        owner_corps = owner_data.get("corp_ids", [])

                        contract_id = channels_info.get("contract_id")
                        partners = channels_info.get("partners", [])  # List of dicts {account_id, ...}

                        # 2. Define Targets
                        target_channels = set()

                        # LOGIC BRANCH:
                        # If we have a Contract AND Partners -> Send to User + Partners
                        if contract_id and len(partners) > 0:
                            if owner_acc:
                                target_channels.add(f"map:user:{owner_acc}")

                            for p in partners:
                                if p.get("account_id"):
                                    target_channels.add(f"map:user:{p['account_id']}")

                        # Otherwise -> Send to User + His Corp
                        else:
                            if owner_acc:
                                target_channels.add(f"map:user:{owner_acc}")

                            for corp_id in owner_corps:
                                target_channels.add(f"map:corp:{corp_id}")

                        if not target_channels:
                            continue

                        # 3. Broadcast
                        update_message = {
                            "type": "FLIGHT_PLAN_UPDATE",
                            "data": flight_data,
                        }

                        for channel_id in target_channels:
                            await global_ws_manager.broadcast(channel_id, update_message)

    except Exception as e:
        logger.error(f"Failed to broadcast flight plan updates: {e}", exc_info=True)

    end_time = time.perf_counter()
    logger.debug(f"Processing ship flight records took {end_time - start_time:.4f} seconds")
    return {"success": True, "message": "Processed ship flight records successfully."}


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


async def fetch_updated_flight_plans_batch(conn: asyncpg.Connection, flight_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Fetches flight data and all associated segments for a list of flight IDs
    and returns them in the final nested structure.
    """
    # 1. Fetch all flight headers in one query
    flights_query = """
    SELECT 
        id, originplanetid, originstationid, originsystemid, shipid, 
        stldistance, ftldistance, damage, currentsegmentindex, 
        destinationplanetid, destinationsystemid, destinationstationid, 
        arrivaltimestamp, departuretimestamp, 
        stltotalconsumption, ftltotalconsumption, userid
    FROM ship_flights
    WHERE id = ANY($1::text[]);
    """
    flight_records = await conn.fetch(flights_query, flight_ids)

    # 2. Fetch all segments for all the requested flights in one query
    segments_query = """
    SELECT
        flight_id, segment_type, segment_index, "departure", "arrival", 
        duration, origin_system_id, origin_location_id, origin_location_type, 
        origin_orbit_data, destination_system_id, destination_location_id, 
        destination_location_type, destination_orbit_data, stl_distance, 
        ftl_distance, stl_fuel, ftl_fuel, damage, transferellipse
    FROM ship_flight_segments
    WHERE flight_id = ANY($1::text[])
    ORDER BY flight_id, segment_index ASC;
    """
    segment_records = await conn.fetch(segments_query, flight_ids)

    # 3. Assemble the segments into a hash map for fast nesting
    segments_by_flight = {}
    for record in segment_records:
        segment_data = dict(record)
        flight_id = segment_data["flight_id"]
        if flight_id not in segments_by_flight:
            segments_by_flight[flight_id] = []
        segments_by_flight[flight_id].append(segment_data)

    # 4. Nest the segments into the flight headers
    updated_flights = []
    for record in flight_records:
        flight_data = dict(record)
        flight_id = flight_data["id"]
        flight_data["segments"] = segments_by_flight.get(flight_id, [])
        updated_flights.append(flight_data)

    return updated_flights
