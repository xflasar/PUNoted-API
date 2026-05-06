import json
import logging
import time
from typing import Any, Dict

from db import Database

logger = logging.getLogger(__name__)


async def handle_gateway_data_message(db: Database, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()

    try:
        data = raw_payload.get("data")
    except Exception as e:
        logger.error(f"Failed to convert gateway data: {e}", exc_info=True)
        return {"success": False, "message": "Conversion failed"}

    gateways = data["gateways"]
    if not gateways:
        return {"success": True, "message": "No gateway data."}

    gateway_ids = [g["id"] for g in gateways]

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                # 1. Main Gateways
                await _upsert_gateways(con, gateways)

                # 2. Fuel Contractors (Delete old for these gateways, insert new)
                await con.execute(
                    "DELETE FROM gateway_fuel_contractors WHERE gateway_id = ANY($1::text[])",
                    gateway_ids,
                )
                if data["fuel_contractors"]:
                    await _insert_fuel_contractors(con, data["fuel_contractors"])

                # 3. Traffic (Upsert 1:1)
                await _upsert_traffic(con, data["traffic"])

                # 4. Upkeep Main (Upsert 1:1)
                await _upsert_upkeep(con, data["upkeep"])

                # 5. Upkeep Requirements (Delete old, insert new)
                await con.execute(
                    "DELETE FROM gateway_upkeep_requirements WHERE gateway_id = ANY($1::text[])",
                    gateway_ids,
                )
                if data["upkeep_requirements"]:
                    await _insert_upkeep_reqs(con, data["upkeep_requirements"])

                # 6. Upkeep Phases (Upsert by Phase ID)
                if data["upkeep_phases"]:
                    await _upsert_upkeep_phases(con, data["upkeep_phases"])

                # 7. Upkeep Contractors (Delete old, insert new)
                await con.execute(
                    "DELETE FROM gateway_upkeep_contractors WHERE gateway_id = ANY($1::text[])",
                    gateway_ids,
                )
                if data["upkeep_contractors"]:
                    await _insert_upkeep_contractors(con, data["upkeep_contractors"])

        logger.debug(f"Processed {len(gateways)} gateways in {time.perf_counter() - start_time:.4f}s")
        return {"success": True, "message": "Gateways fully updated."}

    except Exception as e:
        logger.error(f"DB Error in gateway handler: {e}", exc_info=True)
        raise


# --- SQL Helpers ---


async def _upsert_gateways(con, records):
    query = """
    INSERT INTO gateways (
        id, natural_id, name, type, system_id, planet_id, satellite_id, owner_admin_center_id, currency_code, established,
        operational_state, link_status, outgoing_link_id, incoming_links, is_linked,
        max_ship_volume, linking_radius, jumps_per_day,
        capacity_upgrades, volume_upgrades, distance_upgrades,
        fuel_available, fuel_max, fuel_per_jump, fuel_usage_fee, fuel_usage_currency, avg_fuel_availability,
        updated_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, NOW())
    ON CONFLICT (id) DO UPDATE SET
        natural_id=EXCLUDED.natural_id, name=EXCLUDED.name, type=EXCLUDED.type, 
        satellite_id=EXCLUDED.satellite_id, established=EXCLUDED.established,
        operational_state=EXCLUDED.operational_state, link_status=EXCLUDED.link_status,
        outgoing_link_id=EXCLUDED.outgoing_link_id, incoming_links=EXCLUDED.incoming_links, is_linked=EXCLUDED.is_linked,
        fuel_available=EXCLUDED.fuel_available, fuel_max=EXCLUDED.fuel_max,
        updated_at=NOW();
    """

    vals = [
        (
            r["id"], r["natural_id"], r["name"], r["type"], r["system_id"], 
            r["planet_id"], r["satellite_id"], r["owner_admin_center_id"], 
            r["currency_code"], r["established"], r["operational_state"], 
            r["link_status"], r["outgoing_link_id"], r["incoming_links"], 
            r["is_linked"], r["max_ship_volume"], r["linking_radius"], 
            r["jumps_per_day"], r["capacity_upgrades"], r["volume_upgrades"], 
            r["distance_upgrades"], r["fuel_available"], r["fuel_max"], 
            r["fuel_per_jump"], r["fuel_usage_fee"], r["fuel_usage_currency"], 
            r["avg_fuel_availability"]
        )
        for r in records
    ]
    await con.executemany(query, vals)


async def _insert_fuel_contractors(con, records):
    query = """
    INSERT INTO gateway_fuel_contractors (gateway_id, phase_index, contractor_id, contractor_code, contractor_name, contract_id)
    VALUES ($1, $2, $3, $4, $5, $6)
    """
    vals = [
        (
            r["gateway_id"],
            r["phase_index"],
            r["contractor_id"],
            r["contractor_code"],
            r["contractor_name"],
            r["contract_id"],
        )
        for r in records
    ]
    await con.executemany(query, vals)


async def _upsert_traffic(con, records):
    query = """
    INSERT INTO gateway_traffic (
        gateway_id, total_jumps, current_phase_jumps, current_phase_inbound, 
        current_phase_start, current_phase_end, avg_jumps, avg_inbound,
        raw_current_phase, raw_last_phase, raw_averages
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb, $11::jsonb)
    ON CONFLICT (gateway_id) DO UPDATE SET
        total_jumps=EXCLUDED.total_jumps, current_phase_jumps=EXCLUDED.current_phase_jumps,
        current_phase_inbound=EXCLUDED.current_phase_inbound, raw_current_phase=EXCLUDED.raw_current_phase;
    """
    vals = [
        (
            r["gateway_id"], r["total_jumps"], r["current_phase_jumps"], 
            r["current_phase_inbound"], r["current_phase_start"], 
            r["current_phase_end"], r["avg_jumps"], r["avg_inbound"],
            json.dumps(r["raw_current_phase"]),
            json.dumps(r["raw_last_phase"]),
            json.dumps(r["raw_averages"])
        )
        for r in records
    ]
    await con.executemany(query, vals)


async def _upsert_upkeep(con, records):
    query = """
    INSERT INTO gateway_upkeep (gateway_id, average_uptime, updated_at) VALUES ($1, $2, NOW())
    ON CONFLICT (gateway_id) DO UPDATE SET average_uptime=EXCLUDED.average_uptime, updated_at=NOW();
    """
    vals = [(r["gateway_id"], r["average_uptime"]) for r in records]
    await con.executemany(query, vals)


async def _insert_upkeep_reqs(con, records):
    query = """
    INSERT INTO gateway_upkeep_requirements (gateway_id, material_id, material_ticker, material_name, amount_current, amount_required)
    VALUES ($1, $2, $3, $4, $5, $6)
    """
    vals = [
        (
            r["gateway_id"],
            r["material_id"],
            r["material_ticker"],
            r["material_name"],
            r["amount_current"],
            r["amount_required"],
        )
        for r in records
    ]
    await con.executemany(query, vals)


async def _upsert_upkeep_phases(con, records):
    query = """
    INSERT INTO gateway_upkeep_phases (id, gateway_id, natural_id, start_time, end_time, service_level, materials_json)
    VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
    ON CONFLICT (id) DO UPDATE SET service_level=EXCLUDED.service_level, materials_json=EXCLUDED.materials_json;
    """
    vals = [
        (
            r["id"], r["gateway_id"], r["natural_id"], r["start_time"], 
            r["end_time"], r["service_level"], json.dumps(r["materials_json"])
        )
        for r in records
    ]
    await con.executemany(query, vals)


async def _insert_upkeep_contractors(con, records):
    query = """
    INSERT INTO gateway_upkeep_contractors (gateway_id, phase_index, contractor_id, contractor_code, contractor_name, contract_id)
    VALUES ($1, $2, $3, $4, $5, $6)
    """
    vals = [
        (
            r["gateway_id"],
            r["phase_index"],
            r["contractor_id"],
            r["contractor_code"],
            r["contractor_name"],
            r["contract_id"],
        )
        for r in records
    ]
    await con.executemany(query, vals)
