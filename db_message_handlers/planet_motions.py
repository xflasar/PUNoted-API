# db_message_handlers/planet_motions.py
import logging
import time
from typing import Any, Dict, List

from helpers.db import _upsert_records

logger = logging.getLogger(__name__)

# Primary Keys / Unique Keys for Planet Motions, Votes & Components
MOTIONS_UNIQUE_KEYS = ["motionid"]
MOTION_VOTES_UNIQUE_KEYS = ["id"]
MOTION_COMPONENTS_UNIQUE_KEYS = ["componentid"]


async def handle_planet_motion_message(db, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles single converted planet motion data (motion, votes, components),
    performs bulk UPSERTs into PostgreSQL.
    """
    start_time = time.perf_counter()
    logger.debug("Starting processing single planet motion data.")

    converted_data = data.get("data", {})
    planet_motions = converted_data.get("planet_motions", [])
    planet_motion_votes = converted_data.get("planet_motion_votes", [])
    planet_motion_components = converted_data.get("planet_motion_components", [])

    total_records = len(planet_motions) + len(planet_motion_votes) + len(planet_motion_components)

    if total_records == 0:
        return {"success": True, "message": "No single planet motion data to process."}

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                if planet_motions:
                    await _upsert_records(con, "planet_motions", planet_motions, MOTIONS_UNIQUE_KEYS)

                if planet_motion_votes:
                    motion_ids = list({v["motionid"] for v in planet_motion_votes if "motionid" in v})
                    if motion_ids:
                        await con.execute(
                            "DELETE FROM planet_motion_votes WHERE motionid = ANY($1::text[])",
                            motion_ids
                        )
                    await _upsert_records(con, "planet_motion_votes", planet_motion_votes, MOTION_VOTES_UNIQUE_KEYS)

                if planet_motion_components:
                    await _upsert_records(con, "planet_motion_components", planet_motion_components, MOTION_COMPONENTS_UNIQUE_KEYS)

        end_time = time.perf_counter()
        logger.debug(f"Single planet motion processing took {end_time - start_time:.4f}s.")
        return {"success": True, "message": f"Processed {total_records} single planet motion records."}

    except Exception as e:
        logger.error(f"Error processing single planet motion message: {e}", exc_info=True)
        raise


async def handle_planet_motions_message(db, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles list/batch converted planet motions data (motions, votes, components),
    performs bulk UPSERTs into PostgreSQL.
    """
    start_time = time.perf_counter()
    logger.debug("Starting processing planet motions batch data.")

    converted_data = data.get("data", {})
    planet_motions = converted_data.get("planet_motions", [])
    planet_motion_votes = converted_data.get("planet_motion_votes", [])
    planet_motion_components = converted_data.get("planet_motion_components", [])

    total_records = len(planet_motions) + len(planet_motion_votes) + len(planet_motion_components)

    if total_records == 0:
        return {"success": True, "message": "No planet motions batch data to process."}

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                if planet_motions:
                    await _upsert_records(con, "planet_motions", planet_motions, MOTIONS_UNIQUE_KEYS)

                if planet_motion_votes:
                    motion_ids = list({v["motionid"] for v in planet_motion_votes if "motionid" in v})
                    if motion_ids:
                        await con.execute(
                            "DELETE FROM planet_motion_votes WHERE motionid = ANY($1::text[])",
                            motion_ids
                        )
                    await _upsert_records(con, "planet_motion_votes", planet_motion_votes, MOTION_VOTES_UNIQUE_KEYS)

                if planet_motion_components:
                    await _upsert_records(con, "planet_motion_components", planet_motion_components, MOTION_COMPONENTS_UNIQUE_KEYS)

        end_time = time.perf_counter()
        logger.debug(f"Planet motions batch processing took {end_time - start_time:.4f}s.")
        return {"success": True, "message": f"Processed {total_records} planet motion batch records."}

    except Exception as e:
        logger.error(f"Error processing planet motions batch message: {e}", exc_info=True)
        raise
