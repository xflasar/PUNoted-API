# db_message_handlers/planet_government.py
import logging
import time
from typing import Any, Dict, List

from helpers.db import _upsert_records
from db_message_handlers.planet_motions import (
    MOTIONS_UNIQUE_KEYS,
    MOTION_VOTES_UNIQUE_KEYS,
    MOTION_COMPONENTS_UNIQUE_KEYS,
)

logger = logging.getLogger(__name__)

# Primary Keys / Unique Keys for Government Terms & Candidates
GOV_TERMS_UNIQUE_KEYS = ["termid"]
GOV_CANDIDATES_UNIQUE_KEYS = ["id"]


async def handle_planet_government_term_message(db, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles single converted planet government term data (and any embedded motions),
    performs bulk UPSERTs into PostgreSQL.
    """
    start_time = time.perf_counter()
    logger.debug("Starting processing single planet government term data.")

    converted_data = data.get("data", {})
    gov_terms = converted_data.get("gov_terms", [])
    gov_candidates = converted_data.get("gov_candidates", [])
    planet_motions = converted_data.get("planet_motions", [])
    planet_motion_votes = converted_data.get("planet_motion_votes", [])
    planet_motion_components = converted_data.get("planet_motion_components", [])

    total_records = (
        len(gov_terms)
        + len(gov_candidates)
        + len(planet_motions)
        + len(planet_motion_votes)
        + len(planet_motion_components)
    )

    if total_records == 0:
        return {"success": True, "message": "No single planet government term data to process."}

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                if gov_terms:
                    await _upsert_records(con, "planet_government_terms", gov_terms, GOV_TERMS_UNIQUE_KEYS)

                if gov_candidates:
                    term_ids = list({c["termid"] for c in gov_candidates if "termid" in c})
                    if term_ids:
                        await con.execute(
                            "DELETE FROM planet_government_candidates WHERE termid = ANY($1::text[])",
                            term_ids
                        )
                    await _upsert_records(con, "planet_government_candidates", gov_candidates, GOV_CANDIDATES_UNIQUE_KEYS)

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
        logger.debug(f"Single planet government term processing took {end_time - start_time:.4f}s.")
        return {"success": True, "message": f"Processed {total_records} single planet government term records."}

    except Exception as e:
        logger.error(f"Error processing single planet government term message: {e}", exc_info=True)
        raise

# Currently this is unused
async def handle_planet_government_terms_message(db, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles list/batch converted planet government terms data (and any embedded motions),
    performs bulk UPSERTs into PostgreSQL.
    """
    start_time = time.perf_counter()
    logger.debug("Starting processing planet government terms batch data.")

    converted_data = data.get("data", {})
    gov_terms = converted_data.get("gov_terms", [])
    gov_candidates = converted_data.get("gov_candidates", [])
    planet_motions = converted_data.get("planet_motions", [])
    planet_motion_votes = converted_data.get("planet_motion_votes", [])
    planet_motion_components = converted_data.get("planet_motion_components", [])

    total_records = (
        len(gov_terms)
        + len(gov_candidates)
        + len(planet_motions)
        + len(planet_motion_votes)
        + len(planet_motion_components)
    )

    if total_records == 0:
        return {"success": True, "message": "No planet government terms batch data to process."}

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                if gov_terms:
                    await _upsert_records(con, "planet_government_terms", gov_terms, GOV_TERMS_UNIQUE_KEYS)

                if gov_candidates:
                    term_ids = list({c["termid"] for c in gov_candidates if "termid" in c})
                    if term_ids:
                        await con.execute(
                            "DELETE FROM planet_government_candidates WHERE termid = ANY($1::text[])",
                            term_ids
                        )
                    await _upsert_records(con, "planet_government_candidates", gov_candidates, GOV_CANDIDATES_UNIQUE_KEYS)

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
        logger.debug(f"Planet government terms batch processing took {end_time - start_time:.4f}s.")
        return {"success": True, "message": f"Processed {total_records} planet government terms batch records."}

    except Exception as e:
        logger.error(f"Error processing planet government terms batch message: {e}", exc_info=True)
        raise
