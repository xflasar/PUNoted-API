import json
import logging
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)


async def handle_system_data(db: Any, raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles incoming 'systems' data messages, converts them, and synchronizes
    them with the database.
    """
    start_time = time.perf_counter()
    logger.debug("Starting processing batch of systems data.")
    try:
        converted_data = raw_data["data"]

        async with db.pool.acquire() as con:
            async with con.transaction():
                system_id = converted_data["id"]
                asteroid_count = converted_data["meteoroidDensity"]
                luminosity = converted_data["luminosity"]
                mass = converted_data["mass"]
                masssol = converted_data["masssol"]

                stations = converted_data["celestialbodies"]
                del converted_data["celestialbodies"]

                for station in stations:
                    station_id = station["stationid"]

                    orbit_dict = station["orbit"]
                    orbit_json_string = json.dumps(orbit_dict)

                    station_query = """
                        INSERT INTO stations (stationid, systemid, orbit)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (stationid) DO UPDATE
                        SET systemid = EXCLUDED.systemid,
                            orbit = EXCLUDED.orbit;
                    """

                    await con.execute(station_query, station_id, system_id, orbit_json_string)

                query = """
                    UPDATE systems 
                    SET microasteroidcount = $1,
                    mass = $2,
                    masssol = $3
                    WHERE systemid = $4;
                """

                await con.execute(query, asteroid_count, mass, masssol, system_id)

        end_time = time.perf_counter()
        logger.debug(f"Total processing time took {end_time - start_time:.4f} seconds.")
        logger.debug("Successfully processed system data.")
        return {"success": True, "message": "System data processed successfully."}

    except Exception as e:
        logger.error(f"Error handling systems data: {e}", exc_info=True)
        raise
