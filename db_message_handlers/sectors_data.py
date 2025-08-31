import logging
import time
from typing import Dict, Any

logger = logging.getLogger(__name__)

async def handle_sectors_message(db, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    start_time = time.perf_counter()
    logger.info("Starting processing batch of sector data.")

    # Convert the payload to the required lists
    converted_data = raw_payload['data']
    sectors_data = converted_data['sectors']
    subsectors_data = converted_data['subsectors']
    vertices_data = converted_data['subsector_vertices']

    if not sectors_data:
        logger.info("No sectors found in payload. Exiting.")
        return {"success": True, "message": "No sectors to process."}

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                # 1. Bulk UPSERT for the 'sectors' table
                sectors_to_upsert = [(s['externalsectorid'], s['name'], s['hexq'], s['hexr'], s['hexs'], s['size']) for s in sectors_data]
                await con.executemany("""
                    INSERT INTO sectors (externalsectorid, name, hexq, hexr, hexs, size)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (externalsectorid) DO UPDATE SET
                        name = EXCLUDED.name, hexq = EXCLUDED.hexq, hexr = EXCLUDED.hexr,
                        hexs = EXCLUDED.hexs, size = EXCLUDED.size;
                """, sectors_to_upsert)
                logger.info(f"Successfully UPSERTed {len(sectors_to_upsert)} sectors.")

                # 2. Bulk UPSERT for the 'subsectors' table
                subsectors_to_upsert = []
                for ss in subsectors_data:
                    internal_sector_id = ss['externalsectorid']
                    if internal_sector_id:
                        subsectors_to_upsert.append((ss['externalsubsectorid'], internal_sector_id))

                await con.executemany("""
                    INSERT INTO subsectors (externalsubsectorid, externalsectorid)
                    VALUES ($1, $2)
                    ON CONFLICT (externalsubsectorid) DO UPDATE SET
                        externalsectorid = EXCLUDED.externalsectorid;
                """, subsectors_to_upsert)
                logger.info(f"Successfully UPSERTed {len(subsectors_to_upsert)} subsectors.")

                # 3. Bulk UPSERT for the 'subsector_vertices' table
                vertices_to_upsert = []
                for v in vertices_data:
                    internal_subsector_id = v['externalsubsectorid']
                    if internal_subsector_id:
                        vertices_to_upsert.append((internal_subsector_id, v['index'], v['x'], v['y'], v['z']))

                await con.executemany("""
                    INSERT INTO subsector_vertices (externalsubsectorid, index, x, y, z)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (externalsubsectorid, index) DO UPDATE SET
                        x = EXCLUDED.x, y = EXCLUDED.y, z = EXCLUDED.z;
                """, vertices_to_upsert)
                logger.info(f"Successfully UPSERTed {len(vertices_to_upsert)} vertices.")

            end_time = time.perf_counter()
            logger.info(f"Total processing for the entire batch took {end_time - start_time:.4f} seconds.")
            return {"success": True, "message": "Batch of sector data processed successfully."}

    except Exception as e:
        logger.error(f"Error processing batch of sector data: {e}", exc_info=True)
        return {"success": False, "message": "Failed to process sector data."}