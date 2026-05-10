import logging
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)

async def handle_leaderboard_scores(conn, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Asynchronously handles inserting/updating leaderboard score history.
    Expects the payload to already be processed by the converter and wrapped in the 'data' key.
    """
    start_time = time.perf_counter()

    # Extract the pre-converted data
    data = raw_payload.get("data", {})
    scores = data.get("leaderboard_scores", [])

    if not scores:
        return {"success": True, "message": "No leaderboard scores found in data."}

    # Convert the list of dictionaries into a strict list of tuples for executemany optimization
    values = [
        (
            s["category"],
            s["time_range"],
            s["material_ticker"],
            s["company_id"],
            s["rank"],
            s["score"]
        )
        for s in scores
    ]

    SQL_UPSERT = """
        INSERT INTO leaderboard_history (
            record_date, category, time_range, material_ticker, company_id, rank, score
        ) VALUES (
            CURRENT_DATE, $1, $2, $3, $4, $5, $6
        )
        ON CONFLICT (record_date, category, time_range, material_ticker, company_id) 
        DO UPDATE SET 
            rank = EXCLUDED.rank,
            score = EXCLUDED.score;
    """

    try:
        async with conn.pool.acquire() as con:
            async with con.transaction():
                await con.executemany(SQL_UPSERT, values)

        logger.debug(f"Processed {len(values)} leaderboard records in {time.perf_counter() - start_time:.4f}s.")
        return {"success": True, "message": f"Processed {len(values)} leaderboard records."}

    except Exception as e:
        logger.error(f"Database error processing 'LEADERBOARD_SCORES': {e}", exc_info=True)
        raise
