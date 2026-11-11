import asyncio
import logging
import time
from typing import Any, Dict, List
import asyncpg

from managers.global_ws_manager import global_ws_manager
from services.internal.corp_site_delta_service import compute_and_broadcast_site_delta
from helpers.db import _upsert_records

logger = logging.getLogger(__name__)


async def handle_site_platform_updated_message(db, raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles SITE_PLATFORM_UPDATED message for a single building platform on a site.
    Performs UPSERT of the platform, synchronizes reclaimable and repair materials,
    and broadcasts a WebSocket delta update to the user.
    """
    start_time = time.perf_counter()
    converted_data = raw_payload.get("data", {}) if isinstance(raw_payload, dict) else {}

    site_id = converted_data.get("siteid")
    platform = converted_data.get("platform", {})

    if not site_id:
        logger.warning(f"[WARNING] Missing 'siteid' in SITE_PLATFORM_UPDATED payload: {list(converted_data.keys())}")
        return {"success": False, "message": "Missing siteid"}

    platform_id = platform.get("platformid")
    if not platform_id:
        logger.warning(f"[WARNING] Missing 'platformid' for site {site_id}")
        return {"success": False, "message": "Missing platformid"}

    reclaimable_materials = platform.get("reclaimable_materials") or []
    repair_materials = platform.get("repair_materials") or []

    clean_platform = platform.copy()
    clean_platform.pop("reclaimable_materials", None)
    clean_platform.pop("repair_materials", None)

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                # Lock parent site row to serialize updates
                await con.execute("SELECT siteid FROM sites WHERE siteid = $1 FOR UPDATE;", site_id)

                # 1. UPSERT site_platform record
                keys = list(clean_platform.keys())
                keys_str = ", ".join(keys)
                placeholders = ", ".join([f"${i + 1}" for i in range(len(keys))])
                update_clause = ", ".join([f"{k} = EXCLUDED.{k}" for k in keys if k != "platformid"])

                upsert_query = f"""
                    INSERT INTO site_platforms ({keys_str})
                    VALUES ({placeholders})
                    ON CONFLICT (platformid) DO UPDATE
                    SET {update_clause};
                """
                await con.execute(upsert_query, *[clean_platform[k] for k in keys])

                # 2. Synchronize materials for this platform
                # Delete existing materials for this specific platform to keep state completely fresh
                await con.execute("DELETE FROM platform_materials WHERE platformid = $1;", platform_id)

                all_materials = reclaimable_materials + repair_materials
                if all_materials:
                    mat_keys = ["platformid", "materialid", "amount", "materialtype"]
                    mat_cols = ", ".join(mat_keys)
                    mat_placeholders = ", ".join([f"${i + 1}" for i in range(len(mat_keys))])
                    mat_insert_query = f"""
                        INSERT INTO platform_materials ({mat_cols})
                        VALUES ({mat_placeholders})
                        ON CONFLICT (platformid, materialid, materialtype) DO UPDATE
                        SET amount = EXCLUDED.amount;
                    """
                    mat_tuples = [tuple(m.get(k) for k in mat_keys) for m in all_materials]
                    await con.executemany(mat_insert_query, mat_tuples)

        elapsed = time.perf_counter() - start_time
        if elapsed > 1.0:
            logger.warning(f"[WARNING] Slow platform update! Site {site_id} platform {platform_id} took {elapsed:.2f}s")

        # 3. Broadcast real-time WebSocket event
        user_id = raw_payload.get("userId") or converted_data.get("userid")
        if user_id and site_id:
            try:
                await global_ws_manager.send_personal_message(user_id, {"type": "REFRESH_DASHBOARD", "siteid": site_id})
                await compute_and_broadcast_site_delta(db, global_ws_manager, site_id, user_id)
            except Exception as ws_err:
                logger.error(f"[WEBSOCKET ERROR] Failed to send site platform delta update: {ws_err}")

        return {"success": True, "message": f"Updated platform {platform_id} on site {site_id}"}

    except Exception as e:
        logger.error(f"[ERROR] Failed to update site platform {platform_id} for site {site_id}: {e}", exc_info=True)
        raise
