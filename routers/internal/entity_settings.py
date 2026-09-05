import json
import logging
from typing import Any, Dict, Optional
from pydantic import BaseModel

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from app.core.security import require_internal_origin
from auth import get_current_user_id

entity_settings_router = APIRouter(dependencies=[Depends(require_internal_origin)])
logger = logging.getLogger("entity_settings_router")


class EntitySettingsPayload(BaseModel):
    domain: str        # 'site', 'ship', 'cx', 'contract', 'storage', 'page'
    entity_id: str     # siteid, shipid, 'RAT_ICA', contractid, or 'GLOBAL'
    settings: Dict[str, Any]


@entity_settings_router.get("")
async def get_domain_entity_settings(
    request: Request,
    domain: str = Query(..., description="Domain e.g. site, ship, cx, contract, storage"),
    user_id: str = Depends(get_current_user_id)
):
    """
    Returns all entity settings for a specific domain for the user as a map { entity_id: settings }.
    """
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT entity_id, settings FROM user_entity_settings WHERE accountid = $1 AND domain = $2;",
                user_id, domain
            )
            result = {}
            for r in rows:
                settings_val = r["settings"]
                if isinstance(settings_val, str):
                    settings_val = json.loads(settings_val)
                result[r["entity_id"]] = settings_val

            return {"success": True, "domain": domain, "entities": result}
    except Exception as e:
        logger.error(f"Error fetching domain entity settings ({domain}) for {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch entity settings")


@entity_settings_router.put("")
async def save_entity_settings(
    request: Request,
    body: EntitySettingsPayload,
    user_id: str = Depends(get_current_user_id)
):
    """
    Upserts per-entity settings for any page/domain.
    """
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            settings_json = json.dumps(body.settings)
            await conn.execute(
                """
                INSERT INTO user_entity_settings (accountid, domain, entity_id, settings, updated_at)
                VALUES ($1, $2, $3, $4::jsonb, CURRENT_TIMESTAMP)
                ON CONFLICT (accountid, domain, entity_id) DO UPDATE SET
                    settings = EXCLUDED.settings,
                    updated_at = CURRENT_TIMESTAMP;
                """,
                user_id, body.domain, body.entity_id, settings_json
            )
            return {"success": True, "message": f"Settings saved for {body.domain}:{body.entity_id}"}
    except Exception as e:
        logger.error(f"Error saving entity settings for {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to save entity settings")
