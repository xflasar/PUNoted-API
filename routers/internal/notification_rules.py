import json
import logging
from typing import Any, Dict, List, Optional
from pydantic import BaseModel

from fastapi import APIRouter, Depends, HTTPException, Request
from app.core.security import require_internal_origin
from auth import get_current_user_id
from services.notification_evaluator import get_user_rules

notification_rules_router = APIRouter(dependencies=[Depends(require_internal_origin)])
logger = logging.getLogger("notification_rules_router")


class MarketWatcherRule(BaseModel):
    ticker: str
    exchange: Optional[str] = "ICA"
    target_price: float
    direction: Optional[str] = "below"  # 'below' or 'above'


class NotificationRulesPayload(BaseModel):
    fleet_enabled: Optional[bool] = True
    health_threshold: Optional[int] = 70
    storage_enabled: Optional[bool] = True
    storage_threshold: Optional[int] = 90
    production_enabled: Optional[bool] = True
    supply_days_threshold: Optional[float] = 1.0
    contracts_enabled: Optional[bool] = True
    cx_enabled: Optional[bool] = True
    cx_market_watchers: Optional[List[MarketWatcherRule]] = []


@notification_rules_router.get("/settings")
async def get_notification_settings(
    request: Request,
    user_id: str = Depends(get_current_user_id)
):
    """
    Returns user notification preferences and CX Market Watchers rules.
    """
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            rules = await get_user_rules(conn, user_id)
            if isinstance(rules.get("cx_market_watchers"), str):
                rules["cx_market_watchers"] = json.loads(rules["cx_market_watchers"])
            return {"success": True, "settings": rules}
    except Exception as e:
        logger.error(f"Error getting notification rules for {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch notification rules")


@notification_rules_router.put("/settings")
async def save_notification_settings(
    request: Request,
    body: NotificationRulesPayload,
    user_id: str = Depends(get_current_user_id)
):
    """
    Saves updated notification preferences and CX Market Watchers rules.
    """
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            async with conn.transaction():
                watchers_json = json.dumps([w.dict() for w in body.cx_market_watchers]) if body.cx_market_watchers else "[]"
                await conn.execute("DELETE FROM user_notification_rules WHERE accountid = $1;", user_id)
                await conn.execute(
                    """
                    INSERT INTO user_notification_rules (
                        accountid, fleet_enabled, health_threshold, storage_enabled, storage_threshold,
                        production_enabled, supply_days_threshold, contracts_enabled, cx_enabled,
                        cx_market_watchers, updated_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, CURRENT_TIMESTAMP);
                    """,
                    user_id, body.fleet_enabled, body.health_threshold, body.storage_enabled, body.storage_threshold,
                    body.production_enabled, body.supply_days_threshold, body.contracts_enabled, body.cx_enabled,
                    watchers_json
                )
            return {"success": True, "message": "Notification preferences saved successfully."}
    except Exception as e:
        logger.error(f"Error saving notification rules for {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to save notification rules")
