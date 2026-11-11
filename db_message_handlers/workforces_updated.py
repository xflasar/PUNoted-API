import logging
from typing import Any, Dict
from managers.global_ws_manager import global_ws_manager
from services.internal.corp_site_delta_service import compute_and_broadcast_site_delta
from db_message_handlers.workforce_data import handle_workforce_data_message

logger = logging.getLogger(__name__)


async def handle_workforces_updated_message(db: Any, raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Dedicated handler for WORKFORCE_WORKFORCES_UPDATED messages.
    Persists updated populations and needs for a specific site and notifies the client via WebSockets.
    """
    res = await handle_workforce_data_message(db, raw_data)

    user_account_id = raw_data.get("userId")
    payload = raw_data.get("data", [])
    site_id = payload[0].get("siteid") if payload and isinstance(payload[0], dict) else None

    if user_account_id and site_id:
        try:
            await global_ws_manager.send_personal_message(user_account_id, {"type": "REFRESH_DASHBOARD", "siteid": site_id})
            await compute_and_broadcast_site_delta(db, global_ws_manager, site_id, user_account_id)
        except Exception as ws_err:
            logger.error(f"[WEBSOCKET ERROR] Failed to send workforce update delta: {ws_err}")

    return res
