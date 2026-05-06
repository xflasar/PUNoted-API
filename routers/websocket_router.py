import json
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect, status

from app.core.security import require_internal_origin
from auth import validate_token

from helpers.cx_analysis import get_cx_dashboard_data
from helpers.fetchdb import fetch_initial_ship_data
from helpers.shipments import (
    fetch_active_shipments_structured as fetch_active_shipments,
)
from managers.global_ws_manager import global_ws_manager

ws_router = APIRouter(dependencies=[Depends(require_internal_origin)])
logger = logging.getLogger("WSRouter")


# --- Helper: JSON Serializer for Decimals & Dates ---
def serialize_payload(data: Any) -> str:
    def default_serializer(obj):
        if isinstance(obj, (datetime, date)):
            # Unix timestamp in milliseconds for frontend compatibility
            return int(obj.timestamp() * 1000) if hasattr(obj, "timestamp") else obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return str(obj)

    return json.dumps(data, default=default_serializer)


async def fetch_user_corp_id(pool, userid: str):
    async with pool.acquire() as con:
        userdataid = await con.fetchval("SELECT userdataid FROM users WHERE accountid = $1", userid)
        if userdataid:
            return await con.fetchval(
                "SELECT corporationid FROM corporation_shareholders WHERE userid = $1",
                userdataid,
            )
    return None


async def get_user_from_token(websocket: WebSocket):
    token = websocket.query_params.get("token")
    if not token:
        return None

    pool = websocket.app.state.db.pool
    async with pool.acquire() as conn:
        user_id, _, error = await validate_token(conn, token)

        if not error and user_id.startswith("rec_"):
            row = await conn.fetchrow("SELECT accountid FROM users WHERE xata_id=$1", user_id)
            if row:
                user_id = str(row["accountid"])

        if error:
            return None
        return user_id


@ws_router.websocket("/ws/global")
async def global_websocket_endpoint(websocket: WebSocket):
    # 1. Authentication
    user_id = await get_user_from_token(websocket)
    if not user_id:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    # 2. Connect to Manager
    await global_ws_manager.connect(websocket, user_id)
    db = websocket.app.state.db

    try:
        # --- INITIAL DATA & AUTO-SUBSCRIPTIONS ---

        # Subscribe to personal map channel
        await global_ws_manager.subscribe(websocket, f"map:user:{user_id}")

        # Subscribe to corporation map channel if applicable
        corp_id = await fetch_user_corp_id(db.pool, user_id)
        if corp_id:
            await global_ws_manager.subscribe(websocket, f"map:corp:{corp_id}")

        # Send Initial Ship Data
        initial_ship_data = await fetch_initial_ship_data(db, user_id)
        await websocket.send_text(serialize_payload({"type": "INITIAL_SHIP_DATA", "data": initial_ship_data}))

        initial_cx_data = await get_cx_dashboard_data(
            db, user_id, datetime.utcnow() - timedelta(days=7), datetime.utcnow()
        )
        await websocket.send_text(
            serialize_payload(
                {
                    "type": "DASHBOARD_UPDATE",
                    "data": {"cx_analytics": initial_cx_data},
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )
        )

        active_shipments = await fetch_active_shipments(db, user_id)
        await websocket.send_text(serialize_payload({"type": "INITIAL_SHIPMENT_DATA", "data": active_shipments}))

        # 3. Message Loop
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                action = msg.get("action")

                # --- A. SUBSCRIPTIONS ---
                if action == "SUBSCRIBE":
                    channel = msg.get("channel")
                    if channel:
                        await global_ws_manager.subscribe(websocket, channel)

                elif action == "UNSUBSCRIBE":
                    channel = msg.get("channel")
                    if channel:
                        await global_ws_manager.unsubscribe(websocket, channel)

                # --- B. DASHBOARD FETCH LOGIC ---
                elif action == "FETCH_DASHBOARD":
                    filters = msg.get("filters", {})
                    range_mode = filters.get("range", "7D")  # Default to "7D" string
                    start_str = filters.get("startDate")
                    end_str = filters.get("endDate")
                    exchange = filters.get("exchange")

                    # 1. Handle "ALL" (Dynamic calculation inside function)
                    if range_mode == "ALL":
                        start = None
                        end = None

                    # 2. Handle "CUSTOM" (Explicit dates from date picker)
                    elif range_mode == "CUSTOM" and start_str and end_str:
                        start = datetime.fromisoformat(start_str.replace("Z", ""))
                        end = datetime.fromisoformat(end_str.replace("Z", ""))

                    elif start_str and end_str:
                        start = datetime.fromisoformat(start_str.replace("Z", ""))
                        end = datetime.fromisoformat(end_str.replace("Z", ""))
                    # 3. Handle Presets ("24H", "7D", "30D", "1H")
                    else:
                        end = datetime.utcnow()

                        if range_mode == "24H":
                            start = end - timedelta(hours=24)
                        elif range_mode == "7D":
                            start = end - timedelta(days=7)
                        elif range_mode == "30D":
                            start = end - timedelta(days=30)
                        elif range_mode == "1H":
                            start = end - timedelta(hours=1)
                        else:
                            # Fallback safety (e.g. if range is missing or unknown)
                            start = end - timedelta(days=7)

                    cx_data = await get_cx_dashboard_data(db, user_id, start, end, exchange)

                    response = {
                        "type": "DASHBOARD_UPDATE",
                        "data": {"cx_analytics": cx_data},
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                    await websocket.send_text(serialize_payload(response))

                # --- C. GROUP LOGIC ---
                elif action == "GROUP_UPDATE":
                    group_id = msg.get("groupId")
                    if group_id:
                        await global_ws_manager.broadcast(f"group:{group_id}", msg, exclude=websocket)

                # --- D. HEARTBEAT ---
                elif action == "PING":
                    await websocket.send_json({"type": "PONG"})

            except json.JSONDecodeError:
                pass
            except Exception as e:
                logger.error(f"Error handling WS message: {e}")

    except WebSocketDisconnect:
        await global_ws_manager.disconnect(websocket)
