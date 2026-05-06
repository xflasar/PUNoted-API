import json
import logging
from datetime import date, datetime
from typing import Any, Dict

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.services.groups import (
    LIVE_GROUP_STATES,
    apply_change_to_live_chain,
    get_latest_chain_from_db,
)
from helpers.fetchdb import fetch_initial_ship_data
from websocket.websocket_manager import manager

# Initialize Router
ws_router = APIRouter()
logger = logging.getLogger(__name__)


# Helper for JSON serialization
def json_datetime_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


# --- DATABASE HELPER ---
async def fetch_user_corp_id(pool, userid: str):
    async with pool.acquire() as con:
        userdataid = await con.fetchval("SELECT userdataid FROM users WHERE accountid = $1", userid)
        if userdataid:
            return await con.fetchval(
                "SELECT corporationid FROM corporation_shareholders WHERE userid = $1",
                userdataid,
            )
    return None


# ==============================================================================
# 1. GROUP COLLABORATION ENDPOINT
# ==============================================================================
@ws_router.websocket("/group/{group_id}/{user_id}")
async def websocket_group_endpoint(websocket: WebSocket, group_id: str, user_id: str):
    """
    Handles real-time collaboration for production groups (Node moves, edits, etc.).
    """
    # We access the DB pool from the app state
    pool = websocket.app.state.db.pool

    try:
        await manager.connect(websocket, group_id, user_id)
        logger.debug(f"User {user_id} connected to Group {group_id}")

        while True:
            data = await websocket.receive_text()

            try:
                message: Dict[str, Any] = json.loads(data)
                message["groupId"] = group_id
            except json.JSONDecodeError:
                continue

            # --- A. INITIAL SYNC REQUEST ---
            if message.get("type") == "INITIAL_LOAD_REQUEST":
                latest_chain = None

                # Check Cache First
                if group_id in LIVE_GROUP_STATES:
                    latest_chain = LIVE_GROUP_STATES[group_id]
                else:
                    # Fallback to DB
                    latest_chain = await get_latest_chain_from_db(pool, group_id)
                    if latest_chain:
                        LIVE_GROUP_STATES[group_id] = latest_chain
                    else:
                        # New/Empty Group
                        latest_chain = {"nodes": [], "links": [], "members": []}
                        LIVE_GROUP_STATES[group_id] = latest_chain

                await websocket.send_text(
                    json.dumps(
                        {
                            "type": "FULL_CHAIN_STATE",
                            "userId": "SERVER",
                            "payload": latest_chain,
                        }
                    )
                )
                continue

            # --- B. INCREMENTAL UPDATES (Broadcast + Persistence) ---
            if message.get("type") in [
                "NODE_MOVE",
                "NODE_ADD",
                "NODE_REMOVE",
                "NODE_UPDATE",
                "EDGE_UPDATE",
            ]:
                # Update in-memory state and trigger background save
                if group_id in LIVE_GROUP_STATES:
                    await apply_change_to_live_chain(pool, LIVE_GROUP_STATES[group_id], message, group_id)

                # Broadcast to others
                await manager.broadcast(group_id, data, exclude_websocket=websocket)

            # --- C. EPHEMERAL EVENTS (Cursor moves, etc.) ---
            else:
                await manager.broadcast(group_id, data, exclude_websocket=websocket)

    except WebSocketDisconnect:
        logger.debug(f"User {user_id} disconnected from Group {group_id}")
    except Exception as e:
        logger.error(f"WS Error in group {group_id}: {e}")
    finally:
        await manager.disconnect(websocket, group_id, user_id)
        # Optional: Broadcast user leave event
        leave_msg = json.dumps({"type": "USER_LEAVE", "payload": {"user_id": user_id}})
        await manager.broadcast(group_id, leave_msg)


# ==============================================================================
# 2. USER GLOBAL DASHBOARD ENDPOINT
# ==============================================================================
@ws_router.websocket("/dashboard/{user_id}")
async def user_dashboard_websocket_endpoint(websocket: WebSocket, user_id: str):
    """
    Global channel for Map presence, ship updates, and alerts.
    """
    db = websocket.app.state.db

    try:
        await manager.connect(websocket, user_id)

        # Subscribe to standard channels
        await manager.subscribe(websocket, f"map:user:{user_id}")

        corp_id = await fetch_user_corp_id(db.pool, user_id)
        if corp_id:
            await manager.subscribe(websocket, f"map:corp:{corp_id}")

        # Send Initial Data
        initial_ship_data = await fetch_initial_ship_data(db, user_id)
        await websocket.send_text(
            json.dumps(
                {"type": "INITIAL_SHIP_DATA", "data": initial_ship_data},
                default=json_datetime_serializer,
            )
        )

        # Listen for client-side subscription changes
        while True:
            raw_data = await websocket.receive_text()
            message = json.loads(raw_data)
            action = message.get("action")
            channel = message.get("channel")

            if action == "SUBSCRIBE" and channel:
                await manager.subscribe(websocket, channel)
            elif action == "UNSUBSCRIBE" and channel:
                await manager.unsubscribe(websocket, channel)

    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WS Error in dashboard {user_id}: {e}")
    finally:
        await manager.disconnect(websocket, user_id)


# ==============================================================================
# 3. PUBLIC CONTRACT TRACKING ENDPOINT
# ==============================================================================
@ws_router.websocket("/ws/v1/tracking/{contract_id}")
async def public_shipment_websocket_endpoint(websocket: WebSocket, contract_id: str):
    """
    Read-only public channel for tracking a specific contract shipment.
    """
    contract_id = contract_id.upper()
    if not contract_id:
        await websocket.close(code=1008)
        return

    try:
        await manager.connect(websocket, contract_id)
        # Keep connection open for broadcasts
        while True:
            await websocket.receive_text()  # Ignore incoming data (Read-only)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WS Error in public tracking {contract_id}: {e}")
    finally:
        await manager.disconnect(websocket, contract_id)
