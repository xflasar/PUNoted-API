import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from websockets.exceptions import ConnectionClosedOK

# --- Pydantic Models ---


class FlightPlan(BaseModel):
    origin: str
    destination: str
    currentSystem: str
    status: str  # 'IN_FLIGHT', 'DOCKED', 'PENDING'
    departureTime: int
    arrivalTime: int


class ContractShipmentMaterial(BaseModel):
    materialId: str
    ticker: str
    requiredAmount: int
    amountOnShip: int


class ContractShipmentResult(BaseModel):
    contractLocalId: str
    shipId: str
    shipName: str
    shipFlightPlan: FlightPlan
    materials: List[ContractShipmentMaterial]
    totalMaterialsRequired: int
    totalMaterialsOnShips: int


# --- Helper Functions ---


def json_datetime_serializer(obj):
    if isinstance(obj, datetime):
        # Format as Unix timestamp in milliseconds
        return int(obj.timestamp() * 1000)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


logger = logging.getLogger(__name__)

# --- Connection Manager ---


class ConnectionManager:
    """
    Manages active WebSocket connections grouped by dynamic, data-centric channels
    (e.g., contract:ID, map:user:ID).
    """

    # Map 1: CHANNELS -> List of WebSockets subscribed to that channel
    channel_connections: Dict[str, List[WebSocket]] = {}

    # Map 2: SOCKET -> List of Channels it is currently subscribed to (for efficient cleanup)
    socket_channel_map: Dict[WebSocket, List[str]] = {}

    # Map 3: SOCKET -> User ID (for easy retrieval of user_id during cleanup/broadcast)
    socket_user_map: Dict[WebSocket, str] = {}

    # Map 4: Global user connections (for sending personal notifications)
    user_connections: Dict[str, List[WebSocket]] = {}

    # Map 5: Last known state (used for delta/patch calculation - retains structure from original)
    last_known_state: Dict[str, List[ContractShipmentResult]] = {}

    lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, user_id: str):
        """Accepts a new connection and initializes mappings."""
        await websocket.accept()

        async with self.lock:
            # 1. Add to Global user-specific list
            self.user_connections.setdefault(user_id, []).append(websocket)

            # 2. Initialize mappings for the socket
            self.socket_user_map[websocket] = user_id
            self.socket_channel_map[websocket] = []

        logger.debug(f"Client {user_id} connected.")

    async def disconnect(self, websocket: WebSocket, user_id: str):
        """Removes a disconnected connection from all relevant lists, safely."""

        async with self.lock:
            # 1. Get and remove channels from the socket's list
            channels_to_cleanup = self.socket_channel_map.pop(websocket, [])

            # 2. Remove the socket from all channel lists
            for channel_id in channels_to_cleanup:
                if channel_id in self.channel_connections and websocket in self.channel_connections[channel_id]:
                    self.channel_connections[channel_id].remove(websocket)
                    if not self.channel_connections[channel_id]:
                        del self.channel_connections[channel_id]

            # 3. Remove from Global user-specific list
            if user_id in self.user_connections and websocket in self.user_connections[user_id]:
                self.user_connections[user_id].remove(websocket)
                if not self.user_connections[user_id]:
                    del self.user_connections[user_id]

            # 4. Remove from socket_user_map
            self.socket_user_map.pop(websocket, None)

        logger.debug(f"Client {user_id} disconnected.")

    # --- Subscription Management ---

    async def subscribe(self, websocket: WebSocket, channel_id: str):
        """Adds a WebSocket to a dynamic channel ID."""
        async with self.lock:
            # Add to Channel list
            if websocket not in self.channel_connections.setdefault(channel_id, []):
                self.channel_connections[channel_id].append(websocket)

            # Update the socket's own list of channels
            if channel_id not in self.socket_channel_map.get(websocket, []):
                self.socket_channel_map.setdefault(websocket, []).append(channel_id)
        logger.debug(f"WebSocket subscribed to channel: {channel_id}")

    async def unsubscribe(self, websocket: WebSocket, channel_id: str):
        """Removes a WebSocket from a dynamic channel ID."""
        async with self.lock:
            # Remove from Channel list
            if channel_id in self.channel_connections and websocket in self.channel_connections[channel_id]:
                self.channel_connections[channel_id].remove(websocket)
                if not self.channel_connections[channel_id]:
                    del self.channel_connections[channel_id]

            # Update the socket's own list of channels
            if channel_id in self.socket_channel_map.get(websocket, []):
                self.socket_channel_map[websocket].remove(channel_id)
        logger.debug(f"WebSocket unsubscribed from channel: {channel_id}")

    # --- Cleanup Helper ---

    async def _cleanup_disconnected(self, disconnected_websockets: List[WebSocket]):
        """
        Called by broadcast() to gracefully handle websockets that failed to send.
        """
        for ws in disconnected_websockets:
            user_id = self.socket_user_map.get(ws)

            if user_id:
                # Call the full disconnect logic which handles cleanup across all maps
                await self.disconnect(ws, user_id)
            else:
                # Fallback for sockets without user_id (shouldn't happen with connect)
                logger.warning("Disconnected WebSocket found with no associated user_id.")

    # --- Broadcast Methods ---

    async def send_to_user(self, user_id: str, message: str):
        """Sends a personal message to all active connections for a specific user ID."""
        async with self.lock:
            connections = self.user_connections.get(user_id, []).copy()

        disconnected_websockets = []
        for connection in connections:
            try:
                await connection.send_text(message)
            except (WebSocketDisconnect, ConnectionClosedOK):
                disconnected_websockets.append(connection)
            except Exception as e:
                logger.error(f"Error sending message to user {user_id}: {e}")
                disconnected_websockets.append(connection)

        if disconnected_websockets:
            # Cleanup helper doesn't need channel_id here, as it cleans up globally
            await self._cleanup_disconnected(disconnected_websockets)

    async def broadcast(
        self,
        channel_id: str,
        message: str,
        exclude_websocket: Optional[WebSocket] = None,
    ):
        """Sends a message to all clients subscribed to a specific channel."""

        async with self.lock:
            connections = self.channel_connections.get(channel_id, []).copy()

        disconnected_websockets = []
        for connection in connections:
            if connection is not exclude_websocket:
                try:
                    await connection.send_text(message)
                except (WebSocketDisconnect, ConnectionClosedOK):
                    disconnected_websockets.append(connection)
                except Exception as e:
                    logger.error(f"Error broadcasting to channel {channel_id}: {e}")
                    disconnected_websockets.append(connection)

        if disconnected_websockets:
            await self._cleanup_disconnected(disconnected_websockets)

    async def broadcast_patch(self, contract_id: str, patch_payload: List[Dict[str, Any]]):
        """Sends the JSON Patch delta to all connected clients on a channel."""

        # NOTE: This method should now use self.broadcast with the channel_id 'contract:{contract_id}'

        channel_id = f"contract:{contract_id}"

        message = {
            "type": "SHIPMENT_UPDATE",
            "resourceId": contract_id,
            "patch": patch_payload,
        }
        json_payload = json.dumps(message, default=json_datetime_serializer)

        # Use the standard broadcast method
        await self.broadcast(channel_id, json_payload)


manager = ConnectionManager()
