# managers/global_ws_manager.py
import asyncio
import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Set

from fastapi import WebSocket

logger = logging.getLogger("GlobalWS")


class GlobalConnectionManager:
    def __init__(self):
        # socket -> user_id
        self.active_sockets: Dict[WebSocket, str] = {}

        # user_id -> List[WebSocket] (A user can have multiple tabs open)
        self.user_sockets: Dict[str, List[WebSocket]] = {}

        # channel_name -> Set[WebSocket] (Pub/Sub channels)
        # Channels examples: "group:123", "dashboard:user:ABC", "public:contract:XYZ"
        self.channels: Dict[str, Set[WebSocket]] = {}

        self.lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, user_id: str):
        await websocket.accept()
        async with self.lock:
            self.active_sockets[websocket] = user_id
            if user_id not in self.user_sockets:
                self.user_sockets[user_id] = []
            self.user_sockets[user_id].append(websocket)

        logger.debug(f"WS Connected: User {user_id}")

    async def disconnect(self, websocket: WebSocket):
        async with self.lock:
            user_id = self.active_sockets.pop(websocket, None)

            # Remove from user list
            if user_id and user_id in self.user_sockets:
                if websocket in self.user_sockets[user_id]:
                    self.user_sockets[user_id].remove(websocket)
                if not self.user_sockets[user_id]:
                    del self.user_sockets[user_id]

            # Remove from ALL channels efficiently
            # (In production, you might want a reverse map socket->channels for speed)
            for channel, sockets in self.channels.items():
                if websocket in sockets:
                    sockets.remove(websocket)

            # Clean empty channels
            self.channels = {k: v for k, v in self.channels.items() if v}

        logger.debug(f"WS Disconnected: User {user_id}")

    async def subscribe(self, websocket: WebSocket, channel: str):
        """Subscribe a specific socket to a channel (e.g., 'group:123')"""
        async with self.lock:
            if channel not in self.channels:
                self.channels[channel] = set()
            self.channels[channel].add(websocket)
        logger.debug(f"Socket subscribed to {channel}")

    async def unsubscribe(self, websocket: WebSocket, channel: str):
        async with self.lock:
            if channel in self.channels and websocket in self.channels[channel]:
                self.channels[channel].remove(websocket)
                if not self.channels[channel]:
                    del self.channels[channel]

    async def broadcast(self, channel: str, message: dict, exclude: WebSocket = None):
        """Send JSON to everyone on a channel"""
        if channel not in self.channels:
            return

        payload = self._serialize(message)

        # Snapshot the set to avoid modification during iteration issues
        target_sockets = self.channels[channel].copy()

        for ws in target_sockets:
            if ws == exclude:
                continue
            try:
                await ws.send_text(payload)
            except Exception:
                # If send fails, assume disconnect and let the receive loop handle cleanup
                pass

    async def send_personal_message(self, user_id: str, message: dict):
        """Send to all tabs belonging to a specific user"""
        if user_id not in self.user_sockets:
            return

        payload = self._serialize(message)
        for ws in self.user_sockets[user_id]:
            try:
                await ws.send_text(payload)
            except Exception:
                pass

    def _serialize(self, data: Any) -> str:
        """Helper to safely serialize datetime/decimals"""

        def default_serializer(obj):
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            return str(obj)

        return json.dumps(data, default=default_serializer)


# Singleton Instance
global_ws_manager = GlobalConnectionManager()
