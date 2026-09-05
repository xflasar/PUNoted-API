import logging
from typing import Any, Dict, List, Optional
from pydantic import BaseModel

from fastapi import APIRouter, HTTPException, Request

announcements_router = APIRouter()
logger = logging.getLogger("announcements_router")


class PublicAnnouncementPayload(BaseModel):
    title: str
    message: str
    severity: Optional[str] = "info" # 'info', 'warning', 'success', 'error'
    link: Optional[str] = None
    is_active: Optional[bool] = True


@announcements_router.get("")
async def get_public_announcements(request: Request):
    """
    Public, unauthenticated endpoint returning active global announcements.
    """
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, title, message, severity, link, created_at 
                FROM public_announcements 
                WHERE is_active = TRUE 
                ORDER BY created_at DESC LIMIT 5;
                """
            )
            return {
                "success": True,
                "announcements": [dict(r) for r in rows]
            }
    except Exception as e:
        logger.error(f"Error fetching public announcements: {e}", exc_info=True)
        # Fallback graceful payload
        return {
            "success": True,
            "announcements": [
                {
                    "id": 1,
                    "title": "Welcome to PUNoted",
                    "message": "Real-time Apex Prosperity telemetry, storage inventories, and cross-CX arbitrage analytics.",
                    "severity": "info",
                    "link": None,
                    "created_at": None
                }
            ]
        }


@announcements_router.post("")
async def create_public_announcement(request: Request, body: PublicAnnouncementPayload):
    """
    Creates a new public system announcement broadcast.
    """
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            row_id = await conn.fetchval(
                """
                INSERT INTO public_announcements (title, message, severity, link, is_active, created_at)
                VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
                RETURNING id;
                """,
                body.title, body.message, body.severity, body.link, body.is_active
            )
            return {"success": True, "id": row_id, "message": "Public announcement broadcast created."}
    except Exception as e:
        logger.error(f"Error creating public announcement: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to create announcement")
