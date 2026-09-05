import logging
from typing import Any, Dict, List, Optional
from pydantic import BaseModel

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from app.core.security import require_internal_origin
from auth import get_current_user_id
from services.notification_evaluator import evaluate_user_telemetry_notifications

notifications_router = APIRouter(dependencies=[Depends(require_internal_origin)])
logger = logging.getLogger("notifications_router")


class MarkReadRequest(BaseModel):
    notificationIds: Optional[List[str]] = None
    markAll: Optional[bool] = False


@notifications_router.get("/list")
async def list_user_notifications(
    request: Request,
    category: Optional[str] = None,
    unread_only: bool = False,
    limit: int = 50,
    offset: int = 0,
    user_id: str = Depends(get_current_user_id)
):
    """
    Returns user notifications for the authenticated user (bound to accountid).
    """
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            query = "SELECT id::text, category, type, title, message, data, is_read, created_at FROM user_notifications WHERE accountid = $1 AND (is_deleted IS FALSE OR is_deleted IS NULL)"
            params = [user_id]
            param_idx = 2

            if category and category != "all":
                query += f" AND category = ${param_idx}"
                params.append(category)
                param_idx += 1

            if unread_only:
                query += f" AND is_read = FALSE"

            query += f" ORDER BY created_at DESC LIMIT ${param_idx} OFFSET ${param_idx + 1};"
            params.extend([limit, offset])

            rows = await conn.fetch(query, *params)
            total_unread = await conn.fetchval("SELECT COUNT(*) FROM user_notifications WHERE accountid = $1 AND is_read = FALSE AND (is_deleted IS FALSE OR is_deleted IS NULL);", user_id) or 0

            return {
                "success": True,
                "unreadCount": total_unread,
                "notifications": [dict(r) for r in rows]
            }
    except Exception as e:
        logger.error(f"Error fetching notifications for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch notifications")


@notifications_router.get("/unread-count")
async def get_unread_notification_count(
    request: Request,
    user_id: str = Depends(get_current_user_id)
):
    """
    Returns total unread notifications count for the header bell badge.
    """
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            unread_count = await conn.fetchval("SELECT COUNT(*) FROM user_notifications WHERE accountid = $1 AND is_read = FALSE AND (is_deleted IS FALSE OR is_deleted IS NULL);", user_id) or 0
            return {"success": True, "unreadCount": unread_count}
    except Exception as e:
        logger.error(f"Error fetching unread notification count for {user_id}: {e}", exc_info=True)
        return {"success": False, "unreadCount": 0}


@notifications_router.put("/mark-read")
async def mark_notifications_read(
    request: Request,
    body: MarkReadRequest,
    user_id: str = Depends(get_current_user_id)
):
    """
    Marks selected notifications or all notifications as read.
    """
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            if body.markAll:
                await conn.execute("UPDATE user_notifications SET is_read = TRUE WHERE accountid = $1 AND (is_deleted IS FALSE OR is_deleted IS NULL);", user_id)
                msg = "All notifications marked as read."
            elif body.notificationIds:
                await conn.execute(
                    "UPDATE user_notifications SET is_read = TRUE WHERE accountid = $1 AND id::text = ANY($2::text[]);",
                    user_id, body.notificationIds
                )
                msg = f"{len(body.notificationIds)} notification(s) marked as read."
            else:
                msg = "No notifications updated."

            return {"success": True, "message": msg}
    except Exception as e:
        logger.error(f"Error marking notifications as read for {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to update notification read status")


@notifications_router.delete("/clear")
async def clear_user_notifications(
    request: Request,
    user_id: str = Depends(get_current_user_id)
):
    """
    Clears all notifications for the authenticated user (soft-delete to preserve deduplication).
    """
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            await conn.execute("UPDATE user_notifications SET is_deleted = TRUE WHERE accountid = $1;", user_id)
            return {"success": True, "message": "All notifications cleared successfully."}
    except Exception as e:
        logger.error(f"Error clearing notifications for {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to clear notifications")


@notifications_router.delete("/{notification_id}")
async def delete_single_notification(
    notification_id: str,
    request: Request,
    user_id: str = Depends(get_current_user_id)
):
    """
    Deletes a single notification (soft-delete to preserve deduplication) by ID.
    """
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            await conn.execute("UPDATE user_notifications SET is_deleted = TRUE WHERE accountid = $1 AND id::text = $2;", user_id, notification_id)
            return {"success": True, "message": "Notification deleted successfully."}
    except Exception as e:
        logger.error(f"Error deleting notification {notification_id} for {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to delete notification")


@notifications_router.post("/trigger-eval")
async def trigger_user_evaluation(
    request: Request,
    user_id: str = Depends(get_current_user_id)
):
    """
    Manually triggers an immediate telemetry evaluation pass for the calling user.
    """
    try:
        pool = request.app.state.db.pool
        await evaluate_user_telemetry_notifications(pool, user_id)
        return {"success": True, "message": "Telemetry notification evaluation completed."}
    except Exception as e:
        logger.error(f"Error running notification evaluation for {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to run notification evaluation")
