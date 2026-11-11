# routers/internal/ship_routes.py
import logging
from typing import Dict, Any, List
from fastapi import APIRouter, Depends, Request, HTTPException

from app.db.dependencies import get_db
from app.core.security import require_internal_origin
from auth import get_current_user_id
from repositories.ship_routes_repo import repo_get_user_ship_routes

ship_routes_router = APIRouter(dependencies=[Depends(require_internal_origin)])
logger = logging.getLogger("ship_routes_router")


@ship_routes_router.get("/")
async def get_user_routes(request: Request, user_id: str = Depends(get_current_user_id)):
    """
    Fetches all shipping routes configured by the authenticated user,
    including waypoints, steps, and assigned ships.
    Returns: {"routes": [...]}
    """
    db = get_db(request)
    try:
        async with db.pool.acquire() as conn:
            routes = await repo_get_user_ship_routes(conn, user_id)
        return {"routes": routes}
    except Exception as e:
        logger.error(f"Failed to fetch ship routes for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch routes: {str(e)}")


@ship_routes_router.get("/{route_id}")
async def get_single_route(route_id: str, request: Request, user_id: str = Depends(get_current_user_id)):
    """
    Fetches details of a single route.
    Returns: {...route_data...}
    """
    db = get_db(request)
    try:
        async with db.pool.acquire() as conn:
            routes = await repo_get_user_ship_routes(conn, user_id)
            for r in routes:
                if r["routeid"] == route_id or r["naturalid"] == route_id:
                    return r
        raise HTTPException(status_code=404, detail="Route not found.")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to fetch route {route_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


