from fastapi import APIRouter, Depends, Request
import logging

from app.db.dependencies import get_db
from app.core.security import require_internal_origin
from auth import get_current_user_id
from app.db.models.ships import Ship
from services.internal.ships_service import service_get_user_ships

ships_router = APIRouter(dependencies=[Depends(require_internal_origin)])
logger = logging.getLogger("ships_router")

@ships_router.get("/")
async def get_ships(request: Request, user_id: str = Depends(get_current_user_id)):
    db = get_db(request)

    async with db.pool.acquire() as conn:
      ships = await service_get_user_ships(conn, user_id)

    return {"ships": ships}

@ships_router.get("/blueprints")
async def get_blueprints(request: Request, user_id: str = Depends(get_current_user_id)):
    db = get_db(request)
    try:
        async with db.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, natural_id, name, type, bill_of_material
                FROM ship_blueprints
                """
            )
            blueprints = [
                {
                    "id": r["id"],
                    "natural_id": r["natural_id"],
                    "name": r["name"],
                    "type": r["type"],
                    "bill_of_material": r["bill_of_material"] if isinstance(r["bill_of_material"], (dict, list)) else (r["bill_of_material"] or {})
                }
                for r in rows
            ]
            return {"blueprints": blueprints}
    except Exception as e:
        logger.error(f"Error fetching ship blueprints: {e}")
        return {"blueprints": []}
