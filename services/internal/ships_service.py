import logging
from typing import List, Dict, Any
from asyncpg import Connection
from app.db.models.ships import Ship, ShipFlight, ShipFlightSegment, ShipRepairMaterial
from repositories.ships_repo import repo_get_all_accessible_ships

logger = logging.getLogger("ships_service")

async def service_get_user_ships(conn: Connection, user_id: str):
    """
    Service to retrieve all ships associated with a specific user.
    Follows SRP by separating database access from the router logic.
    Strips internal user IDs for non-owned shared ships for data privacy.
    """
    try:
        rows = await repo_get_all_accessible_ships(conn, user_id)
        
        result = []
        for row in rows:
            d = dict(row)
            if not d.get("is_owner"):
                d.pop("userid", None)
                d.pop("user_id", None)
            result.append(d)
        return result
    except Exception as e:
        logger.error(f"Error in service_get_user_ships for user {user_id}: {e}")
        raise

async def service_get_ship_details(conn: Connection, user_id: str, ship_id: str) -> Dict[str, Any]:
    """
    Service to retrieve detailed information for a specific ship.
    """
    pass
