import logging
from typing import List, Dict, Any
from asyncpg import Connection
from app.db.models.ships import Ship, ShipFlight, ShipFlightSegment, ShipRepairMaterial
from repositories.ships_repo import repo_get_ships_by_user

logger = logging.getLogger("ships_service")

async def service_get_user_ships(conn: Connection, user_id: str) -> List[Ship]:
    """
    Service to retrieve all ships associated with a specific user.
    Follows SRP by separating database access from the router logic.
    """
    try:
        rows = await repo_get_ships_by_user(conn, user_id)
        
        ships = [Ship(**dict(row)) for row in rows]
        return ships
    except Exception as e:
        logger.error(f"Error in service_get_user_ships for user {user_id}: {e}")
        raise

async def service_get_ship_details(conn: Connection, user_id: str, ship_id: str) -> Dict[str, Any]:
    """
    Service to retrieve detailed information for a specific ship.
    """
    pass
