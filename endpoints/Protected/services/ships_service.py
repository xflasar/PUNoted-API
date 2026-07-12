import logging
from endpoints.Protected.repositories.ships_repo import search_ships

logger = logging.getLogger(__name__)

async def get_ships_data(
    db,
    usernames_list: list,
    shipname: str = None,
    inflight: bool = None,
    location: str = None,
    ship_type: str = None,
) -> list:
    """
    Service layer for protected ships data retrieval.
    """
    try:
        return await search_ships(
            db,
            usernames_list,
            shipname=shipname,
            inflight=inflight,
            location=location,
            ship_type=ship_type,
        )
    except Exception as e:
        logger.error(f"Failed to fetch ships data in service: {e}", exc_info=True)
        raise
