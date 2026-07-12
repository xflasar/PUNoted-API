import logging
from endpoints.Protected.repositories.flights_repo import search_flights

logger = logging.getLogger(__name__)

async def get_flights_data(db, usernames_list, ship_identifier=None, is_current=None, limit=50) -> list:
    """
    Service layer for protected flights data retrieval.
    """
    try:
        return await search_flights(db, usernames_list, ship_identifier, is_current, limit)
    except Exception as e:
        logger.error(f"Failed to fetch flights data in service: {e}", exc_info=True)
        raise
