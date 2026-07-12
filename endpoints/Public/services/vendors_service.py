import logging
from endpoints.Public.repositories.vendors_repo import fetch_public_vendors

logger = logging.getLogger(__name__)

async def get_vendors_data(db, search: str = None, corp: str = None, operator: str = None) -> list:
    """
    Service layer for public vendors data directory.
    """
    try:
        return await fetch_public_vendors(db, search=search, corp=corp, operator=operator)
    except Exception as e:
        logger.error(f"Failed to fetch vendors data in service: {e}", exc_info=True)
        raise
