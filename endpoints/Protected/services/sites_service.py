import logging
from endpoints.Protected.repositories.sites_repo import fetch_sites

logger = logging.getLogger(__name__)

async def get_sites_data(
    db,
    usernames_list: list,
    location: str = None,
    include_buildings: bool = False,
    include_reclaimable: bool = False,
    include_repair: bool = False,
) -> list:
    """
    Service layer for protected sites data retrieval.
    """
    try:
        return await fetch_sites(
            db,
            usernames_list,
            location=location,
            include_buildings=include_buildings,
            include_reclaimable=include_reclaimable,
            include_repair=include_repair,
        )
    except Exception as e:
        logger.error(f"Failed to fetch sites data in service: {e}", exc_info=True)
        raise
