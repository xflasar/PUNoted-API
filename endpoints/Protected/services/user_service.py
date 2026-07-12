import logging
from endpoints.Protected.repositories.user_repo import fetch_company_data

logger = logging.getLogger(__name__)

async def get_company_data_service(db, usernames: list, codes: list = None, names: list = None) -> list:
    """
    Service layer for fetching protected user company data.
    """
    try:
        return await fetch_company_data(db, usernames=usernames, codes=codes, names=names)
    except Exception as e:
        logger.error(f"Failed to fetch user company data in service: {e}", exc_info=True)
        raise
