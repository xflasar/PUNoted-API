import logging
from endpoints.Protected.repositories.accounting_repo import fetch_user_accounts

logger = logging.getLogger(__name__)

async def get_accounting_data(db, target_usernames: list, currency: str = None) -> list:
    """
    Business logic and orchestration layer for currency accounts data.
    """
    try:
        # The database query aggregates everything into a JSONB list, so we get a Python list directly
        return await fetch_user_accounts(db, target_usernames, currency)
    except Exception as e:
        logger.error(f"Failed to fetch accounting data in service: {e}", exc_info=True)
        raise
