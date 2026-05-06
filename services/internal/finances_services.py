import logging
from app.core.redis_client import redis_client
from repositories.finances_repo import get_financial_overview_json, get_transaction_details_json

logger = logging.getLogger(__name__)

async def fetch_financial_overview(db, user_id: str) -> str:
    cache_key = f"finances:overview:{user_id}"
    ttl = 60  # 1 minute cache for volatile financial data

    try:
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            return cached_data.decode("utf-8") if isinstance(cached_data, bytes) else cached_data

        json_string = await get_financial_overview_json(db, user_id)

        if json_string and json_string != "{}":
            await redis_client.set(cache_key, json_string, ex=ttl)
        
        return json_string

    except Exception as e:
        logger.error(f"Failed to generate financial overview: {e}", exc_info=True)
        raise

async def fetch_transaction_details(db, user_id: str, tx_id: str) -> str:
    cache_key = f"finances:tx_detail:{tx_id}:{user_id}"
    ttl = 86400  # 24 hours. Historical tx details never change once recorded.

    try:
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            return cached_data.decode("utf-8") if isinstance(cached_data, bytes) else cached_data

        json_string = await get_transaction_details_json(db, user_id, tx_id)

        if json_string and json_string != "{}":
            await redis_client.set(cache_key, json_string, ex=ttl)
        
        return json_string

    except Exception as e:
        logger.error(f"Failed to generate transaction detail: {e}", exc_info=True)
        raise