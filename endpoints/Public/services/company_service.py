import logging

from app.core.redis_client import redis_client
from endpoints.Public.repositories.company_repo import get_public_company_json

logger = logging.getLogger(__name__)

async def fetch_public_company_profile(db, company_code: str) -> str:
    # Standardize cache key format
    normalized_code = company_code.strip().upper()
    cache_key = f"public:company_profile:{normalized_code}"

    # Public profiles change rarely. A 1-hour TTL (3600s) provides massive database offloading.
    ttl = 3600

    try:
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            return cached_data.decode("utf-8") if isinstance(cached_data, bytes) else cached_data

        json_string = await get_public_company_json(db, normalized_code)

        if json_string:
            await redis_client.set(cache_key, json_string, ex=ttl)

        return json_string

    except Exception as e:
        logger.error(f"Failed to generate public company profile for {normalized_code}: {e}", exc_info=True)
        raise
