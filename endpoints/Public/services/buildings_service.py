import logging
from app.core.redis_client import redis_client
from endpoints.Public.repositories.buildings_repo import get_buildings_json

logger = logging.getLogger(__name__)

async def fetch_building_data(db, ticker: str = None) -> str:
    """
    Routes the request, manages Redis caching, and returns JSON string.
    """
    # 1. Parse tickers into a clean, uppercase, deduplicated list
    tickers_list = []
    if ticker and ticker.strip():
        tickers_list = list(set(t.strip().upper() for t in ticker.split(",") if t.strip()))

    # 2. Determine Cache Key & TTL based purely on the ticker list
    if tickers_list:
        sorted_tickers = ",".join(sorted(tickers_list))
        cache_key = f"buildings:full:{sorted_tickers}"
        ttl = 3600  # 1 hour for specific building(s)
    else:
        # NO TICKER = FETCH ALL
        cache_key = "buildings:full:all"
        ttl = 43200  # 12 hours for the massive all-buildings payload

    try:
        # 3. Check Redis
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            return cached_data.decode("utf-8") if isinstance(cached_data, bytes) else cached_data

        # 4. Cache Miss - DB Fetch
        json_string = await get_buildings_json(db, tickers=tickers_list)

        # 5. Store in Redis
        if json_string and json_string != "[]":
            await redis_client.set(cache_key, json_string, ex=ttl)
        
        return json_string

    except Exception as e:
        logger.error(f"Failed to generate Building data: {e}", exc_info=True)
        raise