import json
import logging
from app.core.redis_client import redis_client
from endpoints.Public.repositories.planets_repo import (
    fetch_minimal_planets,
    fetch_full_planets_all,
    fetch_full_planet_single
)

logger = logging.getLogger(__name__)

async def get_planet_data(db, ticker: str = None, full: bool = False) -> str:
    """
    Routes the request, manages Redis caching, and returns JSON string.
    """
    # 1. Determine Cache Key & TTL
    if ticker:
        cache_key = f"planets:full:{ticker.upper()}"
        ttl = 3600  # 1 hour for specific planet
    elif full:
        cache_key = "planets:full:all"
        ttl = 43000  # 2 hours for the massive 4k payload
    else:
        cache_key = "planets:minimal:all"
        ttl = 3600 # 24 hours for minimal payload

    try:
        # 1. Check Redis
        cached_data = await redis_client.get(cache_key)
        if cached_data:
            return cached_data

        # 2. Cache Miss - DB Fetch
        if not ticker and not full:
            records = await fetch_minimal_planets(db)
            result = [{"PlanetNaturalId": r["PlanetNaturalId"], "PlanetName": r["PlanetName"]} for r in records]
            json_string = json.dumps(result)
            
        elif ticker:
            # Returns a pre-formatted JSON string of the object directly from DB
            json_string = await fetch_full_planet_single(db, ticker.upper())
            
        else:
            # Returns a pre-formatted massive JSON string array directly from DB
            json_string = await fetch_full_planets_all(db)

        # 4. Store in Redis
        await redis_client.set(cache_key, json_string, ex=ttl)
        
        return json_string

    except Exception as e:
        logger.error(f"Failed to generate Planet data: {e}", exc_info=True)
        raise