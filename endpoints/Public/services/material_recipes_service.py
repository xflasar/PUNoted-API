import logging
from app.core.redis_client import redis_client
from endpoints.Public.repositories.material_recipes_repo import (
    fetch_recipes_all,
    fetch_recipes_detailed
)

logger = logging.getLogger(__name__)

async def generate_recipes_json(db, ticker: str = None, tickers: str = None) -> str:
    target_tickers = []
    if ticker:
        target_tickers.append(ticker.upper())
    if tickers:
        target_tickers.extend([t.strip().upper() for t in tickers.split(",") if t.strip()])
    
    if target_tickers:
        target_tickers.sort() 
        cache_key = f"recipes:detailed:{','.join(target_tickers)}"
    else:
        target_tickers = None 
        cache_key = "recipes:minimal:all"

    try:
        """ cached_data = await redis_client.get(cache_key)
        if cached_data:
            return cached_data """

        if target_tickers:
            json_string = await fetch_recipes_detailed(db, target_tickers)
        else:
            json_string = await fetch_recipes_all(db)

        """ await redis_client.set(cache_key, json_string, ex=86400) """
        
        return json_string

    except Exception as e:
        logger.error(f"Failed to generate Recipes data: {e}", exc_info=True)
        raise