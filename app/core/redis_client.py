import os
import redis.asyncio as redis

ENVIRONMENT = os.getenv("ENVIRONMENT", "development").lower()
DEFAULT_REDIS_URL = (
    "redis://redis_cache:6379/0" if ENVIRONMENT == "production" else "redis://localhost:6379/0"
)
REDIS_URL = os.getenv("REDIS_URL", DEFAULT_REDIS_URL)

# decode_responses=True ensures Redis returns standard Python strings instead of bytes
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

