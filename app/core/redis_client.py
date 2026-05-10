import os

import redis.asyncio as redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# decode_responses=True ensures Redis returns standard Python strings instead of bytes
redis_client = redis.from_url("redis://localhost:6379/0", decode_responses=True)
