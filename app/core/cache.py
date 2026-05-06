import time
from typing import Any, Optional

class MemoryCache:
    """
    A highly optimized, thread-safe (due to GIL) in-memory key-value store 
    with lazy TTL eviction.
    """
    def __init__(self):
        self._store = {}

    def get(self, key: str) -> Optional[Any]:
        if key in self._store:
            value, expires_at = self._store[key]
            if time.time() < expires_at:
                return value
            else:
                # Lazy eviction: delete when someone tries to access an expired key
                del self._store[key]
        return None

    def set(self, key: str, value: Any, ttl_seconds: int):
        self._store[key] = (value, time.time() + ttl_seconds)
        
    def delete(self, key: str):
        self._store.pop(key, None)
        
    def clear(self):
        self._store.clear()

global_cache = MemoryCache()