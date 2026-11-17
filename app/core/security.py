from typing import Optional

from fastapi import Header, HTTPException

# List of allowed origins for internal endpoints
INTERNAL_ORIGINS = [
    "https://punoted.ddns.net",
    "http://punoted.ddns.net:5174",
    "http://localhost:5174",
    "http://127.0.0.1:5174",
    "https://punoted.net"
]

async def require_internal_origin(origin: Optional[str] = Header(None)):
    # 1. Origin must be present. Server-side scripts or direct cURL calls without Origin are rejected.
    if not origin:
        raise HTTPException(
            status_code=403,
            detail="Access denied. Missing Origin header.",
        )

    # 2. Origin must be strictly in the internal whitelist.
    if origin not in INTERNAL_ORIGINS:
        raise HTTPException(
            status_code=403,
            detail=f"Access denied. Origin '{origin}' is not whitelisted.",
        )

    return True
