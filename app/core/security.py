from typing import Optional

from fastapi import Header, HTTPException

# List of allowed origins for internal endpoints needs to be revised
INTERNAL_ORIGINS = [
    "https://punoted.ddns.net",
    "http://punoted.ddns.net:5174",
    "http://localhost:5174",
    "https://punoted.net"
]

async def require_internal_origin(origin: Optional[str] = Header(None)):
    # 1. If Origin is missing, it's usually a direct browser hit or a server-side call.
    # We allow it here because CORS protection is handled separately by CORSMiddleware.
    if origin is None:
        return True

    # 2. If an Origin IS present, it must be in the whitelist.
    # This prevents other malicious websites from making requests on behalf of the user.
    if origin not in INTERNAL_ORIGINS:
        raise HTTPException(
            status_code=403,
            detail=f"Access denied. Origin '{origin}' is not whitelisted.",
        )

    return True
