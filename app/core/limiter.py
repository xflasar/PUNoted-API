import logging
import os

from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.requests import Request
from starlette.responses import JSONResponse

logging.getLogger("slowapi").setLevel(logging.CRITICAL)

# --- HELPER: Extract Token ---
def _get_token(request: Request):
    """Internal helper to find token in Header or Query."""
    # 1. Check Standard Authorization Header
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        return auth_header.split(" ")[1]

    # 2. Check X-Data-Token Header (RequireAuth supports this)
    x_header = request.headers.get("X-Data-Token")
    if x_header:
        return x_header

    # 3. Check Query Param
    query_token = request.query_params.get("token")
    if query_token:
        return query_token

    return None


# --- 1. AUTH KEY (Returns Token OR None) ---
def get_auth_key(request: Request):
    """
    If user is Authenticated, return their Token.
    If Public, return None (This causes the Auth Limit to be SKIPPED).
    """
    token = _get_token(request)
    if token:
        return token
    return None


# --- 2. PUBLIC KEY (Returns IP OR None) ---
def get_public_key(request: Request):
    """
    If user is Public, return their IP.
    If Authenticated, return None (This causes the Public Limit to be SKIPPED).
    """
    if _get_token(request):
        return None  # User is logged in, ignore public limit
    return get_remote_address(request)


# --- INITIALIZE ---
STORAGE_URI = os.getenv("REDIS_URL") #"memory://", "REDIS_URL" -> if redis is running set "REDIS_URL" otherwise use "memory://"

limiter = Limiter(
    key_func=get_remote_address,
    storage_uri=STORAGE_URI,
    strategy="fixed-window",
    default_limits=["120/minute"]
)


# --- HELPER: Parse Retry-After Seconds ---
def _get_retry_after(error_detail: str) -> str:
    """
    Parses the time unit from the error message (e.g. '30 per 1 minute')
    and returns the seconds as a string.
    """
    error_detail = error_detail.lower()
    if "second" in error_detail:
        return "1"
    if "minute" in error_detail:
        return "60"
    if "hour" in error_detail:
        return "3600"
    if "day" in error_detail:
        return "86400"
    return "60" # Default fallback


# --- ERROR HANDLER ---
def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    """
    Custom handler to return JSON instead of plain text, 
    AND inject the Retry-After header.
    """
    error_detail = getattr(exc, "detail", str(exc))

    # Calculate wait time based on the limit window
    retry_seconds = _get_retry_after(str(error_detail))

    response = JSONResponse(
        status_code=429,
        content={
            "success": False,
            "message": f"Rate limit exceeded: {error_detail}",
            "retry_after": retry_seconds
        }
    )

    response.headers["Retry-After"] = retry_seconds

    return response
