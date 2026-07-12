# routers/workforce.py
from typing import Any, Optional

import orjson
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse as DefaultJSONResponse
from fastapi.responses import StreamingResponse

from app.core.limiter import get_auth_key, limiter
from auth import RequireAuth
from endpoints.Protected.services.workforce_service import get_workforce_data, generate_workforce_csv
from endpoints.Protected.schemas.workforce import UserWorkforce, Workforce
from typing import List

workforce_router = APIRouter()

class ORJSONResponse(DefaultJSONResponse):
    media_type = "application/json"
    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)

# ==============================================================================
# 1. LIST Endpoint (Multi-User) -> [{ "Username": "x", "Workforce": [...] }]
# ==============================================================================
@workforce_router.get(
    "/",
    summary="Get Workforce Data",
    description="Returns workforce list. If no usernames provided, returns your own data.",
    responses={200: {"model": List[UserWorkforce]}}
)
@limiter.limit("30/minute", key_func=get_auth_key)
async def get_workforce_list(
    request: Request,
    usernames: Optional[str] = Query(None, description="Comma-separated list of usernames"),
    location: Optional[str] = Query(None, description="Filter by Planet Name or Natural ID"),
    user_id: str = Depends(RequireAuth(["workforce:read"]))
):
    db = request.app.state.db
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        return []

    workforce_data = await get_workforce_data(db, valid_targets, location)
    return workforce_data



# ==============================================================================
# 2. SINGLE USER Endpoint (Unwrapped) -> [...] (Just the workforce list)
# ==============================================================================
@workforce_router.get(
    "/user",
    summary="Get Single User Workforce",
    description="Returns a flat list of workforce data for a specific user.",
    response_class=ORJSONResponse,
    responses={200: {"model": List[Workforce]}}
)
@limiter.limit("30/minute", key_func=get_auth_key)
async def get_workforce_data_user(
    request: Request,
    username: Optional[str] = Query(None, description="Specific username"),
    location: Optional[str] = Query(None, description="Filter by Planet Name or Natural ID"),
    user_id: str = Depends(RequireAuth(["workforce:read"], is_single_user_endpoint=True))
):
    db = request.app.state.db
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        raise HTTPException(status_code=404, detail="User not found or access denied")

    # 1. Fetch standard multi-user structure
    workforce_data = await get_workforce_data(db, valid_targets, location)

    if not workforce_data:
        return []

    if workforce_data and "Workforce" in workforce_data[0]:
        return workforce_data[0]["Workforce"]
    return []


# ==============================================================================
# 3. CSV Endpoint (Multi-User)
# ==============================================================================
@workforce_router.get(
    "/csv",
    summary="Download Workforce CSV",
    description="Returns flat CSV."
)
@limiter.limit("10/minute", key_func=get_auth_key)
async def get_workforce_csv(
    request: Request,
    usernames: Optional[str] = Query(None, description="Comma-separated list of usernames"),
    location: Optional[str] = Query(None, description="Filter by Planet Name or Natural ID"),
    user_id: str = Depends(RequireAuth(["workforce:read"]))
):
    db = request.app.state.db
    valid_targets = getattr(request.state, "valid_target_users", [])

    if not valid_targets:
        return Response(content="No permission or users found", media_type="text/plain")

    csv_buffer = await generate_workforce_csv(db, valid_targets, location)

    filename = f"workforce_{location}.csv" if location else "workforce.csv"

    return StreamingResponse(
        iter([csv_buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
