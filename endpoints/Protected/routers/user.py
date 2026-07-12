import logging
from typing import Any, Optional

import orjson
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse as DefaultJSONResponse

from app.core.limiter import get_auth_key, limiter
from auth import RequireAuth
from endpoints.Protected.services.user_service import get_company_data_service
from endpoints.Protected.schemas.user import UserCompany, Company
from typing import List

user_router = APIRouter()
logger = logging.getLogger("user_router")

class ORJSONResponse(DefaultJSONResponse):
    media_type = "application/json"
    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)

# ==============================================================================
# 1. COMPANY DATA - LIST
# ==============================================================================
@user_router.get(
    "/companydata",
    summary="Get Company Data",
    description="Search by Usernames, Company Codes, or Names. Scopes automatically to Group or Self.",
    responses={200: {"model": List[UserCompany]}}
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_company_data(
    request: Request,
    usernames: Optional[str] = Query(None, description="Comma-separated usernames"),
    codes: Optional[str] = Query(None, description="Comma-separated Company Codes"),
    names: Optional[str] = Query(None, description="Comma-separated Company Names"),
    user_id: str = Depends(RequireAuth(["profile:read"], is_single_user_endpoint=False)),
):
    db = request.app.state.db
    valid_targets = getattr(request.state, "valid_target_users", [])
    if not valid_targets:
        return []

    target_codes = [c.strip() for c in codes.split(",") if c.strip()] if codes else None
    target_names = [n.strip() for n in names.split(",") if n.strip()] if names else None

    json_data = await get_company_data_service(
        db,
        usernames=valid_targets,
        codes=target_codes,
        names=target_names
    )

    if not json_data:
        return []

    if json_data and isinstance(json_data, list) and "Company" in json_data[0] and isinstance(json_data[0], dict) and len(json_data) == 1 and json_data[0].get("Username") is None: 
        return json_data[0]["Company"]

    if isinstance(json_data, dict) and 'Company' in json_data:
        return json_data['Company']

    return json_data


# ==============================================================================
# 2. COMPANY DATA - SINGLE USER
# ==============================================================================
@user_router.get(
    "/companydata/user",
    summary="Get Single Company Data",
    description="Returns a single company object.",
    response_class=ORJSONResponse,
    responses={200: {"model": Company}}
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_company_data_user(
    request: Request,
    username: Optional[str] = Query(None, description="Specific username"),
    code: Optional[str] = Query(None, description="Specific Company Code"),
    name: Optional[str] = Query(None, description="Specific Company Name"),
    user_id: str = Depends(RequireAuth(["profile:read"], is_single_user_endpoint=True)),
):
    db = request.app.state.db
    valid_targets = getattr(request.state, "valid_target_users", [])
    if not valid_targets:
        raise HTTPException(status_code=404, detail="Company not found or access denied")

    target_codes = [code.strip()] if code else None
    target_names = [name.strip()] if name else None

    json_data = await get_company_data_service(
        db,
        usernames=valid_targets,
        codes=target_codes,
        names=target_names
    )

    if json_data and "Company" in json_data[0]:
        return json_data[0]["Company"]
    return {}
