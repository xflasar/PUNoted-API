from fastapi import APIRouter, HTTPException, Request, Response

from app.core.limiter import get_auth_key, limiter
from endpoints.Public.services.company_service import fetch_public_company_profile

company_router = APIRouter()

@company_router.get(
    "/{company_code}",
    summary="Public Company Profile",
    description="Fetch public profile data for a specific company code.",
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_public_company(
    company_code: str,
    request: Request
):
    db = request.app.state.db

    if not company_code or len(company_code) > 4:
        raise HTTPException(status_code=400, detail="Invalid company code format.")

    company_data = await fetch_public_company_profile(db, company_code)

    if not company_data:
        return []
    
    return company_data

