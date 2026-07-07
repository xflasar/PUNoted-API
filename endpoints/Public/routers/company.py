from fastapi import APIRouter, HTTPException, Request, Response

from app.core.limiter import get_auth_key, limiter
from endpoints.Public.services.company_service import fetch_public_company_profile
from endpoints.Public.schemas.company import PublicCompanyProfile

company_router = APIRouter()

@company_router.get(
    "/{company_code}",
    summary="Public Company Profile",
    description="Fetch public profile data for a specific company code.",
    responses={200: {"model": PublicCompanyProfile}}
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

    if not company_data or company_data == "[]" or company_data == "{}":
        return Response(content="{}", media_type="application/json")
    
    return Response(content=company_data, media_type="application/json")

