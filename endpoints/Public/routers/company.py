from fastapi import APIRouter, Depends, Request, Response, HTTPException
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

    json_string = await fetch_public_company_profile(db, company_code)

    if not json_string:
        raise HTTPException(status_code=404, detail="Company not found.")

    return Response(
        content=json_string, 
        media_type="application/json",
        headers={
            "Cache-Control": "public, max-age=3600"
        }
    )