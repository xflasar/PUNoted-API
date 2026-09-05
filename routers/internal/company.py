import logging
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from app.core.security import require_internal_origin
from endpoints.Public.services.company_service import fetch_public_company_profile

logger = logging.getLogger(__name__)

internal_company_router = APIRouter(dependencies=[Depends(require_internal_origin)])

@internal_company_router.get("/{company_code}")
async def get_internal_company_profile(company_code: str, request: Request):
    db = request.app.state.db
    if not company_code or len(company_code) > 4:
        raise HTTPException(status_code=400, detail="Invalid company code format.")

    company_data = await fetch_public_company_profile(db, company_code)
    if not company_data or company_data == "[]" or company_data == "{}":
        return Response(content="{}", media_type="application/json")
    
    return Response(content=company_data, media_type="application/json")
