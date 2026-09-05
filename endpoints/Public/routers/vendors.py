from typing import Optional, List
from fastapi import APIRouter, Depends, Query, Request, Response
from fastapi.responses import JSONResponse

from app.core.limiter import get_auth_key, get_public_key, limiter
from auth import OptionalAuth
from endpoints.Public.services.vendors_service import get_vendors_data, generate_vendors_csv
from endpoints.Public.schemas.vendors import VendorEntry

vendors_router = APIRouter()

@vendors_router.get(
    "/",
    summary="Public Vendor Directory",
    description="Search for active vendors in JSON or CSV format. Public access allowed.",
    responses={200: {"model": List[VendorEntry]}}
)
@limiter.limit("30/minute", key_func=get_auth_key)
@limiter.limit("10/minute", key_func=get_public_key)
async def get_vendors(
    request: Request,
    search: Optional[str] = Query(None, description="Search by Company Name or Code"),
    corp: Optional[str] = Query(None, description="Filter by Corporation Name"),
    operator: Optional[str] = Query(None, description="Filter by In-Game Username"),
    format: Optional[str] = Query("json", description="Response format: 'json' or 'csv'"),
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db

    if format and format.lower() == "csv":
        csv_string = await generate_vendors_csv(db, search=search, corp=corp, operator=operator)
        return Response(
            content=csv_string,
            media_type="text/csv",
            headers={
                "Content-Disposition": "inline; filename=vendor_stores.csv",
                "Cache-Control": "public, max-age=300"
            }
        )

    vendors_data = await get_vendors_data(db, search=search, corp=corp, operator=operator)
    return JSONResponse(content={"success": True, "vendors": vendors_data})

@vendors_router.get(
    "/csv",
    summary="Public Vendor Directory CSV",
    description="Get Vendor directory in CSV format. Public access allowed.",
    response_class=Response
)
@limiter.limit("30/minute", key_func=get_auth_key)
@limiter.limit("10/minute", key_func=get_public_key)
async def get_vendors_csv(
    request: Request,
    search: Optional[str] = Query(None, description="Search by Company Name or Code"),
    corp: Optional[str] = Query(None, description="Filter by Corporation Name"),
    operator: Optional[str] = Query(None, description="Filter by In-Game Username"),
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db
    csv_string = await generate_vendors_csv(db, search=search, corp=corp, operator=operator)
    return Response(
        content=csv_string,
        media_type="text/csv",
        headers={
            "Content-Disposition": "inline; filename=vendor_stores.csv",
            "Cache-Control": "public, max-age=300"
        }
    )

