import logging
from typing import Optional

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.core.limiter import get_auth_key, get_public_key, limiter
from helpers.shipments import fetch_public_shipment_by_local_id

logger = logging.getLogger(__name__)

shipments_router = APIRouter()

@shipments_router.get(
    "/{local_id}",
    summary="Public Shipment Lookup",
    description="Public lookup for a single shipment contract by local_id or contract_id. Requires company code (via {local_id}:{company} or ?company= parameter) matching either partner.",
)
@limiter.limit("60/minute", key_func=get_auth_key)
@limiter.limit("30/minute", key_func=get_public_key)
async def get_public_shipment(request: Request, local_id: str, company: Optional[str] = None):
    try:
        db = request.app.state.db
        shipment_data = await fetch_public_shipment_by_local_id(db, local_id, company_code=company)
        if not shipment_data:
            return JSONResponse(
                status_code=404,
                content={"success": False, "message": "Shipment contract not found or company code does not match."},
            )
        return JSONResponse(content={"success": True, "data": shipment_data})
    except Exception as e:
        logger.error(f"Failed public shipment lookup for {local_id}: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": "An unexpected error occurred."},
        )
