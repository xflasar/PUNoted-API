from fastapi import APIRouter, Depends, Request, Response

from app.core.security import require_internal_origin
from auth import get_current_user_id
from services.internal.finances_services import fetch_financial_overview, fetch_transaction_details

finances_router = APIRouter(dependencies=[Depends(require_internal_origin)])

@finances_router.get(
    "/overview",
    summary="Financial Dashboard Overview",
    description="Retrieve liquid balances, locked capital, and 30-day cash flow analysis.",
)
async def get_financial_overview(
    request: Request,
    user_id: str = Depends(get_current_user_id)
):
    db = request.app.state.db

    json_string = await fetch_financial_overview(db, user_id)

    return Response(
        content=json_string,
        media_type="application/json",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"
        }
    )

@finances_router.get(
    "/transaction/{tx_id}",
    summary="Financial Transaction Context",
    description="Fetch deep context for a specific transaction ID.",
)
async def get_transaction_details(
    tx_id: str,
    request: Request,
    user_id: str = Depends(get_current_user_id)
):
    db = request.app.state.db

    json_string = await fetch_transaction_details(db, user_id, tx_id)

    if not json_string or json_string == "{}":
        return Response(status_code=404, content="{\"error\": \"Transaction not found\"}")

    return Response(
        content=json_string,
        media_type="application/json",
        headers={
            "Cache-Control": "public, max-age=86400"
        }
    )
