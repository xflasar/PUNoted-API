import csv
import io
from datetime import datetime, timedelta
from typing import Optional

import orjson
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from fastapi.responses import JSONResponse as DefaultJSONResponse

from app.core.limiter import get_auth_key, limiter
from auth import RequireAuth

from ..services.cxuser import fetch_orders_as_json, stream_orders_csv
from endpoints.Protected.schemas.cxuser import UserCXOrders, CXOrder

cxuser_router = APIRouter()

async def common_params(
    start_date: Optional[datetime] = Query(None, description="ISO start date"),
    end_date: Optional[datetime] = Query(None, description="ISO end date"),
    all_time: bool = Query(False, description="If true, ignores default 30-day window"),
    limit: Optional[int] = Query(None, gt=0, description="Max records. Omit to fetch all."),

    status: Optional[str] = Query(None, description="Filter by status. [FILLED, PARTIALLY_FILLED, CANCELED]"),
    type: Optional[str] = Query(None, description="Filter by type. [BUYING, SELLING]"),
    ticker: Optional[str] = Query(None, description="Filter by ticker. [H2O, RAT...]"),
):
    if not end_date:
        end_date = datetime.utcnow()

    if all_time:
        start_date = None
    elif not start_date:
        start_date = datetime.utcnow() - timedelta(days=30)

    return {
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit,
        "status": status,
        "order_type": type,
        "ticker": ticker
    }


# ==============================================================================
# 1. LIST Endpoint (Multi-User JSON)
# ==============================================================================
@cxuser_router.get(
    "/orders",
    responses={200: {"model": list[UserCXOrders]}}
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_orders_json(
    request: Request,
    usernames: Optional[str] = Query(None, description="Comma-separated list of usernames"),
    params: dict = Depends(common_params),
    user_id: str = Depends(RequireAuth(["cxdata:read"])),
):
    try:
        db = request.app.state.db
        valid_targets = getattr(request.state, "valid_target_users", [])

        if not valid_targets:
            return Response(content='[]', media_type="application/json")

        orders_json_str = await fetch_orders_as_json(db, valid_targets, **params)
        if not orders_json_str:
            return []

        if isinstance(orders_json_str, dict) and 'Orders' in orders_json_str:
            return orders_json_str['Orders']

        return Response(content=orders_json_str, media_type="application/json")

    except Exception as e:
        print(f"External API Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch orders")


# ==============================================================================
# 2. SINGLE USER Endpoint (JSON)
# ==============================================================================
@cxuser_router.get(
    "/orders/user",
    responses={200: {"model": list[CXOrder]}}
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_order_user(
    request: Request,
    username: Optional[str] = Query(None, description="Specific username to fetch"),
    params: dict = Depends(common_params),
    user_id: str = Depends(RequireAuth(["cxdata:read"])),
):
    try:
        db = request.app.state.db
        valid_targets = getattr(request.state, "valid_target_users", [])

        if not valid_targets:
            raise HTTPException(status_code=404, detail="User not found or access denied")

        orders_json_str = await fetch_orders_as_json(db, valid_targets, **params)

        if not orders_json_str:
            return []

        if isinstance(orders_json_str, str):
            try:
                data_list = orjson.loads(orders_json_str)
                if data_list and isinstance(data_list, list) and "Orders" in data_list[0]:
                    return data_list[0]["Orders"]
            except Exception:
                return []
                
        if isinstance(orders_json_str, dict) and 'Orders' in orders_json_str:
            return orders_json_str['Orders']

        return []

    except HTTPException as he:
        raise he
    except Exception as e:
        print(f"External API Error (Single): {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch user orders")


# ==============================================================================
# 3. CSV Endpoint (Multi/Single Stream)
# ==============================================================================
@cxuser_router.get("/orders/csv")
@limiter.limit("30/minute", key_func=get_auth_key)
async def get_orders_csv(
    request: Request,
    username: Optional[str] = Query(None, description="Specific username (Singular)"),
    usernames: Optional[str] = Query(None, description="Comma-separated list of usernames (Plural)"),
    params: dict = Depends(common_params),
    user_id: str = Depends(RequireAuth(["cxdata:read"])),
):
    try:
        db = request.app.state.db

        # Valid targets are already filtered by Auth based on ?username OR ?usernames
        valid_targets = getattr(request.state, "valid_target_users", [])

        if not valid_targets:
            return Response(content="No permission or users found", media_type="text/plain")

        async def iter_csv():
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(
                [
                    "Username",
                    "Order ID",
                    "Date",
                    "Ticker",
                    "Type",
                    "Status",
                    "Price",
                    "Currency",
                    "Filled Amount",
                    "Total Value",
                ]
            )

            batch_size = 1000
            count = 0

            async for row_data in stream_orders_csv(db, valid_targets, **params):
                writer.writerow(row_data)
                count += 1
                if count >= batch_size:
                    yield output.getvalue()
                    output.seek(0)
                    output.truncate(0)
                    count = 0

            if count > 0:
                yield output.getvalue()

        filename = "orders_export.csv"
        return StreamingResponse(
            iter_csv(),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except Exception as e:
        print(f"External CSV Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate CSV")
