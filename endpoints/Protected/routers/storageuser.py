import csv
import io
from typing import Any, Optional

import orjson
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse as DefaultJSONResponse
from fastapi.responses import StreamingResponse

from app.core.limiter import get_auth_key, limiter
from auth import RequireAuth
from endpoints.Protected.services.storageuser_service import fetch_storages_as_json, stream_storages_csv
from endpoints.Protected.schemas.storageuser import UserStorages, Storage
from typing import List

storage_router = APIRouter()

class ORJSONResponse(DefaultJSONResponse):
    media_type = "application/json"
    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)

# ==============================================================================
# 1. LIST Endpoint (Multi-User) -> [{ "Username": "x", "Storages": [...] }]
# ==============================================================================
@storage_router.get(
    "/",
    summary="Get Storages",
    description="Get all storages and items nested by location. Returns list grouped by user.",
    responses={200: {"model": List[UserStorages]}}
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_storages_json(
    request: Request,
    usernames: Optional[str] = Query(None, description="Comma-separated list of usernames"),
    location: Optional[str] = Query(None, description="Filter by location name (e.g. 'Hortus')"),
    user_id: str = Depends(RequireAuth(["storage:read"], is_single_user_endpoint=False)),
):
    try:
        db = request.app.state.db
        valid_targets = getattr(request.state, "valid_target_users", [])

        if not valid_targets:
            return []

        db_data = await fetch_storages_as_json(db, valid_targets, location)
        return db_data if db_data else []

    except Exception as e:
        print(f"Storage API Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch storages")


# ==============================================================================
# 2. SINGLE USER Endpoint (Unwrapped) -> [...] (Just the Storages list)
# ==============================================================================
@storage_router.get(
    "/user",
    summary="Get Single User Storages",
    description="Returns a flat list of storages for a specific user.",
    response_class=ORJSONResponse,
    responses={200: {"model": List[Storage]}}
)
@limiter.limit("60/minute", key_func=get_auth_key)
async def get_storages_user(
    request: Request,
    username: Optional[str] = Query(None, description="Specific username"),
    location: Optional[str] = Query(None, description="Filter by location name"),
    user_id: str = Depends(RequireAuth(["storage:read"], is_single_user_endpoint=True)),
):
    try:
        db = request.app.state.db
        valid_targets = getattr(request.state, "valid_target_users", [])

        if not valid_targets:
            raise HTTPException(status_code=404, detail="User not found or access denied")

        db_data = await fetch_storages_as_json(db, valid_targets, location)

        if isinstance(db_data, list) and db_data and "Storages" in db_data[0]:
            return db_data[0]["Storages"]
            
        if isinstance(db_data, str):
            try:
                data_list = orjson.loads(db_data)
                if data_list and isinstance(data_list, list) and "Storages" in data_list[0]:
                    return data_list[0]["Storages"]
            except Exception:
                return []

        return []

    except HTTPException as he:
        raise he
    except Exception as e:
        print(f"Storage Single API Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch user storages")


# ==============================================================================
# 3. CSV Endpoint (Multi-User Stream)
# ==============================================================================
@storage_router.get(
    "/csv",
    summary="Download Storage CSV",
    description="Download flattened storage inventory as CSV.",
)
@limiter.limit("10/minute", key_func=get_auth_key) # Stricter limit for heavy CSVs
async def get_storages_csv(
    request: Request,
    usernames: Optional[str] = Query(None, description="Comma-separated list of usernames"),
    location: Optional[str] = Query(None, description="Filter by location"),
    user_id: str = Depends(RequireAuth(["storage:read"])),
):
    try:
        db = request.app.state.db
        valid_targets = getattr(request.state, "valid_target_users", [])

        if not valid_targets:
            return Response(content="No permission or users found", media_type="text/plain")

        async def iter_csv():
            output = io.StringIO()
            writer = csv.writer(output)

            writer.writerow(
                [
                    "Username",
                    "Location",
                    "Type",
                    "Last Updated",
                    "Ticker",
                    "Name",
                    "Category",
                    "Amount",
                    "Total Weight",
                    "Total Volume",
                ]
            )

            batch_size = 1000
            count = 0

            async for row in stream_storages_csv(db, valid_targets, location):
                writer.writerow(row)
                count += 1
                if count >= batch_size:
                    yield output.getvalue()
                    output.seek(0)
                    output.truncate(0)
                    count = 0

            if count > 0:
                yield output.getvalue()

        return StreamingResponse(
            iter_csv(),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=storages_export.csv"},
        )

    except Exception as e:
        print(f"Storage CSV Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate CSV")
