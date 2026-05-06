import csv
import io
import orjson
from typing import Optional, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse, JSONResponse as DefaultJSONResponse

from auth import RequireAuth
from app.core.limiter import get_auth_key, limiter
from endpoints.Protected.services.storageuser import fetch_storages_as_json, stream_storages_csv

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
            return Response(content='[]', media_type="application/json")

        json_str = await fetch_storages_as_json(db, valid_targets, location)

        return Response(content=json_str, media_type="application/json")

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
    response_class=ORJSONResponse
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

        # 1. Fetch standard multi-user structure
        json_str = await fetch_storages_as_json(db, valid_targets, location)

        # 2. Unwrap to return ONLY the Storages list
        try:
            data_list = orjson.loads(json_str)
            if data_list and "Storages" in data_list[0]:
                return data_list[0]["Storages"]
            return []
        except Exception:
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