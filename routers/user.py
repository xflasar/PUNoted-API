import json
import logging
import secrets
import uuid
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

import services.user as UserService
from app.core.security import require_internal_origin
from auth import get_current_user_id
from models.user import UserSettingsOut, UserSettingsUpdate

logger = logging.getLogger(__name__)

user_router = APIRouter(dependencies=[Depends(require_internal_origin)])


# GET current user settings
@user_router.get("/settings", response_model=UserSettingsOut)
async def get_user_settings(request: Request, user_id: str = Depends(get_current_user_id)):
    try:
        uid = user_id
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid user ID")

    db = request.app.state.db
    return await UserService.get_user_settings(db, uid)


# PUT update user settings
@user_router.put("/settings", response_model=UserSettingsOut)
async def update_user_settings(
    update: UserSettingsUpdate,
    request: Request,
    user_id: str = Depends(get_current_user_id),
):
    try:
        uid = user_id
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid user ID")

    db = request.app.state.db
    await UserService.update_user_settings(db, uid, update)

    # Return the updated settings
    return await UserService.get_user_settings(db, uid)


# GET is_synchronized


@user_router.get("/synchronized")
async def is_synchronized(
    request: Request,
    user_id: str = Depends(get_current_user_id),
):
    db = request.app.state.db

    async with db.pool.acquire() as conn:
        async with conn.transaction():
            # Fetch user basic synchronization state
            user = await conn.fetchrow(
                """
                SELECT is_synchronized, userdataid
                FROM users
                WHERE accountid = $1
                """,
                user_id,
            )

            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            # If the user has not completed synchronization
            if not user["is_synchronized"]:
                return {"isSynchronized": False}

            # If user is synchronized but has no userdataid (corrupt or incomplete link)
            if not user["userdataid"]:
                return {"isSynchronized": False, "error": "No userdata associated"}

            # Fetch the userdata info
            userdata = await conn.fetchrow(
                """
                SELECT
                    ud.displayname,
                    uc.companyname,
                    uc.companycode,
                    c.name as corpname
                FROM users_data ud
                LEFT JOIN company_data uc
                    ON ud.companyid = uc.companyid
                LEFT JOIN corporation_shareholders cs ON cs.companyid = ud.companyid
                LEFT JOIN corporations c ON c.id = cs.corporationid
                WHERE ud.userid = $1
                """,
                str(user["userdataid"]),
            )

            if not userdata:
                return {
                    "isSynchronized": False,
                    "userdata": None,
                    "warning": "No userdata found",
                }

            return {"isSynchronized": True, "userdata": dict(userdata)}


# POST synchronize (1-time only)
@user_router.post("/synchronize")
async def synchronize_user(user_id: str, request: Request):
    try:
        uid = uuid.UUID(user_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid user ID")

    db = request.app.state.db
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            user = await conn.fetchrow(
                "SELECT is_synchronized, userdataid FROM users WHERE accountid = $1",
                uid,
            )
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            if not user["userdataid"]:
                raise HTTPException(status_code=404, detail="User has no ingame data.")

            if user["is_synchronized"]:
                raise HTTPException(status_code=403, detail="User already synchronized")

            # Mark as synchronized
            await conn.execute("UPDATE users SET is_synchronized = TRUE WHERE accountid = $1", uid)

    return {"message": "User synchronized successfully", "isSynchronized": True}


class TokenCreate(BaseModel):
    label: str = Field(..., min_length=1, max_length=50)
    description: Optional[str] = None
    permissions: List[str]  # e.g. ["ships:read", "sites:read"]


class TokenUpdate(BaseModel):
    label: Optional[str] = None
    description: Optional[str] = None
    permissions: Optional[List[str]] = None


class TokenResponse(BaseModel):
    id: str
    label: str
    description: Optional[str]
    permissions: List[str]
    token: str
    created_at: datetime
    # We NEVER return the full token hash or the raw token in a "List" response


# --- ENDPOINTS FOR API TOKENS ---


@user_router.post("/tokens", response_model=TokenResponse)
async def create_api_token(payload: TokenCreate, request: Request, user_id: str = Depends(get_current_user_id)):
    raw_token = f"ptk_{secrets.token_urlsafe(32)}"
    token_prefix = raw_token[:10]

    token_hash = raw_token

    query = """
        INSERT INTO user_api_tokens (user_id, token_hash, token_prefix, label, description, permissions)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        RETURNING id, created_at
    """

    permissions = json.dumps(payload.permissions)

    async with request.app.state.db.pool.acquire() as conn:
        row = await conn.fetchrow(
            query,
            user_id,
            token_hash,
            token_prefix,
            payload.label,
            payload.description,
            permissions,
        )

    return {
        "id": str(row["id"]),
        "label": payload.label,
        "description": payload.description,
        "permissions": payload.permissions,
        "token_prefix": token_prefix,
        "token": raw_token,
        "created_at": row["created_at"],
    }


@user_router.get("/tokens", response_model=List[TokenResponse])
async def list_api_tokens(request: Request, user_id: str = Depends(get_current_user_id)):
    query = """
        SELECT id, label, description, permissions, token_hash, created_at
        FROM user_api_tokens
        WHERE user_id = $1
        ORDER BY created_at DESC
    """

    async with request.app.state.db.pool.acquire() as conn:
        rows = await conn.fetch(query, user_id)

    results = []
    for row in rows:
        item = dict(row)

        # 1. Convert UUID -> String
        item["id"] = str(item["id"])
        item["token"] = str(item["token_hash"])

        # 2. Convert JSON String -> List
        if isinstance(item["permissions"], str):
            item["permissions"] = json.loads(item["permissions"])

        results.append(item)

    return results


@user_router.patch("/tokens/{token_id}")
async def update_api_token(
    token_id: str,
    payload: TokenUpdate,
    request: Request,
    user_id: str = Depends(get_current_user_id),
):
    # Dynamic query building
    fields = []
    values = []
    idx = 1

    if payload.label is not None:
        fields.append(f"label = ${idx}")
        values.append(payload.label)
        idx += 1
    if payload.description is not None:
        fields.append(f"description = ${idx}")
        values.append(payload.description)
        idx += 1
    if payload.permissions is not None:
        fields.append(f"permissions = ${idx}::jsonb")
        values.append(json.dumps(payload.permissions))
        idx += 1

    if not fields:
        return {"status": "no_changes"}

    values.append(token_id)
    values.append(user_id)

    query = f"""
        UPDATE user_api_tokens
        SET {", ".join(fields)}
        WHERE id = ${idx} AND user_id = ${idx + 1}
    """

    async with request.app.state.db.pool.acquire() as conn:
        result = await conn.execute(query, *values)

    if result == "UPDATE 0":
        raise HTTPException(404, "Token not found")

    return {"status": "updated"}


@user_router.delete("/tokens/{token_id}")
async def delete_api_token(token_id: str, request: Request, user_id: str = Depends(get_current_user_id)):
    query = "DELETE FROM user_api_tokens WHERE id = $1 AND user_id = $2"

    async with request.app.state.db.pool.acquire() as conn:
        await conn.execute(query, token_id, user_id)

    return {"status": "deleted"}
