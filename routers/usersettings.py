import json
import secrets
from datetime import datetime
from typing import Dict, List, Optional

import orjson
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from app.core.security import require_internal_origin
from auth import get_current_user_id

user_settings_router = APIRouter(dependencies=[Depends(require_internal_origin)])

# ==============================================================================
# MODELS
# ==============================================================================

# --- API TOKENS ---
class TokenCreate(BaseModel):
    label: str = Field(..., min_length=1, max_length=50)
    description: Optional[str] = None
    permissions: List[str]

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

# --- WEB SETTINGS (UI STATE) ---
class WebSettingsUpdate(BaseModel):
    page_context: str
    preferences: dict

# --- GLOBAL SETTINGS (BUSINESS LOGIC) ---
class LeasedSiteItem(BaseModel):
    siteId: str
    description: str
    tenant: str

class GlobalSettingsUpdate(BaseModel):
    default_cx_code: Optional[str] = None
    default_currency: Optional[str] = None
    internal_excluded_sites: Optional[List[str]] = None
    internal_leased_sites: Optional[List[LeasedSiteItem]] = None
    # Privacy settings inside global table (optional override)
    privacy_settings: Optional[Dict[str, bool]] = None

# --- REFERENCE DATA ---
class ExchangeRef(BaseModel):
    code: str
    name: str
    currencyCode: str

class UserSiteRef(BaseModel):
    siteId: str
    name: str
    systemName: str


# ==============================================================================
# ENDPOINTS: REFERENCE DATA
# ==============================================================================

@user_settings_router.get("/refs/commodity-exchanges", response_model=List[ExchangeRef])
async def get_commodity_exchanges_ref(request: Request):
    """Returns available commodity exchanges for dropdowns."""
    query = "SELECT code, name, currencycode FROM commodity_exchanges ORDER BY code ASC"

    async with request.app.state.db.pool.acquire() as conn:
        rows = await conn.fetch(query)

    return [
        {"code": r["code"], "name": r["name"], "currencyCode": r["currencycode"]}
        for r in rows
    ]

@user_settings_router.get("/user/sites", response_model=List[UserSiteRef])
async def get_user_sites_ref(request: Request, user_id: str = Depends(get_current_user_id)):
    """
    Returns user sites for Exclusion/Leasing dropdowns.
    Joins sites -> planets -> systems to construct a readable name.
    """
    # 1. Resolve internal User ID
    async with request.app.state.db.pool.acquire() as conn:
        internal_id = await conn.fetchval("SELECT userdataid FROM users WHERE accountid = $1", user_id)
        final_id = internal_id if internal_id else user_id

        # 2. Query Sites
        query = """
            SELECT 
                s.siteid,
                COALESCE(p.name, p.naturalid) as site_name,
                sys.name as system_name
            FROM sites s
            LEFT JOIN planets p ON s.addressplanetid = p.planetid
            LEFT JOIN systems sys ON s.addresssystemid = sys.systemid
            WHERE s.userid = $1
            ORDER BY sys.name, site_name
        """
        rows = await conn.fetch(query, final_id)

    return [
        {"siteId": r["siteid"], "name": r["site_name"] or "Unknown", "systemName": r["system_name"] or "Unknown"}
        for r in rows
    ]


# ==============================================================================
# ENDPOINTS: GLOBAL SETTINGS
# ==============================================================================

@user_settings_router.get("/global")
async def get_global_settings(request: Request, user_id: str = Depends(get_current_user_id)):
    query = "SELECT * FROM user_global_settings WHERE userid = $1"

    async with request.app.state.db.pool.acquire() as conn:
        row = await conn.fetchrow(query, user_id)

        if not row:
            # Default response if no settings exist
            return {
                "userid": user_id,
                "default_cx_code": "IC1",
                "default_currency": "ICA",
                "internal_excluded_sites": [],
                "internal_leased_sites": [],
                "privacy_settings": {
                    "allow_corp_view": False,
                    "allow_global_stats": True,
                    "show_financials_on_profile": False
                }
            }

        data = dict(row)
        for field in ["internal_excluded_sites", "internal_leased_sites", "privacy_settings"]:
            if isinstance(data.get(field), str):
                data[field] = json.loads(data[field])

        leased_sites = data.get("internal_leased_sites", [])
        
        if leased_sites:
            tenant_strings = [lease.get("tenant") for lease in leased_sites if lease.get("tenant")]

            if tenant_strings:
                match_query = """
                    WITH MatchedUsers AS (
                        SELECT
                            UD.DISPLAYNAME as username, 
                            CD.COMPANYCODE as company_code,
                            TRUE AS is_registered
                        FROM users U
                        JOIN users_data UD ON U.USERDATAID = UD.USERID
                        JOIN company_data CD ON U.USERDATAID = CD.USERDATAID
                        WHERE UD.DISPLAYNAME = ANY($1::text[]) OR CD.COMPANYCODE = ANY($1::text[])

                        UNION ALL

                        SELECT
                            USERNAME as username, 
                            COMPANY_CODE as company_code,
                            FALSE AS is_registered
                        FROM public_users_data
                        WHERE USERNAME = ANY($1::text[]) OR COMPANY_CODE = ANY($1::text[])
                    )
                    SELECT DISTINCT ON (COALESCE(company_code, username)) 
                        username, company_code, is_registered
                    FROM MatchedUsers
                    ORDER BY COALESCE(company_code, username), is_registered DESC;
                """
                matched_rows = await conn.fetch(match_query, tenant_strings)

                match_dict = {}
                for m in matched_rows:
                    rich_data = {
                        "username": m["username"],
                        "companyCode": m["company_code"],
                        "isRegistered": m["is_registered"]
                    }

                    if m["username"]: 
                        match_dict[m["username"]] = rich_data
                    if m["company_code"]: 
                        match_dict[m["company_code"]] = rich_data


                for lease in leased_sites:
                    tenant_str = lease.get("tenant")

                    lease["tenant_data"] = match_dict.get(tenant_str, None)

        return data

@user_settings_router.put("/global")
async def update_global_settings(
    payload: GlobalSettingsUpdate,
    request: Request,
    user_id: str = Depends(get_current_user_id)
):
    # Dynamic Query Builder
    fields = []
    values = []
    idx = 1

    def add_field(col_name, val, is_json=False):
        nonlocal idx
        fields.append(f"{col_name} = ${idx}")
        # Serialize Pydantic models or lists to JSON string for JSONB columns
        if is_json:
            if isinstance(val, list) and len(val) > 0 and hasattr(val[0], "dict"):
                # Handle list of Pydantic models (LeasedSiteItem)
                values.append(json.dumps([v.dict() for v in val]))
            else:
                values.append(json.dumps(val))
        else:
            values.append(val)
        idx += 1

    if payload.default_cx_code is not None:
        add_field("default_cx_code", payload.default_cx_code)
    if payload.default_currency is not None:
        add_field("default_currency", payload.default_currency)
    if payload.internal_excluded_sites is not None:
        add_field("internal_excluded_sites", payload.internal_excluded_sites, is_json=True)
    if payload.internal_leased_sites is not None:
        add_field("internal_leased_sites", payload.internal_leased_sites, is_json=True)
    if payload.privacy_settings is not None:
        add_field("privacy_settings", payload.privacy_settings, is_json=True)

    if not fields:
        return {"status": "no_changes"}

    # Add User ID as the last parameter
    values.append(user_id)

    # UPSERT Query
    col_names = [f.split(' = ')[0] for f in fields]
    placeholders = [f"${i+1}" for i in range(len(col_names))]

    query = f"""
        INSERT INTO user_global_settings (userid, {', '.join(col_names)}, updated_at)
        VALUES (${idx}, {', '.join(placeholders)}, NOW())
        ON CONFLICT (userid) DO UPDATE SET
        {', '.join(fields)}, updated_at = NOW()
    """

    async with request.app.state.db.pool.acquire() as conn:
        await conn.execute(query, *values)

    return {"status": "success", "updated": col_names}


# ==============================================================================
# ENDPOINTS: API TOKENS (Existing)
# ==============================================================================

@user_settings_router.post("/tokens", response_model=TokenResponse)
async def create_api_token(payload: TokenCreate, request: Request, user_id: str = Depends(get_current_user_id)):
    raw_token = f"ptk_{secrets.token_urlsafe(32)}"
    token_prefix = raw_token[:10]
    token_hash = raw_token # Needs to be hashed

    query = """
        INSERT INTO user_api_tokens (user_id, token_hash, token_prefix, label, description, permissions)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        RETURNING id, created_at
    """
    permissions = json.dumps(payload.permissions)

    async with request.app.state.db.pool.acquire() as conn:
        row = await conn.fetchrow(query, user_id, token_hash, token_prefix, payload.label, payload.description, permissions)

    return {
        "id": str(row["id"]),
        "label": payload.label,
        "description": payload.description,
        "permissions": payload.permissions,
        "token_prefix": token_prefix,
        "token": raw_token,
        "created_at": row["created_at"],
    }

@user_settings_router.get("/tokens", response_model=List[TokenResponse])
async def list_api_tokens(request: Request, user_id: str = Depends(get_current_user_id)):
    query = "SELECT id, label, description, permissions, token_hash, created_at FROM user_api_tokens WHERE user_id = $1 ORDER BY created_at DESC"

    async with request.app.state.db.pool.acquire() as conn:
        rows = await conn.fetch(query, user_id)

    results = []
    for row in rows:
        item = dict(row)
        item["id"] = str(item["id"])
        item["token"] = str(item["token_hash"])
        if isinstance(item["permissions"], str):
            item["permissions"] = json.loads(item["permissions"])
        results.append(item)
    return results

@user_settings_router.patch("/tokens/{token_id}")
async def update_api_token(token_id: str, payload: TokenUpdate, request: Request, user_id: str = Depends(get_current_user_id)):
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

    if not fields: return {"status": "no_changes"}

    values.append(token_id)
    values.append(user_id)
    query = f"UPDATE user_api_tokens SET {', '.join(fields)} WHERE id = ${idx} AND user_id = ${idx + 1}"

    async with request.app.state.db.pool.acquire() as conn:
        result = await conn.execute(query, *values)

    if result == "UPDATE 0": raise HTTPException(404, "Token not found")
    return {"status": "updated"}

@user_settings_router.delete("/tokens/{token_id}")
async def delete_api_token(token_id: str, request: Request, user_id: str = Depends(get_current_user_id)):
    query = "DELETE FROM user_api_tokens WHERE id = $1 AND user_id = $2"
    async with request.app.state.db.pool.acquire() as conn:
        await conn.execute(query, token_id, user_id)
    return {"status": "deleted"}


# ==============================================================================
# ENDPOINTS: WEB SETTINGS (Legacy Privacy)
# ==============================================================================

@user_settings_router.get("/privacy")
async def get_privacy_settings(request: Request, user_id: str = Depends(get_current_user_id)):
    query = "SELECT page_context, preferences FROM user_web_settings WHERE user_id = $1"

    async with request.app.state.db.pool.acquire() as conn:
        rows = await conn.fetch(query, user_id)

    # 1. Load DB Data
    settings_map = {row["page_context"]: orjson.loads(row["preferences"]) if isinstance(row["preferences"], str) else row["preferences"] for row in rows}

    # 2. STRICT Defaults
    default_web_settings = [
        {
            "page_context": "DASHBOARD",
            "preferences": {"site_data": True, "ships_data": True, "flight_data": True}
        },
        {
            "page_context": "CORP_PAGE",
            "preferences": {"storage_data": True, "site_data": True, "production_data": True}
        },
        {
            "page_context": "COOPERATION",
            "preferences": {"site_data": True, "storage_data": True, "production_data": True}
        }
    ]

    defaults = {setting["page_context"]: setting["preferences"] for setting in default_web_settings}

    # 3. Merge Logic
    for context, default_prefs in defaults.items():
        if context not in settings_map:
            settings_map[context] = default_prefs
        else:
            for key, val in default_prefs.items():
                if key not in settings_map[context]:
                    settings_map[context][key] = val

    return settings_map

@user_settings_router.post("/privacy")
async def update_privacy_settings(payload: WebSettingsUpdate, request: Request, user_id: str = Depends(get_current_user_id)):
    query = """
        INSERT INTO user_web_settings (user_id, page_context, preferences)
        VALUES ($1, $2, $3::jsonb)
        ON CONFLICT (user_id, page_context)
        DO UPDATE SET preferences = EXCLUDED.preferences
    """
    async with request.app.state.db.pool.acquire() as conn:
        await conn.execute(query, user_id, payload.page_context, json.dumps(payload.preferences))
    return {"status": "updated", "context": payload.page_context}
