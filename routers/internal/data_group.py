from fastapi import APIRouter, Depends, Request, HTTPException
from pydantic import BaseModel
from typing import List, Optional

# Auth
from app.core.security import require_internal_origin
from auth import RequireAuth, get_current_user_id

# Import Services
from services.internal.data_group import (
    service_create_group,
    service_delete_group,
    service_invite_user,
    service_accept_invite,
    service_create_group_token,
    service_kick_member,
    service_leave_group,
    service_search_users,
    service_set_permission,
    service_list_user_groups,
    service_list_group_members,
    service_update_my_shares
)

group_router = APIRouter(dependencies=[Depends(require_internal_origin)])

# ==============================================================================
# INPUT SCHEMAS
# ==============================================================================

class CreateGroupRequest(BaseModel):
    name: str
    description: Optional[str] = None

class InviteRequest(BaseModel):
    username: str

class AcceptInviteRequest(BaseModel):
    granted_permissions: List[str]

class CreateTokenRequest(BaseModel):
    label: str

class SetPermissionRequest(BaseModel):
    username: str
    can_read_data: bool

# ==============================================================================
# ENDPOINTS
# ==============================================================================

@group_router.get("/")
async def list_groups(
    request: Request,
    user_id: str = Depends(get_current_user_id)
):
    """Lists all groups the user belongs to (invited or accepted)."""
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        return await service_list_user_groups(conn, user_id)

@group_router.get("/users/search")
async def search_users(
    request: Request,
    q: str
):
    """Search for invite-able users (is_synchronized=True)."""
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        return await service_search_users(conn, q)

@group_router.get("/{group_id}/members")
async def list_members(
    request: Request,
    group_id: str,
    user_id: str = Depends(get_current_user_id)
):
    """Lists members of a group."""
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        return await service_list_group_members(conn, user_id, group_id)

@group_router.post("/")
async def create_group(
    request: Request,
    payload: CreateGroupRequest,
    user_id: str = Depends(get_current_user_id)
):
    """
    Creates a new group. 
    """
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        return await service_create_group(
            conn, 
            user_id, 
            payload.name, 
            payload.description
        )

@group_router.delete("/{group_id}")
async def delete_group(
    request: Request,
    group_id: str,
    user_id: str = Depends(get_current_user_id)
):
    """Permanently deletes the group (Owner Only)."""
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        return await service_delete_group(conn, user_id, group_id)

@group_router.post("/{group_id}/invite")
async def invite_user(
    request: Request,
    group_id: str,
    payload: InviteRequest,
    user_id: str = Depends(get_current_user_id)
):
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        return await service_invite_user(conn, user_id, group_id, payload.username)

@group_router.post("/{group_id}/accept")
async def accept_invite(
    request: Request,
    group_id: str,
    payload: AcceptInviteRequest,
    user_id: str = Depends(get_current_user_id)
):
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        return await service_accept_invite(conn, user_id, group_id, payload.granted_permissions)

@group_router.delete("/{group_id}/leave")
async def leave_group(
    request: Request,
    group_id: str,
    user_id: str = Depends(get_current_user_id)
):
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        return await service_leave_group(conn, user_id, group_id)

@group_router.delete("/{group_id}/members/{target_id}")
async def kick_member(
    request: Request,
    group_id: str,
    target_id: str,
    user_id: str = Depends(get_current_user_id)
):
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        return await service_kick_member(conn, user_id, group_id, target_id)

@group_router.post("/{group_id}/tokens")
async def create_group_token(
    request: Request,
    group_id: str,
    payload: CreateTokenRequest,
    user_id: str = Depends(get_current_user_id)
):
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        return await service_create_group_token(conn, user_id, group_id, payload.label)

@group_router.post("/{group_id}/permissions")
async def set_member_permission(
    request: Request,
    group_id: str,
    payload: SetPermissionRequest,
    user_id: str = Depends(get_current_user_id)
):
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        return await service_set_permission(conn, user_id, group_id, payload.username, payload.can_read_data)
    
# Schema
class UpdateSharesRequest(BaseModel):
    granted_permissions: List[str]

# Endpoint
@group_router.patch("/{group_id}/shares")
async def update_my_shares(
    request: Request,
    group_id: str,
    payload: UpdateSharesRequest,
    user_id: str = Depends(get_current_user_id)
):
    """Updates the data (permissions) the logged-in user shares with the group."""
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        return await service_update_my_shares(conn, user_id, group_id, payload.granted_permissions)