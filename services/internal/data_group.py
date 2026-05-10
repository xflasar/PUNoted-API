import hashlib
import json
import secrets
import string

from asyncpg import Connection
from fastapi import HTTPException

from repositories.data_group import (
    repo_add_member,
    repo_create_group,
    repo_create_token,
    repo_delete_group,
    repo_get_group_by_id,
    repo_get_member,
    repo_get_user_by_id,
    repo_get_user_by_username,
    repo_list_groups_for_user,
    repo_list_members,
    repo_remove_member,
    repo_search_sync_users,
    repo_set_member_permission,
    repo_update_member_acceptance,
    repo_update_member_shares,
)


# --- Helpers ---
def _generate_suffix(length=12) -> str:
    chars = string.ascii_letters + string.digits
    return ''.join(secrets.choice(chars) for _ in range(length))

def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()

# --- Service Functions ---

async def service_list_user_groups(conn: Connection, user_id: str):
    rows = await repo_list_groups_for_user(conn, user_id)
    results = []
    for row in rows:
        r = dict(row)
        r["id"] = str(r["id"])
        r["owner_id"] = str(r["owner_id"])
        r["is_owner"] = (r["owner_id"] == user_id)

        if r["access_key"] and r["personal_suffix"]:
            r["my_full_token"] = f"{r['access_key']}-{r['personal_suffix']}"
        else:
            r["my_full_token"] = None

        results.append(r)
    return results

async def service_search_users(conn: Connection, query: str):
    if len(query) < 2:
        return []
    rows = await repo_search_sync_users(conn, query)
    return [r["displayname"] for r in rows]

async def service_list_group_members(conn: Connection, requester_id: str, group_id: str):
    """
    Lists all members of a group.
    Security: Checks if requester is actually a member of that group first.
    """
    # 1. Security Check
    membership = await repo_get_member(conn, group_id, requester_id)
    if not membership:
        raise HTTPException(403, "Access denied: You are not a member of this group.")

    # 2. Fetch Members
    rows = await repo_list_members(conn, group_id)

    # 3. Format & Parse JSON
    results = []
    for row in rows:
        r = dict(row)

        perms = r.get("granted_permissions")

        if isinstance(perms, str):
            try:
                r["granted_permissions"] = json.loads(perms)
            except json.JSONDecodeError:
                r["granted_permissions"] = [] # Fallback
        elif perms is None:
            r["granted_permissions"] = []

        results.append(r)

    return results

async def service_create_group(conn: Connection, owner_id: str, name: str, description: str):
    # 1. Generate the Public Group Key
    # This is the "prefix" shared by everyone
    group_access_key = f"grp_{secrets.token_urlsafe(8)}"

    # 2. Create Group
    group_id = await repo_create_group(conn, owner_id, name, group_access_key, description)

    # 3. Add Owner
    # Owner gets a suffix just like everyone else
    owner_suffix = _generate_suffix()
    await repo_add_member(
        conn,
        group_id=group_id,
        user_id=owner_id,
        status="ACCEPTED",
        suffix=owner_suffix,
        can_read=True
    )

    # 4. Return formatted token immediately
    return {
        "status": "created",
        "group_id": group_id,
        # Standard Format: GROUP_KEY-USER_SUFFIX
        "full_token": f"{group_access_key}-{owner_suffix}",
        "message": "Group created."
    }

async def service_delete_group(conn: Connection, requester_id: str, group_id: str):
    # 1. Verify Ownership
    group = await repo_get_group_by_id(conn, group_id)
    if not group:
        raise HTTPException(404, "Group not found")

    if str(group["owner_id"]) != requester_id:
        raise HTTPException(403, "Only the owner can delete the group.")

    # 2. Delete
    await repo_delete_group(conn, group_id)
    return {"status": "deleted", "group_id": group_id}

async def service_invite_user(conn: Connection, requester_id: str, group_id: str, target_username: str):
    # Verify Ownership
    group = await repo_get_group_by_id(conn, group_id)
    if not group or str(group["owner_id"]) != requester_id:
        raise HTTPException(403, "Only the owner can invite users.")

    # Resolve Username
    target = await repo_get_user_by_username(conn, target_username)
    if not target:
        raise HTTPException(404, "User not found.")
    target_id = str(target["accountid"])

    # Check Duplicate
    existing = await repo_get_member(conn, group_id, target_id)
    if existing:
        raise HTTPException(400, "User is already in the group (or invited).")

    # Add Invite
    await repo_add_member(conn, group_id, target_id, status="INVITED")

    # TODO: Add WebSocket trigger here later
    return {"status": "invited", "username": target_username}

async def service_accept_invite(conn: Connection, user_id: str, group_id: str, granted_permissions: list):
    member = await repo_get_member(conn, group_id, user_id)
    if not member or member["status"] != "INVITED":
        raise HTTPException(400, "No pending invite found.")

    secret_suffix = _generate_suffix()
    await repo_update_member_acceptance(conn, group_id, user_id, secret_suffix, granted_permissions)

    return {
        "status": "joined",
        "personal_suffix": secret_suffix,
        "instruction": "IMPORTANT: Save this suffix. Format: GROUP_TOKEN-SUFFIX"
    }

async def service_leave_group(conn: Connection, user_id: str, group_id: str):
    """User removes themselves."""
    group = await repo_get_group_by_id(conn, group_id)
    if not group:
        raise HTTPException(404, "Group not found")

    if str(group["owner_id"]) == user_id:
        raise HTTPException(400, "Owner cannot leave the group. You must delete the group instead.")

    await repo_remove_member(conn, group_id, user_id)
    return {"status": "left", "group_id": group_id}

async def service_kick_member(conn: Connection, requester_id: str, group_id: str, target_user_id: str):
    """Owner removes another user."""
    # 1. Verify Ownership
    group = await repo_get_group_by_id(conn, group_id)
    if not group or str(group["owner_id"]) != requester_id:
        raise HTTPException(403, "Only the owner can remove members.")

    # 2. Prevent Owner Suicide
    if str(group["owner_id"]) == target_user_id:
        raise HTTPException(400, "Cannot remove the owner.")

    # 3. Execute
    await repo_remove_member(conn, group_id, target_user_id)
    return {"status": "removed", "user_id": target_user_id}

async def service_create_group_token(conn: Connection, requester_id: str, group_id: str, label: str):
    group = await repo_get_group_by_id(conn, group_id)
    if not group or str(group["owner_id"]) != requester_id:
        raise HTTPException(403, "Only owner can create tokens.")

    raw_token = secrets.token_urlsafe(32)
    token_hash = _hash_token(raw_token)
    prefix = raw_token[:4]

    await repo_create_token(conn, requester_id, group_id, token_hash, prefix, label)

    return {"token": raw_token, "prefix": prefix}

async def service_set_permission(conn: Connection, requester_id: str, group_id: str, target_userid: str, can_read: bool):
    group = await repo_get_group_by_id(conn, group_id)
    if not group or str(group["owner_id"]) != requester_id:
        raise HTTPException(403, "Not authorized.")

    target = await repo_get_user_by_id(conn, target_userid)
    if not target:
        raise HTTPException(404, "User not found.")

    await repo_set_member_permission(conn, group_id, str(target["accountid"]), can_read)
    return {"status": "updated"}

async def service_update_my_shares(conn: Connection, user_id: str, group_id: str, permissions: list):
    # 1. Verify membership
    member = await repo_get_member(conn, group_id, user_id)
    if not member:
        raise HTTPException(404, "Not a member of this group.")

    # 2. Update
    await repo_update_member_shares(conn, group_id, user_id, permissions)
    return {"status": "updated", "permissions": permissions}
