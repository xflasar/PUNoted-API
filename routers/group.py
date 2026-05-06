# /backend/group.py (Modified to search by displayname)

import json
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field

from app.core.security import require_internal_origin
from auth import get_current_user_id
from websocket.websocket_manager import manager


# --- Pydantic Models ---
class GroupMember(BaseModel):
    uid: str
    username: str


class Chain(BaseModel):
    nodes: List[Dict[str, Any]]
    links: List[Dict[str, Any]]


class GroupMemberOutput(BaseModel):
    uid: str
    username: str
    displayName: Optional[str]
    role: str


class FullGroupData(BaseModel):
    """The desired structure of the final JSON output."""

    id: str
    name: str
    ownerId: str
    chain: Chain
    isActive: bool
    members: List[GroupMemberOutput]
    created_at: str
    updated_at: str


class ChainUpdateModel(BaseModel):
    """Model for updating the group's production chain."""

    user_id: str
    chain_data: Dict[str, Any]


class InvitationModel(BaseModel):
    invitee_username: str
    group_id: str


class GroupNameModel(BaseModel):
    """Model for creating a new group."""

    name: str = Field(..., title="New Group Name")
    owner_id: str = Field(..., title="The ID of the user creating the group")


class Group(BaseModel):
    id: str
    name: str
    ownerId: str
    ownerDisplayName: str
    chain: Dict[str, Any]
    isActive: bool
    members: List[GroupMember] | None
    created_at: str
    updated_at: str


class RoleUpdateModel(BaseModel):
    """Model for changing a user's role in a group."""

    new_role: str = Field(..., pattern="^(owner|editor|viewer)$")


class GroupDeleteModel(BaseModel):
    """Model for deleting a group."""

    user_id: str = Field(..., title="The ID of the user attempting to delete the group")


group_router = APIRouter(dependencies=[Depends(require_internal_origin)])

# --- Helper Functions (Database Access) ---


async def get_user_by_displayname(conn: Any, displayname: str) -> Optional[Dict[str, str]]:
    """Fetches user details needed for membership (uid/accountid, username/displayname)."""
    user_record = await conn.fetchrow(
        """SELECT u.accountid, ud.displayname
        FROM users_data ud
        INNER JOIN
            users u ON u.userdataid = ud.userId
        WHERE ud.displayname = $1
        """,
        displayname,
    )
    if user_record:
        return {
            "uid": str(user_record["accountid"]),
            "username": user_record["displayname"],
        }
    return None


async def is_group_owner(conn: Any, group_id: str, user_id: str) -> bool:
    """Checks if the user is the owner of the group."""
    owner_id = await conn.fetchval("SELECT owner_id FROM production_groups WHERE id = $1", group_id)
    return str(owner_id) == str(user_id)


async def get_single_group_data(conn: Any, group_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetches a single group's full data structure (including all members)
    in the FullGroupData format after a change.
    """
    group_uuid = uuid.UUID(group_id)
    query = """
    SELECT json_build_object(
        'id', pg.id, 
        'name', pg.name, 
        'ownerId', pg.owner_id, 
        'chain', pg.chain_data, 
        'isActive', pg.is_active, 
        'members', json_agg(
            json_build_object(
                'uid', u.accountid, 
                'username', u.username, 
                'displayName', ud.displayname, 
                'role', gm.role 
            )
        )
    ) AS group_data
    FROM production_groups pg
    INNER JOIN group_members gm ON pg.id = gm.group_id
    INNER JOIN users u ON gm.user_id = u.accountid
    INNER JOIN users_data ud ON u.userdataid = ud.userid
    WHERE pg.id = $1 
    GROUP BY pg.id, pg.name, pg.owner_id, pg.chain_data, pg.is_active
    """

    record = await conn.fetchval(query, group_uuid)

    if record:
        return json.loads(record)
    return None


async def get_user_by_username(conn, username: str) -> Optional[Dict[str, Any]]:
    """Fetches a user by their unique username (e.g., email or unique display name)."""
    query = "SELECT uid, display_name FROM users WHERE email = $1"
    record = await conn.fetchrow(query, username)
    if record:
        return {"uid": str(record["uid"]), "display_name": record["display_name"]}
    return None


async def get_user_role(conn, group_id: str, user_id: str) -> Optional[str]:
    """Fetches the role of a user within a specific group."""
    query = """
    SELECT role FROM group_members 
    WHERE group_id = $1 AND user_id = $2
    """
    record = await conn.fetchval(query, uuid.UUID(group_id), uuid.UUID(user_id))
    return record


async def is_already_member(conn, group_id: str, user_id: str) -> bool:
    """Checks if a user is already a member of the group."""
    query = """
    SELECT 1 FROM group_members 
    WHERE group_id = $1 AND user_id = $2
    """
    record = await conn.fetchval(query, uuid.UUID(group_id), uuid.UUID(user_id))
    return record is not None


async def add_group_member(conn, group_id: str, user_id: str, role: str):
    """Inserts a new member into the group_members table."""
    query = """
    INSERT INTO group_members (group_id, user_id, role)
    VALUES ($1, $2, $3)
    ON CONFLICT (group_id, user_id) DO NOTHING; -- Prevents duplicate inserts
    """
    await conn.execute(query, uuid.UUID(group_id), uuid.UUID(user_id), role)


async def remove_group_member(conn: Any, group_id: str, member_uid: str) -> bool:
    """Removes a member from a group. Returns True if successful."""
    try:
        group_uuid = uuid.UUID(group_id)
        # 1. Look up the member's internal user_id (accountid) from their external UID
        user_record = await conn.fetchrow("SELECT accountid FROM users WHERE accountid = $1", member_uid)

        if not user_record:
            print(f"User with UID {member_uid} not found.")
            return False

        internal_user_id = user_record["accountid"]

        # 2. Delete the record from the group_members table
        query = """
        DELETE FROM group_members 
        WHERE group_id = $1 AND user_id = $2
        """
        # conn.execute returns a status tag like 'DELETE 1' if successful
        result = await conn.execute(query, group_uuid, internal_user_id)

        return result == "DELETE 1"

    except Exception as e:
        print(f"Database error during member removal: {e}")
        return False


# --- Group Management Endpoints ---


@group_router.post("/{group_id}/invite", status_code=status.HTTP_202_ACCEPTED)
async def send_invite(
    group_id: str,
    invite_data: InvitationModel,
    request: Request,
    user_id: str = Depends(get_current_user_id),
):
    """
    Instantly adds a user to the group as a 'viewer'.
    Authorization: Only 'owner' or 'editor' can invite.
    """
    db = request.app.state.db

    async with db.pool.acquire() as conn:
        # 1. Authorization: Verify inviter has permission ('owner' or 'editor')
        inviter_role = await get_user_role(conn, group_id, user_id)
        if inviter_role not in ["owner", "editor"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only the group owner or an editor can send invitations.",
            )

        # 2. Find invitee's user ID/details by unique username
        invitee_details = await get_user_by_displayname(conn, invite_data.invitee_username)
        if not invitee_details:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User with identifier '{invite_data.invitee_username}' not found.",
            )

        invitee_id = invitee_details["uid"]

        # 3. Check if already a member
        if await is_already_member(conn, group_id, invitee_id):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"User {invitee_details['display_name']} is already a member of this group.",
            )

        # 4. Add user to the group as 'viewer'
        async with conn.transaction():
            await add_group_member(conn, group_id, invitee_id, "viewer")

        # 5. WEBSOCKET PUSH: Notify the invited user in real-time
        try:
            # Fetch the full group data, which now includes the new member
            full_group_data = await get_single_group_data(conn, group_id)

            if full_group_data and manager:
                invite_message = json.dumps(
                    {
                        "type": "GROUP_INVITE",
                        "userId": "SERVER",
                        "payload": {"group": full_group_data},
                    }
                )

                await manager.send_to_user(invitee_id, invite_message)
                print(f"DEBUG: Sent GROUP_INVITE WebSocket message to user {invitee_id}.")

        except Exception as e:
            print(f"ERROR: Failed to send real-time GROUP_INVITE to {invitee_id}: {e}")

    print(f"User {invitee_details['uid']} instantly added as 'viewer' to group {group_id}.")

    return {
        "message": f"User {invitee_details['uid']} added as a viewer successfully.",
        "new_member_id": invitee_id,
    }


@group_router.delete("/{group_id}/member", status_code=status.HTTP_200_OK)
async def remove_member(
    group_id: str,
    member_data: Dict[str, Any],
    request: Request,
    user_id: str = Depends(get_current_user_id),
):
    """Removes a member from the group. Only the group owner can perform this action."""
    member_to_remove_uid = member_data["member_uid"]

    async with request.app.state.db.pool.acquire() as conn:
        # 1. Authorization: Ensure the current user is the owner
        is_owner = await is_group_owner(conn, group_id, user_id)
        if not is_owner:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only the group owner can remove members.",
            )

        # 2. Prevent the owner from removing themselves
        if member_to_remove_uid == user_id:  # Assuming user_id is the UID
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="The owner cannot remove themselves.",
            )

        # 3. Perform the removal
        success = await remove_group_member(conn, group_id, member_to_remove_uid)

        try:
            if manager:
                invite_message = json.dumps(
                    {
                        "type": "GROUP_REMOVE",
                        "userId": "SERVER",
                        "payload": {"groupId": group_id},
                    }
                )

                await manager.send_to_user(member_to_remove_uid, invite_message)
                print(f"DEBUG: Sent GROUP_REMOVE WebSocket message to user {member_to_remove_uid}.")

        except Exception as e:
            print(f"ERROR: Failed to send real-time GROUP_REMOVE to {member_to_remove_uid}: {e}")

        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Member with UID {member_to_remove_uid} not found in group {group_id}.",
            )

    return {"message": f"Member {member_to_remove_uid} removed from group {group_id}."}


@group_router.get("/all_users")
async def get_all_users(request: Request):
    db = request.app.state.db

    async with db.pool.acquire() as conn:
        query = """
            SELECT COALESCE(ud.displayname, u.displayname) as displayname 
            FROM users u 
            LEFT JOIN users_data ud ON u.userdataid = ud.userid
            WHERE COALESCE(ud.displayname, u.displayname) IS NOT NULL;  -- 🔑 ADDED: Filter out rows where the displayname is NULL
        """
        users_displaynames = await conn.fetch(query)

        return users_displaynames


# for now its mocked not implemented
@group_router.post("/api/groups/join")
async def join_group(token: str, request: Request):
    """
    Validates an invitation token and adds the user to the group.

    Note on Auth: In a real app, this endpoint would verify the user making
    the request matches the 'invitee_id' stored in the token record.
    We MOCK the current user ID for this example.
    """
    db = request.app.state.db
    current_user_id = "MOCK_CURRENT_USER_ACCOUNTID"

    async with db.pool.acquire() as conn:
        # 1. Lookup and validate token (and ensure it matches the current user)
        invite_record = await conn.fetchrow(
            """
            SELECT group_id, invitee_id 
            FROM invitations 
            WHERE token = $1 AND invitee_id = $2 AND created_at > NOW() - INTERVAL '48 hours'
            """,
            token,
            current_user_id,
        )

        if not invite_record:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid, expired, or incorrect invitation token for this user.",
            )

        group_id = str(invite_record["group_id"])

        # 2. Fetch user details to add to group's members list
        # Using the same mapping: accountid is uid, displayname is username
        user_details = await conn.fetchrow("SELECT displayname FROM users_data WHERE accountid = $1", current_user_id)
        user_username = user_details["displayname"]

        new_member = {"uid": current_user_id, "username": user_username}

        # 3. Add user to group members
        await conn.execute(
            """
            UPDATE groups 
            SET members = members || $1::jsonb 
            WHERE id = $2 AND NOT (members @> $1::jsonb)
            """,
            json.dumps([new_member]),
            group_id,
        )

        # 4. Delete token
        await conn.execute("DELETE FROM invitations WHERE token = $1", token)

    print(f"User {current_user_id} successfully joined group {group_id}")
    return {"message": "Successfully joined the group.", "group_id": group_id}


@group_router.put("/{group_id}/members/{member_id}/role", status_code=status.HTTP_200_OK)
async def update_member_role(
    group_id: str,
    member_id: str,
    role_data: RoleUpdateModel,
    request: Request,
    user_id: str = Depends(get_current_user_id),
):
    """
    Allows the group owner to change the role of another member.
    Owner's role cannot be changed via this endpoint.
    """
    db = request.app.state.db
    new_role = role_data.new_role

    # Check if IDs are valid UUIDs
    try:
        group_uuid = uuid.UUID(group_id)
        member_uuid = uuid.UUID(member_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid ID format.")

    async with db.pool.acquire() as conn:
        # 1. Authorization: Only the 'owner' can change roles
        owner_role = await get_user_role(conn, group_id, user_id)
        if owner_role != "owner":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only the group owner can change member roles.",
            )

        # 2. Check: Owner cannot change their own role
        if member_id == user_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="The group owner cannot change their own role.",
            )

        # 3. Perform the UPDATE operation
        updated_count = await conn.execute(
            """
            UPDATE group_members 
            SET role = $1 
            WHERE group_id = $2 AND user_id = $3
            """,
            new_role,
            group_uuid,
            member_uuid,
        )

        if updated_count.endswith(" 0"):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Member or group not found.",
            )

    return {"message": f"Role for member {member_id} updated to {new_role} successfully."}


@group_router.get("", response_model=List[FullGroupData])
async def get_all_user_groups(request: Request, user_id: str = Depends(get_current_user_id)):
    """
    The initial sync endpoint. Returns ALL group data for groups the current user is a member of.
    """
    db = request.app.state.db
    current_user_id = user_id

    try:
        current_user_uuid = uuid.UUID(current_user_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid user ID format.")

    async with db.pool.acquire() as conn:
        query = """
        SELECT json_build_object(
            'id', pg.id,
            'name', pg.name,
            'ownerId', pg.owner_id,
            'chain', pg.chain_data,
            'isActive', pg.is_active,
            'created_at', TO_CHAR(pg.created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US'),
            'updated_at', TO_CHAR(pg.updated_at, 'YYYY-MM-DD"T"HH24:MI:SS.US'),
            'members', json_agg(
                json_build_object(
                    'uid', u.accountid,
                    'username', u.username, 
                    'displayName', ud.displayname,
                    'role', gm.role
                )
            )
        ) AS group_data
        FROM production_groups pg
        INNER JOIN group_members gm ON pg.id = gm.group_id
        INNER JOIN users u ON gm.user_id = u.accountid 
        LEFT JOIN users_data ud ON u.userdataid = ud.userid
        WHERE EXISTS (
            SELECT 1 
            FROM group_members 
            WHERE group_id = pg.id AND user_id = $1
        ) 
        GROUP BY pg.id, pg.name, pg.owner_id, pg.chain_data, pg.is_active, pg.created_at, pg.updated_at
        ORDER BY pg.name 
        """

        results = await conn.fetch(query, current_user_uuid)

        if not results:
            return []

        group_list = [json.loads(record["group_data"]) for record in results]

        return group_list


@group_router.get("/api/groups/{group_id}/full_sync", response_model=FullGroupData)
async def get_full_group_data(group_id: str, request: Request):
    """
    The initial sync endpoint. Returns ALL group data and the entire Chain object.
    """
    db = request.app.state.db
    current_user_id = "CURRENT_AUTHENTICATED_USER_ID"

    async with db.pool.acquire() as conn:
        # 1. Fetch group data
        group_record = await conn.fetchrow(
            """
            SELECT id, name, owner_id, chain, members
            FROM groups 
            WHERE id = $1 
            AND members @> $2::jsonb 
            """,
            group_id,
            json.dumps([{"uid": current_user_id}]),
        )

        if not group_record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Group not found or user is not a member.",
            )

    # 2. Construct the response structure
    group_data = {
        "id": str(group_record["id"]),
        "name": group_record["name"],
        "ownerId": str(group_record["ownerId"]),
        "members": group_record["members"],
        "isActive": True,
    }

    chain_data = group_record["chain"]

    return {"group": group_data, "chain": chain_data}


@group_router.post("", status_code=status.HTTP_201_CREATED)
async def create_group(group_data: Group, request: Request):
    """
    Creates a new production group and automatically enrolls the creator as 'owner'.
    """
    db = request.app.state.db

    try:
        owner_uuid = uuid.UUID(group_data.ownerId)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid owner ID format.")

    serialized_chain_data = json.dumps(group_data.chain)

    async with db.pool.acquire() as conn:
        # Use a transaction to ensure both inserts happen or neither does
        async with conn.transaction():
            # 1. Insert into production_groups and retrieve the full immutable record
            new_group_record = await conn.fetchrow(
                """
                INSERT INTO production_groups (name, owner_id, chain_data)
                VALUES ($1, $2, $3::jsonb) 
                RETURNING * -- Returns the ENTIRE ROW
                """,
                group_data.name,
                owner_uuid,
                serialized_chain_data,
            )

            if not new_group_record:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to retrieve new group data after insert.",
                )

            new_group_dict = dict(new_group_record)
            new_group_dict["ownerId"] = new_group_dict["owner_id"]
            del new_group_dict["owner_id"]

            # 2. Insert into group_members (setting creator as 'owner')
            await conn.execute(
                """
                INSERT INTO group_members (group_id, user_id, role)
                VALUES ($1, $2, 'owner')
                """,
                new_group_dict["id"],
                owner_uuid,
            )

            # 3. Fetch owner display name
            owner_info_record = await conn.fetchrow(  # Rename to avoid confusion
                """
                SELECT ud.displayname, u.username FROM users u 
                LEFT JOIN users_data ud ON u.userdataid = ud.userid 
                WHERE u.accountid = $1;
                """,
                owner_uuid,
            )

            if not owner_info_record and not group_data.ownerDisplayName:
                # Should not happen, but safe check
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Owner data not found.",
                )

            if isinstance(new_group_dict.get("chain_data"), str):
                try:
                    new_group_dict["chain_data"] = json.loads(new_group_dict["chain_data"])
                except json.JSONDecodeError:
                    new_group_dict["chain_data"] = {}

            # 4. Construct the members list and add it to the mutable dictionary
            new_group_dict["members"] = [
                {
                    "uid": owner_uuid,
                    "displayName": owner_info_record["displayname"]
                    or group_data.ownerDisplayName,  # Use 'displayName' for frontend consistency
                    "username": owner_info_record[
                        "username"
                    ],  # Use actual username if available, or fall back to displayname
                    "role": "owner",
                }
            ]

    # 5. Return the full dictionary with the added members key
    return {"message": "Group created successfully.", "group": new_group_dict}


@group_router.delete("/{group_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_group(group_id: str, delete_data: GroupDeleteModel, request: Request):
    """
    Deletes an existing production group.
    Authorization: Requires 'owner' role.
    """
    db = request.app.state.db

    # Check if IDs are valid UUIDs
    try:
        group_uuid = uuid.UUID(group_id)
        user_uuid = uuid.UUID(delete_data.user_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid ID format.")

    # 1. Authorization: Verify user is the 'owner'
    async with db.pool.acquire() as conn:
        user_role = await get_user_role(conn, group_id, delete_data.user_id)

        if user_role != "owner":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only the group owner can delete the group.",
            )

        # 2. Delete the group (ON DELETE CASCADE will handle group_members entries)
        deleted_count = await conn.execute(
            """
            DELETE FROM production_groups 
            WHERE id = $1
            """,
            group_uuid,
        )

        # check if DELETE affected any rows
        if deleted_count.endswith(" 0"):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Group with ID {group_id} not found.",
            )

    return {}


# not yet used all is done with ws
@group_router.post("/{group_id}/save", status_code=status.HTTP_200_OK)
async def save_group_chain_data(group_id: str, update_data: ChainUpdateModel, request: Request):
    """
    Saves/updates the entire production chain data (nodes/links) for a group.

    Authorization: Requires 'owner' or 'editor' role.
    """
    db = request.app.state.db

    # Convert IDs to UUID objects once for the database operations
    try:
        group_uuid = uuid.UUID(group_id)
        user_uuid = uuid.UUID(update_data.user_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid group ID or user ID format.",
        )

    # 1. Authorization: Verify user has 'editor' or 'owner' permission
    async with db.pool.acquire() as conn:
        user_role = await get_user_role(conn, group_id, update_data.user_id)

        if user_role not in ["owner", "editor"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You must be an owner or editor to modify the group's chain data.",
            )

        # 2. Perform the UPDATE operation
        try:
            query = """
            UPDATE production_groups 
            SET chain_data = $1, updated_at = NOW() 
            WHERE id = $2 
            RETURNING id
            """

            updated_group_id = await conn.fetchval(query, update_data.chain_data, group_uuid)

            if not updated_group_id:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Group with ID {group_id} not found.",
                )

        except Exception as e:
            print(f"Database error during group save: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to save group chain data.",
            )

    return {"message": "Group chain data saved successfully.", "group_id": group_id}
