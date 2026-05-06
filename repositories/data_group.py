import json
from asyncpg import Connection

async def repo_create_group(conn: Connection, owner_id: str, name: str, access_key: str, description: str = None) -> str:
    sql = """
        INSERT INTO data_sharing_groups (owner_id, name, description, access_key)
        VALUES ($1, $2, $3, $4)
        RETURNING id
    """
    return str(await conn.fetchval(sql, owner_id, name, description, access_key))

async def repo_delete_group(conn: Connection, group_id: str):
    """Deletes a group. Database CASCADE will remove members automatically."""
    sql = "DELETE FROM data_sharing_groups WHERE id = $1"
    await conn.execute(sql, group_id)

async def repo_add_member(conn: Connection, group_id: str, user_id: str, status: str, suffix: str = None, can_read: bool = False):
    sql = """
        INSERT INTO data_group_members (group_id, user_id, status, personal_suffix, can_read_data)
        VALUES ($1, $2, $3, $4, $5)
    """
    await conn.execute(sql, group_id, user_id, status, suffix, can_read)

async def repo_get_group_by_id(conn: Connection, group_id: str):
    return await conn.fetchrow("SELECT * FROM data_sharing_groups WHERE id = $1", group_id)

async def repo_get_member(conn: Connection, group_id: str, user_id: str):
    return await conn.fetchrow(
        "SELECT * FROM data_group_members WHERE group_id = $1 AND user_id = $2", 
        group_id, user_id
    )

async def repo_get_user_by_username(conn: Connection, username: str):
    return await conn.fetchrow("SELECT accountid FROM users u INNER JOIN users_data ud ON ud.userid = u.userdataid WHERE ud.displayname = $1", username)

async def repo_get_user_by_id(conn: Connection, user_id: str):
    return await conn.fetchrow("SELECT * FROM users WHERE accountid = $1", user_id)

async def repo_update_member_acceptance(conn: Connection, group_id: str, user_id: str, suffix: str, permissions: list):
    sql = """
        UPDATE data_group_members 
        SET status = 'ACCEPTED', 
            personal_suffix = $1, 
            granted_permissions = $2::jsonb,
            joined_at = NOW()
        WHERE group_id = $3 AND user_id = $4
    """
    await conn.execute(sql, suffix, json.dumps(permissions), group_id, user_id)

async def repo_remove_member(conn: Connection, group_id: str, user_id: str):
    """Removes a member from the group (used for both Leave and Kick)."""
    sql = "DELETE FROM data_group_members WHERE group_id = $1 AND user_id = $2"
    await conn.execute(sql, group_id, user_id)

async def repo_create_token(conn: Connection, owner_id: str, group_id: str, token_hash: str, prefix: str, label: str):
    sql = """
        INSERT INTO user_api_tokens (user_id, group_id, token_hash, token_prefix, label, permissions)
        VALUES ($1, $2, $3, $4, $5, '["group:access"]'::jsonb)
    """
    await conn.execute(sql, owner_id, group_id, token_hash, prefix, label)

async def repo_set_member_permission(conn: Connection, group_id: str, user_id: str, can_read: bool):
    sql = """
        UPDATE data_group_members SET can_read_data = $1
        WHERE group_id = $2 AND user_id = $3
    """
    await conn.execute(sql, can_read, group_id, user_id)

async def repo_update_member_shares(conn: Connection, group_id: str, user_id: str, permissions: list):
    sql = """
        UPDATE data_group_members 
        SET granted_permissions = $1::jsonb
        WHERE group_id = $2 AND user_id = $3
    """
    await conn.execute(sql, json.dumps(permissions), group_id, user_id)

# ==============================================================================
# NEW FUNCTIONS (Required for Frontend to list groups/members)
# ==============================================================================

async def repo_list_groups_for_user(conn: Connection, user_id: str):
    """Now returns the access_key and personal_suffix so Frontend can build the token."""
    sql = """
        SELECT 
            g.id, g.name, g.description, g.owner_id, g.access_key,
            m.status as my_status, m.personal_suffix,
            (SELECT COUNT(*) FROM data_group_members gm WHERE gm.group_id = g.id) as members_count
        FROM data_group_members m
        JOIN data_sharing_groups g ON g.id = m.group_id
        WHERE m.user_id = $1
    """
    return await conn.fetch(sql, user_id)

async def repo_list_members(conn: Connection, group_id: str):
    """
    Fetches all members of a specific group + their username.
    """
    sql = """
        SELECT m.user_id, u.username, m.status, m.can_read_data, m.granted_permissions
        FROM data_group_members m
        JOIN users u ON u.accountid = m.user_id
        WHERE m.group_id = $1
    """
    return await conn.fetch(sql, group_id)

async def repo_search_sync_users(conn: Connection, query: str):
    """Finds users with is_synchronized=True matching the query."""
    sql = """
        SELECT ud.displayname FROM users u
        INNER JOIN users_data ud ON ud.userid = u.userdataid
        WHERE u.is_synchronized = TRUE 
        AND ud.displayname ILIKE $1 
        LIMIT 10
    """
    # ILIKE is case-insensitive
    return await conn.fetch(sql, f"%{query}%")