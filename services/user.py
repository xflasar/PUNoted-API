from fastapi import HTTPException

from models.user import UserSettingsUpdate


async def get_user_settings(db, user_id: str):
    async with db.pool.acquire() as conn:
        query = """
            SELECT u.username, COALESCE(ud.displayname, u.displayname) AS displayname, 
                   cd.companyname, cd.companycode, u.isverified, u.is_synchronized, 
                   u.fioapikey
            FROM users u
            LEFT JOIN users_data ud ON ud.userid = u.userdataid
            LEFT JOIN company_data cd ON cd.userdataid = ud.userid
            WHERE u.accountid = $1
        """
        user = await conn.fetchrow(query, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        return {
            "username": user["username"],
            "displayName": user.get("displayname"),
            "companyName": user.get("companyname"),
            "companyCode": user.get("companycode"),
            "isVerified": user["isverified"],
            "isSynchronized": user["is_synchronized"],
            "fioApiKey": user.get("fioapikey"),
        }


async def update_user_settings(db, user_id: str, update: UserSettingsUpdate):
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            # Fetch existing user
            existing = await conn.fetchrow("SELECT is_synchronized FROM users WHERE accountid = $1", user_id)
            if not existing:
                raise HTTPException(status_code=404, detail="User not found")

            # Update fio_api_key and data_api_keys
            await conn.execute(
                """
                UPDATE users
                SET fioapikey = COALESCE($2, fioapikey)
                WHERE accountid = $1
                """,
                user_id,
                update.fioApiKey,
            )

            # Optionally update displayName if not synchronized
            if update.displayName and not existing["is_synchronized"]:
                await conn.execute(
                    """
                    UPDATE users
                    SET displayname = $1
                    WHERE accountid = $2
                    """,
                    update.displayName,
                    user_id,
                )
