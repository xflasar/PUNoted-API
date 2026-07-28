import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

"""
    Needs rewriting to use transaction
"""


async def handle_user_data_message(conn, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Asynchronously processes a user data message, updating an existing record
    with specific fields or inserting a new one, all within a single transaction.
    """
    TABLE_NAME = "users_data"

    if not payload.get("data") or not isinstance(payload["data"], list) or not payload["data"][0]:
        return {"success": False, "message": "Invalid payload format."}

    user_data = payload["data"][0]
    record_id = user_data.get("userid")

    if not record_id:
        return {"success": False, "message": "User ID is missing from payload."}

    try:
        # Atomic UPSERT to avoid concurrency race conditions (UniqueViolationError)
        keys = list(user_data.keys())
        keys_str = ", ".join(keys)
        values_placeholders = ", ".join([f"${i + 1}" for i in range(len(keys))])
        set_clause = ", ".join([f"{k} = EXCLUDED.{k}" for k in keys if k != "userid"])

        upsert_query = f"""
            INSERT INTO {TABLE_NAME} ({keys_str})
            VALUES ({values_placeholders})
            ON CONFLICT (userid) DO UPDATE SET
                {set_clause}
            RETURNING userid;
        """

        inserted_userid = await conn.fetch_one(upsert_query, *user_data.values())

        if not inserted_userid:
            raise Exception("Upsert operation returned no ID.")

        logger.debug(f"Upserted record '{inserted_userid['userid']}' into '{TABLE_NAME}'.")

        # Step 3: Update the 'users' table with the new userdataid
        main_user_id = payload["userId"]
        update_user_query = "UPDATE users SET userdataid = $1, is_synchronized = TRUE WHERE accountid = $2;"
        await conn.execute(update_user_query, inserted_userid["userid"], main_user_id)

        logger.debug(f"Updated user '{main_user_id}' with userdataid '{inserted_userid['userid']}'.")
        return {"success": True, "message": f"Record '{inserted_userid['userid']}' upserted."}

    except Exception as e:
        logger.error(f"Error processing 'USER_DATA' message: {e}")
        raise

async def handle_public_user_data_message(conn, payload: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Asynchronously processes a list of public user data messages using a highly optimized 
    batch upsert to insert new records or update existing ones in a single DB transaction.
    """
    con_payload = payload["data"]

    if not con_payload or not isinstance(con_payload, list) or not con_payload[0]:
        return {"success": False, "message": "Invalid payload format."}

    SQL_UPSERT_PUBLIC_USERS = """
    INSERT INTO public_users_data (
        id, username, company_id, company_name, company_code, 
        subscription_level, highest_tier, pioneer, moderator, 
        team, translator, active_days_per_week, created_timestamp, gifts
    ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::jsonb
    )
    ON CONFLICT (id) DO UPDATE SET
        username = EXCLUDED.username,
        company_id = EXCLUDED.company_id,
        company_name = EXCLUDED.company_name,
        company_code = EXCLUDED.company_code,
        subscription_level = EXCLUDED.subscription_level,
        highest_tier = EXCLUDED.highest_tier,
        pioneer = EXCLUDED.pioneer,
        moderator = EXCLUDED.moderator,
        team = EXCLUDED.team,
        translator = EXCLUDED.translator,
        active_days_per_week = EXCLUDED.active_days_per_week,
        created_timestamp = EXCLUDED.created_timestamp,
        gifts = EXCLUDED.gifts;
    """

    try:
        values = [
            (
                r.get("id"), r.get("username"), r.get("company_id"), r.get("company_name"),
                r.get("company_code"), r.get("subscription_level"), r.get("highest_tier"),
                r.get("pioneer"), r.get("moderator"), r.get("team"), r.get("translator"),
                r.get("active_days_per_week"), r.get("created_timestamp"), r.get("gifts")
            )
            for r in con_payload if r.get("id") # Only process records that have an ID
        ]

        if not values:
            logger.warning("No valid user IDs found in payload. Skipping database execution.")
            return {"success": False, "message": "No valid records with IDs found."}

        # executemany sends the entire list of tuples to Postgres in a single operation
        await conn.executemany(SQL_UPSERT_PUBLIC_USERS, values)

        logger.debug(f"Successfully batch upserted {len(values)} public user records.")
        return {"success": True, "message": f"Processed {len(values)} records."}

    except Exception as e:
        logger.error(f"Error processing 'PUBLIC_USER_DATA' message: {e}", exc_info=True)
        raise
