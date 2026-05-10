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
        existing_record = await conn.fetch_rows(f"SELECT userid FROM {TABLE_NAME} WHERE userid = $1;", record_id)

        if existing_record:
            # Filter the incoming data (rework data_converter does this)
            update_data = {
                "subscriptionlevel": user_data.get("subscriptionlevel"),
                "subscriptionexpiry": user_data.get("subscriptionexpiry"),
                "preferredlocale": user_data.get("preferredlocale"),
                "highesttier": user_data.get("highesttier"),
                "ispayinguser": user_data.get("ispayinguser"),
                "ismuted": user_data.get("ismuted"),
            }
            # Remove any keys that are None to avoid updating with null values.
            update_data = {k: v for k, v in update_data.items() if v is not None}

            # Construct the update query dynamically from the filtered dictionary.
            update_fields = ", ".join([f"{key} = ${i + 2}" for i, key in enumerate(update_data.keys())])
            update_query = f"UPDATE {TABLE_NAME} SET {update_fields} WHERE userid = $1;"

            await conn.execute(update_query, record_id, *update_data.values())

            logger.debug(f"Updated record '{record_id}' in '{TABLE_NAME}'.")
            return {"success": True, "message": f"Record '{record_id}' updated."}
        else:
            # Record does not exist, so insert a new one.
            # Use the original `user_data` for the insert.
            keys = ", ".join(user_data.keys())
            values_placeholders = ", ".join([f"${i + 1}" for i in range(len(user_data))])
            insert_query = f"INSERT INTO {TABLE_NAME} ({keys}) VALUES ({values_placeholders}) RETURNING userid;"

            inserted_userid = await conn.fetch_one(insert_query, *user_data.values())

            if not inserted_userid:
                raise Exception("Insert operation returned no ID.")

            logger.debug(f"Inserted new record '{inserted_userid}' into '{TABLE_NAME}'.")

            # Step 3: Update the 'users' table with the new userdataid
            main_user_id = payload["userId"]
            update_user_query = "UPDATE users SET userdataid = $1, is_synchronized = TRUE WHERE accountid = $2;"
            await conn.execute(update_user_query, inserted_userid["userid"], main_user_id)

            logger.debug(f"Updated user '{main_user_id}' with userdataid '{inserted_userid}'.")
            return {"success": True, "message": f"Record '{inserted_userid}' inserted."}

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
