import logging
from typing import Any, Dict

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
        existing_record = await conn.fetch_rows(
            f"SELECT xata_id FROM {TABLE_NAME} WHERE userid = $1;",
            record_id
        )

        if existing_record:
            # Filter the incoming data (rework data_converter does this)
            update_data = {
                'subscriptionlevel': user_data.get("subscriptionlevel"),
                'subscriptionexpiry': user_data.get("subscriptionexpiry"),
                'preferredlocale': user_data.get("preferredlocale"),
                'highesttier': user_data.get("highesttier"),
                'ispayinguser': user_data.get("ispayinguser"),
                'ismuted': user_data.get("ismuted"),
            }
            # Remove any keys that are None to avoid updating with null values.
            update_data = {k: v for k, v in update_data.items() if v is not None}
            
            # Construct the update query dynamically from the filtered dictionary.
            update_fields = ", ".join([f"{key} = ${i+2}" for i, key in enumerate(update_data.keys())])
            update_query = f"UPDATE {TABLE_NAME} SET {update_fields} WHERE userid = $1;"
            
            await conn.execute(update_query, record_id, *update_data.values())
            
            logger.info(f"Updated record '{record_id}' in '{TABLE_NAME}'.")
            return {"success": True, "message": f"Record '{record_id}' updated."}
        else:
            # Record does not exist, so insert a new one.
            # Use the original `user_data` for the insert.
            keys = ", ".join(user_data.keys())
            values_placeholders = ", ".join([f'${i+1}' for i in range(len(user_data))])
            insert_query = f"INSERT INTO {TABLE_NAME} ({keys}) VALUES ({values_placeholders}) RETURNING userid;"
            
            inserted_userid = await conn.fetch_one(insert_query, *user_data.values())

            if not inserted_userid:
                raise Exception("Insert operation returned no ID.")

            logger.info(f"Inserted new record '{inserted_userid}' into '{TABLE_NAME}'.")
            
            # Step 3: Update the 'users' table with the new userdataid
            main_user_id = payload["userId"]
            update_user_query = "UPDATE users SET userdataid = $1 WHERE xata_id = $2;"
            await conn.execute(update_user_query, inserted_userid['userid'], main_user_id)

            logger.info(f"Updated user '{main_user_id}' with userdataid '{inserted_userid}'.")
            return {"success": True, "message": f"Record '{inserted_userid}' inserted."}

    except Exception as e:
        logger.error(f"Error processing 'USER_DATA' message: {e}")
        raise