import asyncio
import logging
from typing import Optional

from db import Database

logger = logging.getLogger(__name__)

# GLOBAL VARIABLE TO HOLD THE ASYNCPG CONNECTION POOL FOR THE DISCORD BOT
DB_POOL = None


async def initialize_discord_db_pool(loop=None):
    """
    Initializes the dedicated asyncpg pool for the Discord bot's event loop.
    """
    global DB_POOL
    # 1. Create a NEW instance of the Database wrapper object
    db_instance = Database()

    # 2. Get the loop the bot is running on (should be passed as loop=bot.loop)
    current_loop = loop or asyncio.get_event_loop()

    # 3. Create the pool on that specific loop
    await db_instance.create_pool(loop=current_loop)

    # 4. Set the global pool variable for the db_handler to use
    DB_POOL = db_instance.pool

    logger.debug("Discord Bot Database pool successfully created.")


async def lookup_app_id_and_assign_user(link_id: str, discord_id: int) -> bool:
    """
    ASYNC POSTGRESQL BRIDGE for /link:
    Checks if the link_id is valid and associates the Discord ID with the
    application's user account ID.

    Returns True if successful.
    """
    if DB_POOL is None:
        logger.error("Database pool is not initialized!")
        return False

    try:
        await asyncio.sleep(0.1)
        if link_id.startswith("APP-"):
            logger.debug(f"User {discord_id} linked with app ID {link_id} (Simulated)")
            return True
        logger.warning(f"Failed linking attempt for ID: {link_id} (Simulated)")
        return False

    except Exception as e:
        logger.error(f"Error during link_account DB operation: {e}")
        return False


async def get_verification_code_by_server_link_code(
    server_link_code: str,
) -> Optional[str]:
    """
    ASYNC POSTGRESQL BRIDGE for /verify:
    Uses the server_link_code to find the associated application user ID and
    generates/retrieves the final 8-digit verification code.

    Returns a tuple of (verification_code: str, app_user_id: str) if the
    server_link_code is valid, or None otherwise.
    """
    if DB_POOL is None:
        logger.error("Database pool is not initialized!")
        return None

    try:
        async with DB_POOL.acquire() as conn:
            code = await conn.fetch(
                "SELECT code FROM user_verification_codes WHERE servercode=$1",
                server_link_code,
            )
            if code[0]:
                # Logic to return or generate a new code
                logger.debug(
                    f"Lookup successful for link code {server_link_code}. Generated verification code: {code[0]}"
                )
                return code[0].get("code")
            logger.warning(f"Verification lookup failed for server link code: {server_link_code}")
            print(code)
            return None

    except Exception as e:
        print(e)
        logger.error(f"Error during verify_code DB operation: {e}")
        return None
