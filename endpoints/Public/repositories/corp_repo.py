import logging
from typing import Optional, List

logger = logging.getLogger(__name__)

SQL_GET_CORP_CPRICES = """
SELECT
    mp.ticker,
    mp.price
    FROM material_prices mp;
"""

async def fetch_corp_prices(db) -> list:
    """
    Executes the market data query and returns a list of database records.
    """
    try:
        async with db.pool.acquire() as con:
            await con.execute("SET lock_timeout = '10s';")
            records = await con.fetch(SQL_GET_CORP_CPRICES)
            return records
    except Exception as e:
        logger.error(f"Database error fetching market data: {e}", exc_info=True)
        raise

SQL_GET_CORP_MEMBERS = """
WITH TargetCorp AS (
    SELECT corporationid 
    FROM corporation_shareholders cs
    INNER JOIN users u ON cs.userid = u.userdataid
    WHERE u.accountid = $1
    LIMIT 1
)
SELECT 
    c.name,
    c.code,
    cs.companycode,
    cs.companyname
FROM corporation_shareholders cs
INNER JOIN TargetCorp tc ON cs.corporationid = tc.corporationid
INNER JOIN corporations c ON c.id = tc.corporationid
ORDER BY cs.companyname ASC;
"""

async def fetch_corp_members(db, userid: str) -> list:
    """
    Fetches all members belonging to the same corporation as the provided userid.
    """
    try:
        async with db.pool.acquire() as con:
            await con.execute("SET lock_timeout = '10s';")
            records = await con.fetch(SQL_GET_CORP_MEMBERS, userid)
            return records
    except Exception as e:
        logger.error(f"Database error fetching corporation members for {userid}: {e}", exc_info=True)
        raise

async def resolve_corp_id(db, input_id: str) -> str:
    row = await db.fetch_one("SELECT id FROM corporations WHERE code = $1 OR id = $1", input_id)
    if row:
        return row["id"]
    return input_id

async def is_corp_admin(db, user_id: str, corp_id: str) -> bool:
    row = await db.fetch_one("""
        SELECT c.founder, c.officers, ud.displayname, cs.companycode
        FROM corporations c
        LEFT JOIN corporation_shareholders cs ON c.id = cs.corporationid
        LEFT JOIN users usr ON usr.userdataid = cs.userid AND usr.accountid = $2
        LEFT JOIN users_data ud ON ud.userid = cs.userid
        WHERE (c.id = $1 OR c.code = $1)
        LIMIT 1;
    """, corp_id, user_id)

    if not row:
        return False

    founder = row["founder"]
    officers = row["officers"] or []

    if founder is None and not officers:
        return True

    user_id_str = str(user_id)
    if founder == user_id_str or user_id_str in officers:
        return True

    display_name = row["displayname"]
    if display_name and (founder == display_name or display_name in officers):
        return True

    company_code = row["companycode"]
    if company_code and (founder == company_code or company_code in officers):
        return True

    return False

async def fetch_ship_presets(db, resolved_corp_id: str, user_id: str = None) -> list:
    if user_id:
        return await db.fetch_rows("""
            SELECT id, corporation_id, name, price, price_corp, parts, is_admin_preset, created_by, created_at
            FROM ship_build_presets
            WHERE corporation_id = $1 AND (is_admin_preset = TRUE OR created_by = $2)
        """, resolved_corp_id, user_id)
    else:
        return await db.fetch_rows("""
            SELECT id, corporation_id, name, price, price_corp, parts, is_admin_preset, created_by, created_at
            FROM ship_build_presets
            WHERE corporation_id = $1 AND is_admin_preset = TRUE
        """, resolved_corp_id)

async def fetch_ship_orders(db, resolved_corp_id: str) -> list:
    return await db.fetch_rows("""
        SELECT o.*, c.code AS corporation_code 
        FROM corp_ship_orders o
        JOIN corporations c ON c.id = o.corporation_id
        WHERE o.corporation_id = $1 
        ORDER BY o.wait_time_days ASC
    """, resolved_corp_id)

async def fetch_ship_order_by_pin(db, resolved_corp_id: str, pin: str) -> Optional[dict]:
    return await db.fetch_one("""
        SELECT o.*, c.code AS corporation_code 
        FROM corp_ship_orders o
        JOIN corporations c ON c.id = o.corporation_id
        WHERE o.corporation_id = $1 AND o.guest_pin = $2 AND o.status = 'QUEUED'
    """, resolved_corp_id, pin)
