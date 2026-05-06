import logging

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