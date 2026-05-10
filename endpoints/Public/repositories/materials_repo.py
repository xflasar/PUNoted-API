import logging

logger = logging.getLogger(__name__)

SQL_GET_MATERIAL_DATA = """
  SELECT 
      m.ticker as "Ticker", 
      m.name as "Name", 
      mc.name as "Category", 
      m.weight as "Weight", 
      m.volume as "Volume"
  FROM materials m
  INNER JOIN material_categories mc ON mc.id = m.category
  ORDER BY ticker ASC;
"""

async def fetch_materials_data(db) -> list:
    """
    Executes the market data query and returns a list of database records.
    """
    try:
        async with db.pool.acquire() as con:
            await con.execute("SET lock_timeout = '10s';")
            records = await con.fetch(SQL_GET_MATERIAL_DATA)
            return records
    except Exception as e:
        logger.error(f"Database error fetching market data: {e}", exc_info=True)
        raise
