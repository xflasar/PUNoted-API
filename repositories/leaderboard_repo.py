import logging

logger = logging.getLogger(__name__)

SQL_GET_CURRENT_PRODUCTION = """
WITH DailyCounts AS (
    SELECT DATE(record_date) AS snap_date, COUNT(1) AS item_count
    FROM leaderboard_history
    WHERE category = 'PRODUCTION' AND time_range = 'DAYS_7'
    GROUP BY DATE(record_date)
),
MaxCount AS (
    SELECT MAX(item_count) AS max_items FROM DailyCounts
),
ValidDays AS (
    SELECT snap_date
    FROM DailyCounts
    CROSS JOIN MaxCount
    WHERE item_count >= (max_items * 0.95)
),
LeaderboardRef AS (
    -- Safely find the latest time only among days with complete snapshots
    -- FIXME: Not working for some reason, seems to be including partial days. Might need to debug the DailyCounts CTE to ensure it's counting correctly.
    SELECT MAX(record_date) AS latest_time 
    FROM leaderboard_history
    WHERE category = 'PRODUCTION' 
      AND time_range = 'DAYS_7'
      AND DATE(record_date) IN (SELECT snap_date FROM ValidDays)
),
SevenDayCXAvg AS (
    SELECT 
        SPLIT_PART(ticker, '.', 1) AS material_ticker,
        AVG(askprice) AS avg_price
    FROM cx_brokers_history
    CROSS JOIN LeaderboardRef
    WHERE snapshot_at >= LeaderboardRef.latest_time - INTERVAL '7 days'
      AND snapshot_at <= LeaderboardRef.latest_time
    GROUP BY SPLIT_PART(ticker, '.', 1)
),
RankedProduction AS (
    SELECT 
        l.record_date,
        l.material_ticker,
        l.company_id,
        l.score,
        l.rank
    FROM leaderboard_history l
    CROSS JOIN LeaderboardRef
    WHERE l.record_date = LeaderboardRef.latest_time 
      AND l.category = 'PRODUCTION' 
      AND l.time_range = 'DAYS_7'
)
SELECT 
    rp.record_date,
    rp.material_ticker,
    pud.company_code,
    pud.company_name,
    rp.score,
    COALESCE(cx.avg_price, 0) AS avg_price_7d,
    (rp.score * COALESCE(cx.avg_price, 0)) AS estimated_value_7d
FROM RankedProduction rp
LEFT JOIN SevenDayCXAvg cx ON cx.material_ticker = rp.material_ticker
LEFT JOIN public_users_data pud ON pud.company_id = rp.company_id
WHERE rp.rank <= 25
ORDER BY rp.material_ticker ASC, rp.rank ASC;
"""

SQL_GET_HISTORY_PRODUCTION = """
WITH DailyCounts AS (
    SELECT DATE(record_date) AS snap_date, COUNT(1) AS item_count
    FROM leaderboard_history
    WHERE category = 'PRODUCTION' AND time_range = 'DAYS_7'
    GROUP BY DATE(record_date)
),
MaxCount AS (
    SELECT MAX(item_count) AS max_items FROM DailyCounts
),
ValidDays AS (
    SELECT snap_date
    FROM DailyCounts
    CROSS JOIN MaxCount
    WHERE item_count >= (max_items * 0.95)
),
LeaderboardRef AS (
    SELECT MAX(record_date) AS latest_time 
    FROM leaderboard_history
    WHERE category = 'PRODUCTION' 
      AND time_range = 'DAYS_7'
      AND DATE(record_date) IN (SELECT snap_date FROM ValidDays)
),
CurrentTop25 AS (
    SELECT material_ticker, company_id
    FROM leaderboard_history
    CROSS JOIN LeaderboardRef
    WHERE record_date = LeaderboardRef.latest_time 
      AND category = 'PRODUCTION' 
      AND time_range = 'DAYS_7'
      AND rank <= 25
)
SELECT 
    h.material_ticker,
    DATE(h.record_date) AS history_date,
    pud.company_code,
    h.score
FROM leaderboard_history h
INNER JOIN CurrentTop25 c 
    ON h.material_ticker = c.material_ticker 
    AND h.company_id = c.company_id
LEFT JOIN public_users_data pud ON h.company_id = pud.company_id
CROSS JOIN LeaderboardRef
-- Join against ValidDays to instantly strip corrupted/partial days from the final output
INNER JOIN ValidDays vd ON DATE(h.record_date) = vd.snap_date
WHERE h.category = 'PRODUCTION' 
  AND h.time_range = 'DAYS_7'
  AND h.record_date >= LeaderboardRef.latest_time - INTERVAL '30 days'
ORDER BY h.material_ticker ASC, history_date ASC;
"""

async def fetch_current_top_25(db):
    try:
        async with db.pool.acquire() as conn:
            return await conn.fetch(SQL_GET_CURRENT_PRODUCTION)
    except Exception as e:
        logger.error(f"DB Error fetching current top 25: {e}")
        raise e

async def fetch_top_25_history(db):
    try:
        async with db.pool.acquire() as conn:
            return await conn.fetch(SQL_GET_HISTORY_PRODUCTION)
    except Exception as e:
        logger.error(f"DB Error fetching history for top 25: {e}")
        raise e
