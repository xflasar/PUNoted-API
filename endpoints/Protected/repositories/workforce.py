# endpoints/Protected/repositories/workforce.py
from typing import List, Optional

# --- QUERY 1: JSON with Filters (Grouped by User) ---
SQL_FETCH_WORKFORCE_JSON = """
WITH TargetUsers AS (
    SELECT u.username, u.userdataid
    FROM users u
    WHERE u.username = ANY($1::text[]) -- Filter by verified list
),
SiteWorkforce AS (
    SELECT
        tu.username,
        jsonb_build_object(
            'PlanetId', s.addressplanetid,
            'PlanetNaturalId', p.naturalid,
            'PlanetName', p.name,
            'SiteId', w.siteid,
            'UserNameSubmitted', tu.username,
            'Timestamp', w.xata_updatedat,
            'Workforces', COALESCE(jsonb_agg(
                jsonb_build_object(
                    'WorkforceTypeName', w.level,
                    'Population', w.population,
                    'Reserve', w.reserve,
                    'Capacity', w.capacity,
                    'Required', w.required,
                    'Satisfaction', w.satisfaction,
                    'WorkforceNeeds', COALESCE(
                        (
                            SELECT jsonb_agg(
                                jsonb_build_object(
                                    'Category', wn.category,
                                    'Essential', wn.essential,
                                    'MaterialId', wn.materialid,
                                    'MaterialTicker', m.ticker,
                                    'Satisfaction', wn.satisfaction,
                                    'UnitsPerInterval', wn.unitsperinterval,
                                    'UnitsPerOneHundred', wn.unitsper100
                                ) ORDER BY wn.category
                            )
                            FROM workforce_needs wn
                            INNER JOIN materials m ON m.materialid = wn.materialid
                            WHERE wn.workforceid = w.workforceid
                        ), '[]'::jsonb
                    )
                ) ORDER BY w.level
            ), '[]'::jsonb)
        ) as site_json
    FROM workforces w
    JOIN TargetUsers tu ON tu.userdataid = w.userid
    JOIN sites s ON s.siteid = w.siteid
    JOIN planets p ON p.planetid = s.addressplanetid
    WHERE 
        ($2::text IS NULL OR p.name ILIKE $2 OR p.naturalid ILIKE $2)
    GROUP BY tu.username, s.addressplanetid, p.naturalid, p.name, w.siteid, w.userid, w.xata_updatedat
)
SELECT
    COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'Username', sub.username,
                'Workforce', sub.sites_list
            )
        ), '[]'::jsonb
    )
FROM (
    SELECT username, jsonb_agg(site_json) as sites_list
    FROM SiteWorkforce
    GROUP BY username
) sub;
"""

# --- QUERY 2: Flat CSV with Filters (Multi-User) ---
SQL_FETCH_WORKFORCE_FLAT = """
WITH TargetUsers AS (
    SELECT u.username, u.userdataid
    FROM users u
    WHERE u.username = ANY($1::text[])
)
SELECT 
    p.name as planetname,
    p.naturalid as planetnaturalid,
    w.siteid,
    w.level as workforce_type,
    w.population,
    wn.category,
    m.ticker,
    wn.essential,
    wn.satisfaction as need_satisfaction,
    wn.unitsperinterval,
    tu.username
FROM workforces w
JOIN TargetUsers tu ON tu.userdataid = w.userid
LEFT JOIN workforce_needs wn ON wn.workforceid = w.workforceid
JOIN sites s ON s.siteid = w.siteid
JOIN planets p ON p.planetid = s.addressplanetid
LEFT JOIN materials m ON m.materialid = wn.materialid
WHERE 
    ($2::text IS NULL OR p.name ILIKE $2 OR p.naturalid ILIKE $2)
ORDER BY tu.username, p.name, p.naturalid, w.level, wn.category;
"""

async def fetch_workforce_json(conn, usernames_list: List[str], location: Optional[str] = None):
    # Pass arguments: $1=list, $2=location_filter
    p_location = f"%{location}%" if location else None
    result = await conn.fetchval(SQL_FETCH_WORKFORCE_JSON, usernames_list, p_location)
    return result or "[]"

async def fetch_workforce_flat(conn, usernames_list: List[str], location: Optional[str] = None):
    p_location = f"%{location}%" if location else None
    return await conn.fetch(SQL_FETCH_WORKFORCE_FLAT, usernames_list, p_location)