from typing import Any, List

# --- QUERY 1: Fetch Flat Orders ---
SQL_FETCH_CORP_FLAT_ORDERS = """
WITH 
TargetUser AS (SELECT userdataid FROM users WHERE accountid = $1 LIMIT 1),
TargetCorp AS (
    SELECT cs.corporationid 
    FROM corporation_shareholders cs
    JOIN TargetUser tu ON cs.userid = tu.userdataid
    LIMIT 1
),
CorpSites AS (
    SELECT 
        s.siteid,
        u.userdataid,
        COALESCE(cs.companycode, u.username) as player_name,
        p.naturalid as location_name,
        CASE 
            WHEN ud.subscriptionlevel = 'PRO' AND ud.subscriptionexpiry > NOW() THEN TRUE 
            ELSE FALSE 
        END as is_accurate
    FROM TargetCorp tc
    JOIN corporation_shareholders cs ON cs.corporationid = tc.corporationid
    JOIN users_data ud ON ud.userid = cs.userid
    JOIN users u ON u.userdataid = ud.userid
    JOIN sites s ON s.userid = u.userdataid
    JOIN planets p ON p.planetid = s.addressplanetid
    LEFT JOIN user_global_settings ugs ON ugs.userid::uuid = u.accountid
    WHERE u.xata_updatedat > NOW() - INTERVAL '7 day'
      
      -- 1. Check Excluded Sites (FIXME: Seems to not work)
      -- We use COALESCE to handle NULLs safely (treating NULL as an empty list '[]')
      AND NOT EXISTS (
          SELECT 1 
          FROM jsonb_array_elements(COALESCE(ugs.internal_excluded_sites, '[]'::jsonb)) as ex 
          WHERE ex->>'siteId' = s.siteid
      )
      
      -- 2. Check Leased Sites
      -- Only checked if the row passed the first barrier
      AND NOT EXISTS (
          SELECT 1 
          FROM jsonb_array_elements(COALESCE(ugs.internal_leased_sites, '[]'::jsonb)) as ls 
          WHERE ls->>'siteId' = s.siteid
      )
)
SELECT 
    cs.siteid,
    cs.player_name,
    cs.location_name,
    cs.is_accurate,
    pl.productionlineid,
    pl.capacity,
    pl.condition,
    po.orderid,
    po.created,
    po.completion,
    po.duration as order_duration,
    po.recipeid
FROM CorpSites cs
JOIN site_production_lines pl ON pl.siteid = cs.siteid
JOIN site_production_line_orders po ON po.productionlineid = pl.productionlineid
WHERE (po.completion IS NULL OR po.completion > NOW());
"""

# --- SPLIT RECIPE QUERIES ---

# 1. Core Info
SQL_FETCH_RECIPES_CORE = """
WITH Targets AS (
    SELECT unnest($1::text[]) as t_id, unnest($2::text[]) as l_id
)
SELECT 
    r.productiontemplateid as recipe_id, 
    r.productionlineid as line_id,
    r.duration
FROM production_recipes r
JOIN Targets t ON r.productiontemplateid = t.t_id AND r.productionlineid = t.l_id
"""

# 2. Inputs
SQL_FETCH_RECIPE_INPUTS = """
WITH Targets AS (
    SELECT unnest($1::text[]) as t_id, unnest($2::text[]) as l_id
)
SELECT 
    i.productiontemplateid as recipe_id, 
    i.productionlineid as line_id,
    m.ticker, 
    i.factor
FROM production_recipe_input_factors i
JOIN Targets t ON i.productiontemplateid = t.t_id AND i.productionlineid = t.l_id
JOIN materials m ON m.materialid = i.materialid
"""

# 3. Outputs
SQL_FETCH_RECIPE_OUTPUTS = """
WITH Targets AS (
    SELECT unnest($1::text[]) as t_id, unnest($2::text[]) as l_id
)
SELECT 
    o.productiontemplateid as recipe_id, 
    o.productionlineid as line_id,
    m.ticker, 
    o.factor
FROM production_recipe_output_factors o
JOIN Targets t ON o.productiontemplateid = t.t_id AND o.productionlineid = t.l_id
JOIN materials m ON m.materialid = o.materialid
"""

SQL_FETCH_CORP_WORKFORCE = """
WITH 
TargetUser AS (SELECT userdataid FROM users WHERE accountid = $1 LIMIT 1),
TargetCorp AS (SELECT cs.corporationid FROM corporation_shareholders cs JOIN TargetUser tu ON cs.userid = tu.userdataid LIMIT 1),
CorpMembers AS (
    SELECT u.userdataid, COALESCE(cs.companycode, u.username) as player_name,
    CASE WHEN ud.subscriptionlevel = 'PRO' AND ud.subscriptionexpiry > NOW() THEN TRUE ELSE FALSE END as is_accurate
    FROM TargetCorp tc 
    JOIN corporation_shareholders cs ON cs.corporationid = tc.corporationid 
    JOIN users_data ud ON ud.userid = cs.userid 
    JOIN users u ON u.userdataid = ud.userid
),
-- Aggregate materials by User instead of Site
UserMaterialTotals AS (
    SELECT 
        cm.userdataid,
        m.ticker,
        SUM(wfn.unitsperinterval) as total_units
    FROM CorpMembers cm
    JOIN sites s ON s.userid = cm.userdataid
    JOIN workforces wf ON wf.siteid = s.siteid
    JOIN workforce_needs wfn ON wfn.workforceid = wf.workforceid
    JOIN materials m ON m.materialid = wfn.materialid
    GROUP BY cm.userdataid, m.ticker
)
SELECT 
    -- 1. Virtual Site ID: Use userdataid so it is unique and stable
    cm.userdataid as siteid,
    
    cm.player_name,
    cm.is_accurate,
    
    -- 2. Virtual Location Name
    'Workforce' as location_name,
    
    -- 3. Aggregated JSON List
    COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'ticker', umt.ticker,
                'unitsperinterval', umt.total_units
            ) 
            ORDER BY umt.ticker
        ) FILTER (WHERE umt.ticker IS NOT NULL), 
        '[]'::jsonb
    ) as needs
FROM CorpMembers cm
LEFT JOIN UserMaterialTotals umt ON umt.userdataid = cm.userdataid
GROUP BY cm.userdataid, cm.player_name, cm.is_accurate
ORDER BY cm.player_name;
"""


async def fetch_corp_flat_orders(conn: Any, user_id: str):
    return await conn.fetch(SQL_FETCH_CORP_FLAT_ORDERS, user_id)


async def fetch_recipe_components(conn: Any, template_ids: List[str], line_ids: List[str]):
    core = await conn.fetch(SQL_FETCH_RECIPES_CORE, template_ids, line_ids)
    inputs = await conn.fetch(SQL_FETCH_RECIPE_INPUTS, template_ids, line_ids)
    outputs = await conn.fetch(SQL_FETCH_RECIPE_OUTPUTS, template_ids, line_ids)
    return core, inputs, outputs


async def fetch_corp_workforce(conn: Any, user_id: str):
    return await conn.fetch(SQL_FETCH_CORP_WORKFORCE, user_id)
