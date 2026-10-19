from typing import Any, Dict, Iterable, Tuple

# --- QUERY 1: Fetch Flat Orders ---
SQL_FETCH_CORP_FLAT_ORDERS = """
WITH 
TargetUser AS (SELECT userdataid FROM users WHERE accountid = $1::uuid LIMIT 1),
TargetUserVars AS (SELECT $1::uuid as uid),
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

    AND NOT EXISTS (
        SELECT 1 
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(ugs.internal_excluded_sites) = 'array' THEN ugs.internal_excluded_sites 
                ELSE '[]'::jsonb 
            END
        ) as ex 
        WHERE ex->>'siteId' = s.siteid
    )
    AND NOT EXISTS (
        SELECT 1 
        FROM jsonb_array_elements(
            CASE 
                WHEN jsonb_typeof(ugs.internal_leased_sites::jsonb) = 'array' 
                    THEN ugs.internal_leased_sites::jsonb
                WHEN jsonb_typeof(ugs.internal_leased_sites::jsonb) = 'string' 
                     AND jsonb_typeof((ugs.internal_leased_sites::jsonb #>> '{}')::jsonb) = 'array' 
                    THEN (ugs.internal_leased_sites::jsonb #>> '{}')::jsonb
                ELSE '[]'::jsonb 
            END
        ) as ls 
        WHERE ls->>'siteId' = s.siteid
        AND (ls->>'show_in_corp')::boolean IS NOT TRUE 
    )
)
SELECT 
    cs.siteid,
    cs.player_name,
    cs.location_name,
    cs.is_accurate,
    pl.productionlineid,
    pl.type as building_ticker,
    pl.capacity,
    pl.condition,
    po.orderid,
    po.recurring,
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
# Recipe rows are stored once per production line (duration and factors as that line runs the
# recipe), so they are fetched by (template, line) pair, as the single-user endpoint does. Matching
# on the template alone returns one row per line in the whole database.
# $1: template ids, $2: line ids, paired by position.

# 1. Core Info
SQL_FETCH_RECIPES_CORE = """
WITH Targets AS (
    SELECT unnest($1::text[]) as t_id, unnest($2::text[]) as l_id
)
SELECT
    r.productiontemplateid::text as recipe_id,
    r.productionlineid::text as line_id,
    r.duration
FROM production_recipes r
JOIN Targets t ON r.productiontemplateid::text = t.t_id AND r.productionlineid::text = t.l_id;
"""

# 2. Inputs
SQL_FETCH_RECIPE_INPUTS = """
WITH Targets AS (
    SELECT unnest($1::text[]) as t_id, unnest($2::text[]) as l_id
)
SELECT
    i.productiontemplateid::text as recipe_id,
    i.productionlineid::text as line_id,
    m.ticker,
    i.factor
FROM production_recipe_input_factors i
JOIN Targets t ON i.productiontemplateid::text = t.t_id AND i.productionlineid::text = t.l_id
JOIN materials m ON m.materialid = i.materialid;
"""

# 3. Outputs
SQL_FETCH_RECIPE_OUTPUTS = """
WITH Targets AS (
    SELECT unnest($1::text[]) as t_id, unnest($2::text[]) as l_id
)
SELECT
    o.productiontemplateid::text as recipe_id,
    o.productionlineid::text as line_id,
    m.ticker,
    o.factor
FROM production_recipe_output_factors o
JOIN Targets t ON o.productiontemplateid::text = t.t_id AND o.productionlineid::text = t.l_id
JOIN materials m ON m.materialid = o.materialid;
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


async def fetch_recipe_components(conn: Any, pairs: Iterable[Tuple[str, str]]):
    """Recipe rows for (template id, production line id) pairs."""
    pairs = list(pairs)
    template_ids = [t for t, _ in pairs]
    line_ids = [line for _, line in pairs]
    core = await conn.fetch(SQL_FETCH_RECIPES_CORE, template_ids, line_ids)
    inputs = await conn.fetch(SQL_FETCH_RECIPE_INPUTS, template_ids, line_ids)
    outputs = await conn.fetch(SQL_FETCH_RECIPE_OUTPUTS, template_ids, line_ids)
    return core, inputs, outputs


def build_recipe_map(core_rows: Iterable[Any], input_rows: Iterable[Any], output_rows: Iterable[Any]) -> Dict[Tuple[str, str], dict]:
    """{(template id, line id): {"duration", "inputs", "outputs"}} from fetch_recipe_components."""
    recipe_map: Dict[Tuple[str, str], dict] = {}
    for r in core_rows:
        recipe_map[(r["recipe_id"], r["line_id"])] = {"duration": r["duration"], "inputs": [], "outputs": []}
    for rows, side in ((input_rows, "inputs"), (output_rows, "outputs")):
        for f in rows:
            k = (f["recipe_id"], f["line_id"])
            if k in recipe_map:
                recipe_map[k][side].append({"ticker": f["ticker"], "factor": f["factor"]})
    return recipe_map


async def fetch_corp_workforce(conn: Any, user_id: str):
    return await conn.fetch(SQL_FETCH_CORP_WORKFORCE, user_id)
