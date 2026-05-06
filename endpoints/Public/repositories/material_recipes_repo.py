import logging

logger = logging.getLogger(__name__)

SQL_GET_RECIPES_ALL = """
WITH recipe_inputs AS (
    SELECT recipe_id, 
        jsonb_agg(jsonb_build_object('Ticker', material_ticker, 'Amount', amount)) AS inputs,
        string_agg(amount::text || 'x' || material_ticker, ' ' ORDER BY material_ticker) AS input_str
    FROM material_recipe_ingredients
    WHERE UPPER(type) = 'INPUT'
    GROUP BY recipe_id
),
recipe_outputs AS (
    SELECT recipe_id, 
        jsonb_agg(jsonb_build_object('Ticker', material_ticker, 'Amount', amount)) AS outputs,
        string_agg(amount::text || 'x' || material_ticker, ' ' ORDER BY material_ticker) AS output_str
    FROM material_recipe_ingredients
    WHERE UPPER(type) = 'OUTPUT'
    GROUP BY recipe_id
),
recipe_objects AS (
    SELECT jsonb_build_object(
        'BuildingTicker', r.reactor_id,
        'RecipeName', COALESCE(i.input_str, '') || ' = ' || COALESCE(o.output_str, ''),
        'Inputs', COALESCE(i.inputs, '[]'::jsonb),
        'Outputs', COALESCE(o.outputs, '[]'::jsonb),
        'TimeMs', r.duration_ms
    ) AS r_json
    FROM material_recipes r
    LEFT JOIN recipe_inputs i ON r.id = i.recipe_id
    LEFT JOIN recipe_outputs o ON r.id = o.recipe_id
)
SELECT json_agg(r_json)::text AS final_payload 
FROM recipe_objects;
"""

SQL_GET_RECIPES_DETAILED = """
WITH recipe_inputs AS (
    SELECT mri.recipe_id, 
        jsonb_agg(jsonb_build_object(
            'CommodityName', m.name,
            'CommodityTicker', mri.material_ticker,
            'Weight', m.weight,
            'Volume', m.volume,
            'Amount', mri.amount
        )) AS inputs,
        string_agg(mri.amount::text || 'x' || mri.material_ticker, ' ' ORDER BY mri.material_ticker) AS input_str,
        string_agg(mri.amount::text || 'x' || mri.material_ticker, '-' ORDER BY mri.material_ticker) AS input_str_dash,
        array_agg(mri.material_ticker::text) AS input_tickers
    FROM material_recipe_ingredients mri
    LEFT JOIN materials m ON mri.material_ticker = m.ticker
    WHERE UPPER(mri.type) = 'INPUT'
    GROUP BY mri.recipe_id
),
recipe_outputs AS (
    SELECT mri.recipe_id, 
        jsonb_agg(jsonb_build_object(
            'CommodityName', m.name,
            'CommodityTicker', mri.material_ticker,
            'Weight', m.weight,
            'Volume', m.volume,
            'Amount', mri.amount
        )) AS outputs,
        string_agg(mri.amount::text || 'x' || mri.material_ticker, ' ' ORDER BY mri.material_ticker) AS output_str,
        array_agg(mri.material_ticker::text) AS output_tickers
    FROM material_recipe_ingredients mri
    LEFT JOIN materials m ON mri.material_ticker = m.ticker
    WHERE UPPER(mri.type) = 'OUTPUT'
    GROUP BY mri.recipe_id
),
recipe_objects AS (
    SELECT jsonb_build_object(
        'BuildingTicker', r.reactor_id,
        'BuildingRecipeId', r.id,
        'Inputs', COALESCE(i.inputs, '[]'::jsonb),
        'Outputs', COALESCE(o.outputs, '[]'::jsonb),
        'DurationMs', r.duration_ms,
        
        -- Keeps the => but ensures it doesn't look broken if inputs are completely missing
        'RecipeName', COALESCE(i.input_str, '') || '=>' || COALESCE(o.output_str, ''),
        'StandardRecipeName', r.reactor_id || ':' || COALESCE(i.input_str_dash, '') || '=>' || COALESCE(o.output_str, '')
    ) AS r_json
    FROM material_recipes r
    LEFT JOIN recipe_inputs i ON r.id = i.recipe_id
    LEFT JOIN recipe_outputs o ON r.id = o.recipe_id
    
    WHERE ($1::text[] IS NULL OR o.output_tickers && $1::text[] OR i.input_tickers && $1::text[])
)
SELECT json_agg(r_json)::text AS final_payload 
FROM recipe_objects;
"""

async def fetch_recipes_all(db) -> str:
    async with db.pool.acquire() as con:
        await con.execute("SET statement_timeout = '15s';")
        record = await con.fetchrow(SQL_GET_RECIPES_ALL)
        return record["final_payload"] if record and record["final_payload"] else "[]"

async def fetch_recipes_detailed(db, tickers_list: list) -> str:
    async with db.pool.acquire() as con:
        await con.execute("SET statement_timeout = '15s';")
        record = await con.fetchrow(SQL_GET_RECIPES_DETAILED, tickers_list)
        return record["final_payload"] if record and record["final_payload"] else "[]"