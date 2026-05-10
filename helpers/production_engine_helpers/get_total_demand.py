# FULL AI GENERATED CODE Well not really but yeah

from collections import defaultdict, deque
from typing import Any, Counter, Dict, List

import asyncpg


async def get_total_demand(conn: asyncpg.Connection, ship_parts: List[str], overrides: Dict[str, str] = None) -> Dict[str, Any]:
    overrides = overrides or {}

    # 1. Fetch Recipes and Resource Flags
    recipe_query = """
        SELECT r.id, r.reactor_id, r.duration_ms, output.material_ticker as out_t, output.amount as out_q,
               input.material_ticker as in_t, input.amount as in_q, mo.resource as is_res
        FROM material_recipes r 
        JOIN material_recipe_ingredients output ON output.recipe_id = r.id AND output.type = 'OUTPUT' 
        LEFT JOIN material_recipe_ingredients input ON input.recipe_id = r.id AND input.type = 'INPUT'
        INNER JOIN materials mo ON mo.ticker = output.material_ticker
    """
    r_rows = await conn.fetch(recipe_query)

    all_variants = defaultdict(dict)
    resource_tickers = set()

    for r in r_rows:
        rid, out_t = str(r['id']), r['out_t']
        if r['is_res']:
            resource_tickers.add(out_t)
            continue

        if rid not in all_variants[out_t]:
            all_variants[out_t][rid] = {
                "recipe_id": rid,
                "reactor": r['reactor_id'],
                "duration": r['duration_ms'] or 3600000,
                "out_q": float(r['out_q']),
                "inputs": []
            }
        if r['in_t']:
            all_variants[out_t][rid]["inputs"].append({"ticker": r['in_t'], "amount": float(r['in_q'])})

    # 2. BOM Explosion Logic (Corrected for Single-Batch Output)
    total_demand, raw_demand = Counter(), Counter()
    explode_q = deque()

    # Logic Fix: Only add UNIQUE final products, scaled to their recipe output (1 Batch)
    unique_parts = set(ship_parts)

    for root_ticker in unique_parts:
        # Find the recipe to determine batch size
        variants = all_variants.get(root_ticker, {})
        if not variants:
             # If no recipe (e.g. raw resource passed as final), default to 1
             explode_q.append((root_ticker, 1.0))
             continue

        # Resolve override or default
        v_id = overrides.get(root_ticker)
        selected = variants.get(v_id) if v_id else list(variants.values())[0]

        # KEY FIX: Force quantity to exactly one recipe batch
        batch_size = selected["out_q"]
        explode_q.append((root_ticker, batch_size))

    involved = set(ship_parts)
    choices_needed = {}
    material_to_recipe = {}

    while explode_q:
        curr_t, curr_q = explode_q.popleft()

        if curr_t in resource_tickers:
            raw_demand[curr_t] += curr_q
            continue

        variants = all_variants.get(curr_t, {})

        if not variants:
            raw_demand[curr_t] += curr_q
            continue

        total_demand[curr_t] += curr_q

        if len(variants) > 1 and curr_t not in overrides:
            choices_needed[curr_t] = list(variants.values())

        v_id = overrides.get(curr_t)
        selected = variants.get(v_id) if v_id else list(variants.values())[0]
        material_to_recipe[curr_t] = selected

        runs = curr_q / selected["out_q"]
        for inp in selected["inputs"]:
            if inp["ticker"] not in involved:
                involved.add(inp["ticker"])
            explode_q.append((inp["ticker"], inp["amount"] * runs))

    if choices_needed:
        return {"status": "ambiguous", "choices_needed": choices_needed}

    return {
        "status": "success",
        "total_demand": dict(total_demand),
        "raw_demand": dict(raw_demand),
        "resolved_recipes": material_to_recipe
    }
