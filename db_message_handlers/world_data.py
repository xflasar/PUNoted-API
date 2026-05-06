import logging
from typing import Dict, Any, List

import asyncpg
from db import Database

logger = logging.getLogger(__name__)

async def save_material_recipes(conn: asyncpg.Connection, recipes_data: List[Dict]):
    """
    Saves a list of normalized recipe dictionaries to the database.
    Uses 'ON CONFLICT DO NOTHING' to handle duplicates safely based on the hash ID.
    
    Args:
        conn: Active asyncpg connection (or transaction object).
        recipes_data: List of dicts from data_converters.
    """
    if not recipes_data:
        return

    # 1. Prepare Batch for Parent Table (material_recipes)
    # List of tuples: (id, reactor_id, duration_ms)
    recipe_tuples = [
        (r['recipe_id'], r['reactor_id'], r['duration_ms']) 
        for r in recipes_data
    ]

    # 2. Prepare Batch for Child Table (material_recipe_ingredients)
    # List of tuples: (recipe_id, material_id, ticker, amount, type)
    ingredient_tuples = []
    
    for r in recipes_data:
        # Map Inputs
        for i in r['inputs']:
            ingredient_tuples.append((
                r['recipe_id'], 
                i['material_id'], 
                i['material_ticker'], 
                float(i['amount']), 
                'INPUT'
            ))
        
        # Map Outputs
        for o in r['outputs']:
            ingredient_tuples.append((
                r['recipe_id'], 
                o['material_id'], 
                o['material_ticker'], 
                float(o['amount']), 
                'OUTPUT'
            ))

    try:
        # 3. Execute Inserts
        
        # Insert Recipes (Parents)
        query_recipes = """
            INSERT INTO material_recipes (id, reactor_id, duration_ms)
            VALUES ($1, $2, $3)
            ON CONFLICT (id) DO NOTHING
        """
        await conn.executemany(query_recipes, recipe_tuples)

        # Insert Ingredients (Children)
        query_ingredients = """
            INSERT INTO material_recipe_ingredients 
            (recipe_id, material_id, material_ticker, amount, type)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (recipe_id, material_id, type) DO NOTHING
        """
        await conn.executemany(query_ingredients, ingredient_tuples)
        
        logger.debug(f"Processed {len(recipe_tuples)} recipes and {len(ingredient_tuples)} ingredients.")

    except Exception as e:
        logger.error(f"Failed to save material recipes: {e}")
        raise e

import logging
from typing import Any
import asyncpg

logger = logging.getLogger(__name__)

# --- 1. MATERIAL DATA HANDLER ---
async def save_world_materials(conn: asyncpg.Connection, materials_data: list):
    """
    Saves WORLD_MATERIAL_DATA to the 'materials' table.
    """
    if not materials_data:
        return

    # Prepare tuples: (id, name, ticker, category, weight, volume, resource)
    mat_tuples = [
        (
            m['material_id'], 
            m['name'], 
            m['ticker'], 
            m['category_id'], 
            float(m.get('weight', 0)), 
            float(m.get('volume', 0)), 
            m.get('is_resource', False)
        )
        for m in materials_data
    ]

    try:
        async with conn.transaction():
            await conn.executemany("""
                INSERT INTO materials (materialid, name, ticker, category, weight, volume, resource)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (materialid) DO UPDATE 
                SET name = EXCLUDED.name, 
                    weight = EXCLUDED.weight, 
                    volume = EXCLUDED.volume,
                    resource = EXCLUDED.resource
            """, mat_tuples)
            
        logger.debug(f"✅ Saved/Updated {len(mat_tuples)} materials.")

    except Exception as e:
        logger.error(f"❌ Failed to save world materials: {e}")
        raise e


# --- 2. REACTOR DATA HANDLER (Buildings + Costs + Recipes) ---
async def save_world_reactor_data(conn: asyncpg.Connection, reactor_payload: dict):
    """
    Saves WORLD_REACTOR_DATA. 
    Expects the 'reactor_payload' to be the DICTIONARY output from your 'parse_world_reactor_data' function.
    Keys: 'buildings', 'building_build_materials', 'material_recipes', 'material_recipe_ingredients'
    """
    
    # Unpack the converted data
    buildings = reactor_payload.get('buildings', [])
    build_mats = reactor_payload.get('building_build_materials', [])
    recipes = reactor_payload.get('material_recipes', [])
    ingredients = reactor_payload.get('material_recipe_ingredients', [])
    workforce = reactor_payload.get('building_workforce_capacities', [])

    try:
        async with conn.transaction():
            # A. Insert Buildings
            if buildings:
                b_tuples = [(b['buildingid'], b['name'], b['ticker'], b['area'], b['type'], b['expertisecategory']) for b in buildings]
                await conn.executemany("""
                    INSERT INTO buildings (buildingid, name, ticker, area, type, expertisecategory)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (buildingid) DO UPDATE 
                    SET area = EXCLUDED.area, expertisecategory = EXCLUDED.expertisecategory
                """, b_tuples)

            # B. Insert Building Construction Costs
            # Note: We usually DELETE existing costs for these buildings first to handle removed requirements
            if build_mats:
                building_ids = list(set(b['buildingid'] for b in build_mats))
                await conn.execute("DELETE FROM building_build_materials WHERE buildingid = ANY($1)", building_ids)
                
                bm_tuples = [(x['buildingid'], x['materialid'], float(x['amount'])) for x in build_mats]
                await conn.executemany("""
                    INSERT INTO building_build_materials (buildingid, materialid, amount)
                    VALUES ($1, $2, $3)
                """, bm_tuples)

            # C. Insert Recipe Headers
            if recipes:
                r_tuples = [(r['id'], r['reactor_id'], r['duration_ms']) for r in recipes]
                await conn.executemany("""
                    INSERT INTO material_recipes (id, reactor_id, duration_ms)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (id) DO NOTHING
                """, r_tuples)

            # D. Insert Recipe Ingredients (Inputs/Outputs)
            if ingredients:
                i_tuples = [(i['recipe_id'], i['material_id'], i['amount'], i['type']) for i in ingredients]
                await conn.executemany("""
                    INSERT INTO material_recipe_ingredients (recipe_id, material_id, amount, type)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (recipe_id, material_id, type) DO NOTHING
                """, i_tuples)
            
            if workforce:
                w_tuples = [(w['buildingid'], w["workforcelevel"], w["capacity"], w["ishabitation"]) for w in workforce]

                await conn.executemany("""
                    INSERT INTO building_workforce_capacities (buildingid, workforcelevel, capacity, ishabitation)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (buildingid, workforcelevel) DO UPDATE 
                    SET capacity = EXCLUDED.capacity
                """, w_tuples)

        logger.debug(f"✅ Processed Reactor Data: {len(buildings)} Buildings, {len(recipes)} Recipes.")

    except Exception as e:
        logger.error(f"❌ Failed to save reactor data: {e}")
        raise e
    
async def handle_game_data_message(db, converted_payload: Any):
    """
    Main Dispatcher.
    
    Args:
        db: Database wrapper.
        message_type: String identifier (e.g., "WORLD_MATERIAL_DATA").
        converted_payload: The data returned by your transformation functions.
    """
    
    if not converted_payload:
        return {"status": "skipped", "reason": "Empty payload"}

    # Extract the actual data payload
    converted_data = converted_payload.get("data")
    
    # Default determination based on Data Structure
    if isinstance(converted_data, dict):
        message_type = "WORLD_REACTOR_DATA"
    elif isinstance(converted_data, list):
        message_type = "WORLD_MATERIAL_DATA"
    else:
        # Fallback or Error handling
        message_type = "UNKNOWN"
        logger.warning(f"Unknown data structure: {type(converted_data)}")

    try:
        async with db.pool.acquire() as conn:
            
            # --- ROUTE 1: MATERIALS ---
            if message_type == "WORLD_MATERIAL_DATA":
                # Expects converted_payload to be a List of Dicts
                await save_material_recipes(conn, converted_data)
                return {"status": "success", "type": "materials"}

            # --- ROUTE 2: REACTORS (Buildings + Recipes) ---
            elif message_type == "WORLD_REACTOR_DATA":
                # Expects converted_payload to be a Dict containing lists
                await save_world_reactor_data(conn, converted_data)
                return {"status": "success", "type": "reactors"}

            else:
                logger.warning(f"Unknown message type received: {message_type}")
                return {"status": "ignored", "type": message_type}

    except Exception as e:
        logger.error(f"CRITICAL DB ERROR processing {message_type}: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}