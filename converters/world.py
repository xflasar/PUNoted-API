# converters/world.py
import datetime
import hashlib
import json
import string
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

def generate_recipe_hash(reactor_id: str, duration_ms: int, inputs: List[Dict], outputs: List[Dict]) -> str:
    """
    Creates a deterministic MD5 hash to identify a unique recipe.
    Sorts inputs and outputs by Material ID to ensure order independence.
    """
    sorted_inputs = sorted(inputs, key=lambda x: x['material']['id'])
    sorted_outputs = sorted(outputs, key=lambda x: x['material']['id'])

    input_str = ",".join([f"{i['material']['id']}-{i['amount']}" for i in sorted_inputs])
    output_str = ",".join([f"{o['material']['id']}-{o['amount']}" for o in sorted_outputs])

    unique_string = f"{reactor_id}|{duration_ms}|IN:{input_str}|OUT:{output_str}"

    return hashlib.md5(unique_string.encode('utf-8')).hexdigest()

def normalize_recipe_object(raw_recipe: Dict[str, Any], reactor_id: str) -> Dict[str, Any]:
    """Helper to convert a raw recipe node into our flat internal structure."""
    inputs = raw_recipe.get('inputs', [])
    outputs = raw_recipe.get('outputs', [])
    duration = raw_recipe.get('duration', {}).get('millis', 0)

    rec_id = generate_recipe_hash(reactor_id, duration, inputs, outputs)

    return {
        "recipe_id": rec_id,
        "reactor_id": reactor_id,
        "duration_ms": duration,
        "inputs": [
            {
                "material_id": i['material']['id'],
                "material_ticker": i['material']['ticker'],
                "amount": i['amount']
            } for i in inputs
        ],
        "outputs": [
            {
                "material_id": o['material']['id'],
                "material_ticker": o['material']['ticker'],
                "amount": o['amount']
            } for o in outputs
        ]
    }

def determine_building_type(ticker: str, name: str, expertise: str) -> str:
    """
    Determines building type based on specific user rules.
    Priority: Habitation -> Storage -> Core -> Manufacturing (if expertise exists).
    """
    ticker_u = ticker.upper()
    name_l = name.lower()

    if ticker_u.startswith("HB") or "habitation" in name_l:
        return "HABITATION"

    if "storage" in name_l:
        return "STORAGE"

    if ticker_u == "CM":
        return "CORE"

    if expertise:
        return "MANUFACTURING"

    return "INFRASTRUCTURE"



def convert_world_materials_data(
    raw_data: Dict[str, Any],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Converts raw material data into two separate lists of dictionaries,
    one for material_categories and one for materials.
    """
    converted_categories = []
    converted_materials = []

    categories_data = raw_data["payload"].get("categories", [])

    for category in categories_data:
        category_id = category.get("id")
        name = category.get("name")
        children_ids = category.get("children")

        # Add a record for the material_categories table
        converted_categories.append(
            {
                "id": category_id if category_id is not None else "null",
                "name": name if name is not None else "null",
                #'children': json.dumps(children_ids if children_ids is not None else [])
            }
        )

        materials = category.get("materials", [])
        for material in materials:
            material_id = material.get("id")
            name = material.get("name")
            ticker = material.get("ticker")
            weight = material.get("weight")
            volume = material.get("volume")
            resource = material.get("resource")

            # Add a record for the materials table
            converted_materials.append(
                {
                    "materialid": material_id if material_id is not None else "null",
                    "name": name if name is not None else "null",
                    "ticker": ticker if ticker is not None else "null",
                    "category": category_id if category_id is not None else "null",
                    "weight": weight if weight is not None else 0.0,
                    "volume": volume if volume is not None else 0.0,
                    "resource": resource if resource is not None else False,
                }
            )

    return {
        "material_categories": converted_categories,
        "materials": converted_materials,
    }

def convert_world_material_data(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parses WORLD_MATERIAL_DATA.
    Extracts 'outputRecipes' (Manufacturing recipes producing this item).
    """
    payload = data.get('payload', {})
    raw_recipes = payload.get('outputRecipes', [])

    converted = []
    for r in raw_recipes:
        # In Material Data, reactorId is usually embedded in the recipe object
        reactor_id = r.get('reactorId')
        if reactor_id:
            converted.append(normalize_recipe_object(r, reactor_id))

    return converted

def convert_world_reactor_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parses a single World Reactor JSON object (like the Refinery example).
    Returns a dictionary containing lists of rows for:
    1. buildings
    2. building_build_materials
    3. material_recipes
    4. material_recipe_ingredients
    """
    data = data['payload']

    # --- 1. PARSE BUILDING ---
    building_row = {
        "buildingid": data['id'],
        "name": data['name'],
        "ticker": data['ticker'],
        "area": data.get('areaCost'),
        "type": determine_building_type(data['ticker'], data['name'], data.get('expertise')),
        "expertisecategory": data.get('expertise')
    }

    # --- 2. PARSE BUILDING COSTS (building_build_materials) ---
    build_mat_rows = []
    raw_costs = data.get('buildingCosts', [])

    for cost in raw_costs:
        build_mat_rows.append({
            "buildingid": data['id'],
            "materialid": cost['material']['id'],
            "amount": cost['amount']
        })

    # --- 3. PARSE RECIPES ---
    recipe_rows = []
    recipe_ingredient_rows = []

    raw_recipes = data.get('recipes', [])

    for r in raw_recipes:
        # Extract basic data
        duration_ms = r.get('duration', {}).get('millis', 0)
        inputs = r.get('inputs', [])
        outputs = r.get('outputs', [])

        # Generate ID
        rec_id = generate_recipe_hash(data['id'], duration_ms, inputs, outputs)

        # Add to material_recipes (The Header)
        recipe_rows.append({
            "id": rec_id,
            "reactor_id": data['id'],
            "duration_ms": duration_ms,
            "building_ticker": data['ticker'] # Helpful for debugging
        })

        # Add INPUTS to material_recipe_ingredients
        for i in inputs:
            recipe_ingredient_rows.append({
                "recipe_id": rec_id,
                "material_id": i['material']['id'],
                "type": "INPUT",
                "amount": i['amount'],
                "material_ticker": i['material']['ticker'] # Optional, depending on DB schema
            })

        # Add OUTPUTS to material_recipe_ingredients
        for o in outputs:
            recipe_ingredient_rows.append({
                "recipe_id": rec_id,
                "material_id": o['material']['id'],
                "type": "OUTPUT",
                "amount": o['amount'],
                "material_ticker": o['material']['ticker']
            })
    workforce_capacities = []

    if not data["ticker"].startswith("HB"):
        workfoce_capacities_raw = data.get('workforceCapacities', {})
        for wc in workfoce_capacities_raw:
            workforce_capacities.append({
                "buildingid": data['id'],
                "workforcelevel": wc.get('level'),
                "capacity": wc.get('capacity'),
                "ishabitation": data["ticker"].startswith("HB")
            })


    return {
        "messageType": "WORLD_REACTOR_DATA",
        "buildings": [building_row],
        "building_build_materials": build_mat_rows,
        "material_recipes": recipe_rows,
        "material_recipe_ingredients": recipe_ingredient_rows,
        "building_workforce_capacities": workforce_capacities
    }

def convert_sectors_data(
    raw_payload: Dict[str, Any],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Converts a payload with a list of sectors into structured lists for database insertion.

    Args:
        raw_payload: The raw JSON data from the request body.

    Returns:
        A dictionary containing lists of records for 'sectors', 'subsectors',
        and 'subsector_vertices' tables.
    """

    sector_records = []
    subsector_records = []
    vertex_records = []

    for sector in raw_payload["payload"].get("sectors", []):
        external_sector_id = sector.get("id")

        # Prepare record for the 'sectors' table
        sector_records.append(
            {
                "externalsectorid": external_sector_id,
                "name": sector.get("name"),
                "hexq": sector.get("hex", {}).get("q"),
                "hexr": sector.get("hex", {}).get("r"),
                "hexs": sector.get("hex", {}).get("s"),
                "size": sector.get("size"),
            }
        )

        # Prepare records for 'subsectors' and 'subsector_vertices'
        for subsector in sector.get("subsectors", []):
            external_subsector_id = subsector.get("id")

            # Add record for the 'subsectors' table
            subsector_records.append(
                {
                    "externalsubsectorid": external_subsector_id,
                    "externalsectorid": external_sector_id,
                }
            )

            # Add vertex records for the 'subsector_vertices' table
            for vertex_index, vertex in enumerate(subsector.get("vertices", [])):
                vertex_records.append(
                    {
                        "externalsubsectorid": external_subsector_id,
                        "index": vertex_index,
                        "x": vertex.get("x"),
                        "y": vertex.get("y"),
                        "z": vertex.get("z"),
                    }
                )

    return {
        "sectors": sector_records,
        "subsectors": subsector_records,
        "subsector_vertices": vertex_records,
    }

def convert_systems_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'systems' table schema."""
    systems = []
    systems_connections = []
    for record in raw_records.get("payload", {}).get("stars", []):  # Added .get() for safety
        # Ensure 'connections' key exists and is iterable
        for connection in record.get("connections", []):
            systems_connections.append(
                {
                    "systemiddestination": connection,
                    "systemidorigin": record.get("systemId"),
                }
            )

        # Safely access nested dictionary values
        address_lines = record.get("address", {}).get("lines", [])
        natural_id = None
        if address_lines and len(address_lines) > 0:
            entity = address_lines[0].get("entity", {})
            natural_id = entity.get("naturalId")

        systems.append(
            {
                "systemid": record.get("systemId"),
                "name": record.get("name"),
                "naturalid": natural_id,
                "type": record.get("type"),
                "positionx": record.get("position", {}).get("x"),
                "positiony": record.get("position", {}).get("y"),
                "positionz": record.get("position", {}).get("z"),
                "sectorid": record.get("sectorId"),
                "subsectorid": record.get("subSectorId"),
            }
        )
    return {"systems": systems, "systems_connections": systems_connections}

def convert_system_data(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    body: Dict[str, Any] = raw_record.get("payload", {})
    celestial_bodies = []
    celestial_bodies_raw = body.get("celestialBodies", [])
    for cbody in celestial_bodies_raw:
        if cbody.get("address").get("lines")[1].get("type") == "STATION":
            celestial_bodies.append({"stationid": cbody.get("id"), "orbit": cbody.get("orbit")})

    converted_data = {
        # Core System/MapPoint Data
        "id": body.get("id"),
        "meteoroidDensity": body.get("meteoroidDensity", 0),
        "mass": body.get("star").get("mass"),
        "masssol": body.get("star").get("massSol"),
        "luminosity": body.get("star").get("luminosity"),
        "celestialbodies": celestial_bodies,
    }

    return converted_data

def convert_system_connections_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'system_connections' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "systemId": record.get("systemId"),
                "connectedSystemId": record.get("connectedSystemId"),
            }
        )
    return converted_records

def convert_stations_data(raw_record: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'stations' table schema."""
    raw_record = raw_record["payload"]
    # Handle subscription expiry timestamp
    commissioning_time = raw_record.get("commissioningTime")
    if commissioning_time is not None and commissioning_time.get("timestamp") is not None:
        commissioning_time = datetime.fromtimestamp(commissioning_time["timestamp"] / 1000)
    else:
        commissioning_time = None

    converted_record = {
        "stationid": raw_record.get("id"),
        "systemid": raw_record.get("address").get("lines")[0].get("entity").get("id"),
        "name": raw_record.get("name"),
        "naturalid": raw_record.get("naturalId"),
        "commissioningtime": commissioning_time,
        "comexid": raw_record.get("comex").get("id"),
        "warehouseid": raw_record.get("warehouseId"),
        "localmarketid": raw_record.get("localMarketId"),
        "countryid": raw_record.get("country").get("id"),
        "governingentityid": raw_record.get("governingEntity").get("id"),
    }

    return converted_record
