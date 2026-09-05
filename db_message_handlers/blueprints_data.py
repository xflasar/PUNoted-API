# db_message_handlers/blueprints_data.py
import json
import logging
from typing import Any, Dict, List

from helpers.db import _upsert_records

logger = logging.getLogger(__name__)

async def handle_blueprints_data_message(db, payload: Dict[str, Any], user_id: str = None) -> Dict[str, Any]:
    """
    Handles BLUEPRINT_BLUEPRINTS websocket message by upserting converted blueprint records into 5 tables:
      1. ship_blueprints (unique_fields: id)
      2. ship_blueprint_bill_of_materials (unique_fields: blueprintid, materialid)
      3. ship_blueprint_components (unique_fields: id)
      4. ship_blueprints_component_options (unique_fields: id)
      5. ship_blueprints_component_types (unique_fields: id)
    """
    if isinstance(payload, dict) and "data" in payload:
        user_id = payload.get("userId") or user_id
        converted_tables = payload.get("data", {})
    else:
        converted_tables = payload if isinstance(payload, dict) else {}

    if not isinstance(converted_tables, dict):
        return {}

    blueprints = converted_tables.get("ship_blueprints", [])
    boms = converted_tables.get("ship_blueprint_bill_of_materials", [])
    components = converted_tables.get("ship_blueprint_components", [])
    options = converted_tables.get("ship_blueprints_component_options", [])
    types = converted_tables.get("ship_blueprints_component_types", [])

    if user_id:
        for bp in blueprints:
            bp["user_id"] = user_id
        for bom in boms:
            bom["user_id"] = user_id
        for comp in components:
            comp["user_id"] = user_id

    try:
        async with db.pool.acquire() as conn:
            # 1. Main Ship Blueprints
            if blueprints:
                await _upsert_records(
                    con=conn,
                    table_name="ship_blueprints",
                    records=blueprints,
                    unique_fields=["id"],
                )

            # 2. Component Types
            if types:
                await _upsert_records(
                    con=conn,
                    table_name="ship_blueprints_component_types",
                    records=types,
                    unique_fields=["id"],
                )

            # 3. Component Options
            if options:
                await _upsert_records(
                    con=conn,
                    table_name="ship_blueprints_component_options",
                    records=options,
                    unique_fields=["id"],
                )

            # 4. Blueprint Components
            if components:
                await _upsert_records(
                    con=conn,
                    table_name="ship_blueprint_components",
                    records=components,
                    unique_fields=["id"],
                )

            # 5. Bill of Materials
            if boms:
                await _upsert_records(
                    con=conn,
                    table_name="ship_blueprint_bill_of_materials",
                    records=boms,
                    unique_fields=["blueprintid", "materialid"],
                )

        logger.info(
            f"Successfully upserted blueprint data for user {user_id}: "
            f"{len(blueprints)} blueprints, {len(boms)} BOM items, {len(components)} components, "
            f"{len(options)} options, {len(types)} types"
        )
    except Exception as e:
        logger.error(f"Failed to upsert normalized blueprint records for user {user_id}: {e}", exc_info=True)

    return converted_tables
