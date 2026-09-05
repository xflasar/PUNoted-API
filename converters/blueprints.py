# converters/blueprints.py
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

def convert_blueprints_data(raw_records: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Converts raw BLUEPRINT_BLUEPRINTS websocket message into normalized database records for:
      - ship_blueprints
      - ship_blueprint_bill_of_materials
      - ship_blueprint_components
      - ship_blueprints_component_options
      - ship_blueprints_component_types
    """
    payload = raw_records.get("payload", {})
    if isinstance(payload, dict) and "blueprints" in payload:
        records_to_process = payload["blueprints"]
    elif isinstance(payload, list):
        records_to_process = payload
    else:
        records_to_process = []

    blueprints_list = []
    boms_list = []
    components_list = []
    options_dict = {}
    types_dict = {}

    for bp in records_to_process:
        if not isinstance(bp, dict):
            continue

        bp_id = bp.get("id")
        if not bp_id:
            continue

        created_obj = bp.get("created") or {}
        created_ts = created_obj.get("timestamp") if isinstance(created_obj, dict) else None
        created_at = datetime.fromtimestamp(created_ts / 1000.0, tz=timezone.utc).replace(tzinfo=None) if created_ts else datetime.now()

        bom_obj = bp.get("billOfMaterial") or {}
        selections_list = bp.get("selections") or []
        performance_obj = bp.get("performance") or {}

        # 1. Main Blueprint Record
        blueprints_list.append({
            "id": bp_id,
            "natural_id": bp.get("naturalId"),
            "name": bp.get("name"),
            "type": bp.get("type"),
            "status": bp.get("status"),
            "created_at": created_at,
            "bill_of_material": json.dumps(bom_obj),
            "selections": json.dumps(selections_list),
            "performance": json.dumps(performance_obj),
            "build_time": bp.get("buildTime") or 0,
        })

        # 2. Bill of Materials Items
        quantities = bom_obj.get("quantities") or []
        for item in quantities:
            mat = item.get("material") or {}
            mat_id = mat.get("id")
            amount = item.get("amount")
            if mat_id:
                boms_list.append({
                    "blueprintid": bp_id,
                    "materialid": mat_id,
                    "amount": amount or 0,
                })

        # 3. Component Selections, Options & Types
        for sel in selections_list:
            sel_id = sel.get("id")
            c_type = sel.get("type")
            cardinality = sel.get("cardinality")
            option = sel.get("option")
            opt_mat_id = sel.get("optionMaterialId")
            opt_mat_name = sel.get("optionMaterialName")
            amount = sel.get("amount") or 0

            if sel_id:
                components_list.append({
                    "id": sel_id,
                    "blueprintid": bp_id,
                    "type": c_type,
                    "cardinality": cardinality,
                    "option": option,
                    "optionmaterialid": opt_mat_id,
                    "amount": amount,
                })

            if c_type and c_type not in types_dict:
                types_dict[c_type] = {
                    "id": c_type,
                    "type": c_type,
                    "cardinality": cardinality,
                    "selectable": True,
                }

            if opt_mat_id and opt_mat_id not in options_dict:
                options_dict[opt_mat_id] = {
                    "id": opt_mat_id,
                    "type": c_type,
                    "option": option,
                    "materialname": opt_mat_name,
                }

    return {
        "ship_blueprints": blueprints_list,
        "ship_blueprint_bill_of_materials": boms_list,
        "ship_blueprint_components": components_list,
        "ship_blueprints_component_options": list(options_dict.values()),
        "ship_blueprints_component_types": list(types_dict.values()),
    }

