from datetime import datetime
import json
from typing import Any, Dict, List, Union


def _ts_to_dt(ts_dict: Union[Dict[str, Any], None]) -> Any:
    """Helper: timestamp dict {timestamp: 123} -> datetime"""
    if not ts_dict or not isinstance(ts_dict, dict) or "timestamp" not in ts_dict:
        return None
    return datetime.fromtimestamp(ts_dict["timestamp"] / 1000.0)


def convert_gateway_data(raw_payload: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Dict[str, list]:
    """
    Parses raw API gateway payloads and organizes them into Dictionaries.
    """
    # 1. Safely extract nested wrappers first ('payload' or 'body')
    if isinstance(raw_payload, dict):
        core_data = raw_payload.get("payload", raw_payload.get("body", raw_payload))
    else:
        core_data = raw_payload

    # 2. Force into a list for uniform processing
    items_to_process = core_data if isinstance(core_data, list) else [core_data]

    data = {
        "gateways": [], "fuel_contractors": [], "traffic": [],
        "upkeep": [], "upkeep_requirements": [], "upkeep_phases": [], "upkeep_contractors": []
    }

    for item_data in items_to_process:
        # Safety check to ensure we are actually working with a dictionary now
        if not isinstance(item_data, dict):
            continue
            
        g_id = item_data.get("id")
        if not g_id:
            continue

        # 1. Address Parsing
        address_lines = item_data.get("address", {}).get("lines", [])
        system_id, planet_id, satellite_id = None, None, None
        gateway_type = "GATEWAY"

        for line in address_lines:
            ent = line.get("entity", {})
            etype = line.get("type")
            
            if etype == "SYSTEM":
                system_id = ent.get("id")
            elif etype == "PLANET":
                planet_id = ent.get("id")
            elif etype == "SATELLITE":
                satellite_id = ent.get("id")

            if ent.get("id") == g_id:
                gateway_type = etype

        # Safe fallbacks for nested objects that might be null
        fuel = item_data.get("fuel") or {}
        traf = item_data.get("traffic") or {}
        upk = item_data.get("upkeep") or {}

        # 2. Main Gateway
        data["gateways"].append({
            "id": g_id,
            "natural_id": item_data.get("naturalId"),
            "name": item_data.get("name"),
            "type": gateway_type,
            "system_id": system_id,
            "planet_id": planet_id,
            "satellite_id": satellite_id,
            "owner_admin_center_id": item_data.get("owner", {}).get("_proxy_key"),
            "currency_code": item_data.get("owner", {}).get("currency", {}).get("code"),
            "established": _ts_to_dt(item_data.get("established")), 
            "operational_state": item_data.get("operationalState"),
            "link_status": item_data.get("linkStatus"),
            "outgoing_link_id": item_data.get("outgoingLink"),
            "incoming_links": item_data.get("incomingLinks", []),
            "is_linked": item_data.get("linkStatus") == "ESTABLISHED",
            "max_ship_volume": item_data.get("maxShipVolume", 0),
            "linking_radius": item_data.get("linkingRadius", 0),
            "jumps_per_day": item_data.get("jumpsPerDay", 0),
            "capacity_upgrades": item_data.get("capacityUpgrades", 0),
            "volume_upgrades": item_data.get("volumeUpgrades", 0),
            "distance_upgrades": item_data.get("distanceUpgrades", 0),
            "fuel_available": fuel.get("availableFuelUnits", 0),
            "fuel_max": fuel.get("maxFuelUnits", 0),
            "fuel_per_jump": fuel.get("fuelPerJump", 0),
            "fuel_usage_fee": fuel.get("usageFee", {}).get("amount", 0),
            "fuel_usage_currency": fuel.get("usageFee", {}).get("currency"),
            "avg_fuel_availability": fuel.get("averageFuelAvailability", 0)
        })

        # 3. Fuel Contractors
        for fc_group in fuel.get("fuelContractors", []):
            phase = fc_group.get("phase")
            for c in fc_group.get("contractors", []):
                contr = c.get("contractor", {})
                data["fuel_contractors"].append({
                    "gateway_id": g_id,
                    "phase_index": phase,
                    "contractor_id": contr.get("id"),
                    "contractor_code": contr.get("code"),
                    "contractor_name": contr.get("name"),
                    "contract_id": c.get("contractId")
                })

        # 4. Traffic
        curr_phase = traf.get("currentPhase") or {}
        avgs = traf.get("averages") or {}
        data["traffic"].append({
            "gateway_id": g_id,
            "total_jumps": traf.get("totalJumps", 0),
            "current_phase_jumps": curr_phase.get("jumps", 0),
            "current_phase_inbound": curr_phase.get("inboundJumps", 0),
            "current_phase_start": _ts_to_dt(curr_phase.get("start")),
            "current_phase_end": _ts_to_dt(curr_phase.get("end")),
            "avg_jumps": avgs.get("jumps", 0),
            "avg_inbound": avgs.get("inboundJumps", 0),
            "raw_current_phase": curr_phase,
            "raw_last_phase": traf.get("lastPhase") or {},
            "raw_averages": avgs
        })

        # 5. Upkeep Core
        data["upkeep"].append({
            "gateway_id": g_id, 
            "average_uptime": upk.get("averageUptime", 0)
        })

        for req in upk.get("upkeep", []):
            mat = req.get("material", {})
            data["upkeep_requirements"].append({
                "gateway_id": g_id,
                "material_id": mat.get("id"),
                "material_ticker": mat.get("ticker"),
                "material_name": mat.get("name"),
                "amount_current": req.get("current", 0),
                "amount_required": req.get("required") or req.get("amount", 0)
            })

        for phase in upk.get("upkeepPhases", []):
            data["upkeep_phases"].append({
                "id": phase.get("id"),
                "gateway_id": g_id,
                "natural_id": phase.get("naturalId"),
                "start_time": _ts_to_dt(phase.get("start")),
                "end_time": _ts_to_dt(phase.get("end")),
                "service_level": phase.get("serviceLevel", 0),
                "materials_json": phase.get("upkeep", [])
            })

        for uc_group in upk.get("upkeepContractors", []):
            phase = uc_group.get("phase")
            for c in uc_group.get("contractors", []):
                contr = c.get("contractor", {})
                data["upkeep_contractors"].append({
                    "gateway_id": g_id,
                    "phase_index": phase,
                    "contractor_id": contr.get("id"),
                    "contractor_code": contr.get("code"),
                    "contractor_name": contr.get("name"),
                    "contract_id": c.get("contractId")
                })

    return data