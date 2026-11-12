# converters/ship_routes.py
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def get_entity_id(lines: list, entity_type: str) -> Optional[str]:
    """Safely extracts entity ID of a specific type from lines list."""
    if not lines:
        return None
    for line in lines:
        if line.get("type") == entity_type and "entity" in line:
            return line["entity"].get("id")
    return None


def parse_composite_destination(destination_str: str) -> Dict[str, Optional[str]]:
    """
    Parses composite destination string like:
    '7eb87450fab38d5a2d8316ccb9175af9|PLA,c2c310671d012b87c1ed5ffc59490601'
    Returns dict with system_id, location_type, and location_id.
    """
    result = {"system_id": None, "location_type": None, "location_id": None}
    if not destination_str or not isinstance(destination_str, str):
        return result

    try:
        if "|" in destination_str:
            sys_part, loc_part = destination_str.split("|", 1)
            result["system_id"] = sys_part.strip()
            if "," in loc_part:
                type_code, loc_id = loc_part.split(",", 1)
                type_code = type_code.strip().upper()
                result["location_type"] = "PLANET" if type_code == "PLA" else ("STATION" if type_code == "STA" else type_code)
                result["location_id"] = loc_id.strip()
    except Exception as e:
        logger.warning(f"Failed to parse composite destination '{destination_str}': {e}")

    return result


def convert_single_route(route: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converts a single route dictionary into normalized route, waypoints, and steps.
    """
    route_id = route.get("id")
    created_ts = route.get("created", {}).get("timestamp") if isinstance(route.get("created"), dict) else route.get("created")
    created_dt = None
    if created_ts:
        try:
            created_dt = datetime.fromtimestamp(created_ts / 1000, tz=timezone.utc).replace(tzinfo=None)
        except Exception:
            created_dt = None

    converted_route = {
        "routeid": route_id,
        "naturalid": route.get("naturalId") or f"RT-{route_id[:8]}",
        "name": route.get("name"),
        "repeats": route.get("repeats", True),
        "created_at": created_dt,
        "waypoints": [],
        "steps": [],
    }

    raw_waypoints = route.get("waypoints", [])
    for wp_index, wp in enumerate(raw_waypoints):
        wp_id = wp.get("id")
        if not wp_id:
            continue

        system_id = None
        location_id = None
        location_type = None

        dest = wp.get("destination")
        if isinstance(dest, dict) and "lines" in dest:
            lines = dest.get("lines", [])
            system_id = get_entity_id(lines, "SYSTEM")
            planet_id = get_entity_id(lines, "PLANET")
            station_id = get_entity_id(lines, "STATION")
            if planet_id:
                location_id = planet_id
                location_type = "PLANET"
            elif station_id:
                location_id = station_id
                location_type = "STATION"
        elif isinstance(dest, str):
            parsed = parse_composite_destination(dest)
            system_id = parsed["system_id"]
            location_id = parsed["location_id"]
            location_type = parsed["location_type"]

        prefs = wp.get("preferences", {})

        converted_wp = {
            "waypointid": wp_id,
            "routeid": route_id,
            "waypoint_index": wp_index,
            "system_id": system_id,
            "location_id": location_id,
            "location_type": location_type,
            "landing": wp.get("landing", True),
            "fuel_usage_factor": float(wp.get("fuelUsageFactor", 1.0) or 1.0),
            "reactor_usage_factor": float(wp.get("reactorUsageFactor", 1.0) or 1.0),
            "ftl_preference": prefs.get("ftlPreference", "LEAST_JUMPS"),
            "use_gateways": prefs.get("useGateways", True),
        }
        converted_route["waypoints"].append(converted_wp)

        # Convert steps
        raw_steps = wp.get("steps", [])
        for step_index, st in enumerate(raw_steps):
            step_id = st.get("id")
            if not step_id:
                continue

            step_type = st.get("type", "UNKNOWN").upper()
            material_id = None
            material_ticker = None
            mat = st.get("material")
            if isinstance(mat, dict):
                material_id = mat.get("id")
                material_ticker = mat.get("ticker")
            elif isinstance(mat, str):
                material_id = mat

            min_obj = st.get("minimum", {})
            max_obj = st.get("maximum", {})

            converted_step = {
                "stepid": step_id,
                "waypointid": wp_id,
                "routeid": route_id,
                "step_index": step_index,
                "type": step_type,
                "materialid": material_id,
                "material_ticker": material_ticker,
                "min_type": min_obj.get("type") if isinstance(min_obj, dict) else None,
                "min_units": min_obj.get("units") if isinstance(min_obj, dict) else None,
                "max_type": max_obj.get("type") if isinstance(max_obj, dict) else None,
                "max_units": max_obj.get("units") if isinstance(max_obj, dict) else None,
                "retain": st.get("retain", 0) or 0,
                "fuel_type": st.get("fuel"),
                "fuel_source": st.get("source"),
                "wait_amount": st.get("amount"),
                "wait_unit": st.get("unit"),
            }
            converted_route["steps"].append(converted_step)

    return converted_route


def convert_ship_routes_payload(raw_records: Any) -> List[Dict[str, Any]]:
    """
    Converter for SHIP_ROUTES_ROUTES (list) and SHIP_ROUTES_ROUTE (single).
    """
    payload = raw_records.get("payload", raw_records) if isinstance(raw_records, dict) else raw_records

    if isinstance(payload, dict) and "routes" in payload:
        routes_list = payload["routes"]
    elif isinstance(payload, dict) and "id" in payload:
        routes_list = [payload]
    elif isinstance(payload, list):
        routes_list = payload
    else:
        routes_list = []

    results = []
    for r in routes_list:
        if isinstance(r, dict) and r.get("id"):
            results.append(convert_single_route(r))

    return results


def convert_assign_route_payload(raw_payload: Any) -> Dict[str, Any]:
    """Converter for SHIP_ROUTES_ASSIGN_ROUTE."""
    payload = raw_payload.get("payload", raw_payload) if isinstance(raw_payload, dict) else raw_payload
    return {
        "shipId": payload.get("shipId"),
        "routeId": payload.get("routeId"),
    }


def parse_single_execution(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Parses a single ship route execution record."""
    route_data = payload.get("route")
    route_id = None
    if isinstance(route_data, dict):
        route_id = (
            route_data.get("id")
            or route_data.get("routeId")
            or route_data.get("naturalId")
            or route_data.get("naturalid")
            or route_data.get("routeid")
        )
    elif isinstance(route_data, str):
        route_id = route_data

    return {
        "shipId": payload.get("shipId"),
        "routeId": route_id,
        "state": payload.get("state"),
        "waypointIndex": payload.get("waypointIndex"),
        "stepIndex": payload.get("stepIndex"),
        "flightId": payload.get("flightIdd") or payload.get("flightId"),
        "stepStartedAt": payload.get("stepStartedAt"),
        "stepEndsAt": payload.get("stepEndsAt"),
    }


def convert_ship_routes_execution_payload(raw_payload: Any) -> List[Dict[str, Any]]:
    """Converter for SHIP_ROUTES_EXECUTION (single) and SHIP_ROUTES_EXECUTIONS (list)."""
    payload = raw_payload.get("payload", raw_payload) if isinstance(raw_payload, dict) else raw_payload

    if isinstance(payload, dict) and "executions" in payload:
        items = payload["executions"]
    elif isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict) and "shipId" in payload:
        items = [payload]
    else:
        items = []

    return [parse_single_execution(i) for i in items if isinstance(i, dict) and i.get("shipId")]


def convert_delete_route_payload(raw_payload: Any) -> Dict[str, Any]:
    """Converter for SHIP_ROUTES_DELETE_ROUTE."""
    payload = raw_payload.get("payload", raw_payload) if isinstance(raw_payload, dict) else raw_payload
    return {
        "routeId": payload.get("routeId"),
    }

