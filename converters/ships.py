# converters/ships.py
import datetime
import hashlib
import json
import string
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union
import data_converter

def get_entity_id(lines, entity_type):
    """
    Helper function to safely extract the ID of a specific entity type
    (SYSTEM, PLANET, or STATION) from the 'lines' list.
    """
    if lines:
        for line in lines:
            if line.get("type") == entity_type and "entity" in line:
                return line["entity"].get("id")
    return None

def get_total_fuel_consumption(segments, fuel_type):
    """
    Helper function to calculate the total fuel consumption (STL or FTL)
    across all segments.
    """
    total_consumption = 0
    consumption_key = f"{fuel_type}FuelConsumption"
    for segment in segments:
        consumption = segment.get(consumption_key)
        if consumption is not None:
            total_consumption += consumption
    return total_consumption

def get_total_damage(segments):
    """
    Helper function to calculate the total damage across all segments.
    """
    total_damage = 0.0
    for segment in segments:
        damage = segment.get("damage")
        if damage is not None:
            total_damage += damage
    return total_damage



def convert_ships_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'ships' table schema."""
    payload = raw_records["payload"]
    # Determine the format of the incoming data
    if isinstance(payload, dict) and "ships" in payload:
        # Case 1: The data is a dictionary with a 'ships' key containing a list
        records_to_process = payload["ships"]
    elif isinstance(payload, dict) and "id" in payload:
        # Case 2: The data is a single ship record dictionary.
        # Wrap it in a list to process it in the loop.
        records_to_process = [payload]
    else:
        # Case 3: The data is already a list of ships (or an empty list)
        records_to_process = payload

    # Now, the rest of your code can safely assume `records_to_process` is a list
    if not records_to_process:
        print("No records to process.")
        return

    converted_records = []

    for record in records_to_process:
        # Handle created timestamp
        last_repair = record.get("lastRepair")
        if last_repair is not None and last_repair.get("timestamp") is not None:
            last_repair = datetime.fromtimestamp(last_repair["timestamp"] / 1000)
        else:
            last_repair = None

        # Handle created timestamp
        commissioning_time = record.get("commissioningTime")
        if commissioning_time is not None and commissioning_time.get("timestamp") is not None:
            commissioning_time = datetime.fromtimestamp(commissioning_time["timestamp"] / 1000)
        else:
            commissioning_time = None

        repair_materials = []
        for material in record.get("repairMaterials"):
            repair_materials.append(
                {
                    "materialid": material.get("material").get("id"),
                    "amount": material.get("amount"),
                    "shipid": record.get("id"),
                }
            )

        addressSystemId = None
        addressPlanetId = None
        addressStationId = None

        if record.get("flightId") is None:
            adressEntity = record.get("address").get("lines")[0].get("entity")
            if record.get("address").get("lines")[0].get("type") != "SYSTEM":
                print(
                    f"Warning: Expected SYSTEM type for address line 0, got {record.get('address').get('lines')[0].get('type')}"
                )
            addressSystemId = adressEntity.get("id")

            adressEntity = record.get("address").get("lines")[1].get("entity")
            if record.get("address").get("lines")[1].get("type") == "PLANET":
                addressPlanetId = adressEntity.get("id")
            elif record.get("address").get("lines")[1].get("type") == "STATION":
                addressStationId = adressEntity.get("id")
            else:
                print(
                    f"Warning: Unexpected type for address line 1: {record.get('address').get('lines')[1].get('type')}"
                )

        converted_records.append(
            {
                "shipid": record.get("id"),
                "idshipstore": record.get("idShipStore"),
                "idstlfuelstore": record.get("idStlFuelStore"),
                "idftlfuelstore": record.get("idFtlFuelStore"),
                "registration": record.get("registration"),
                "name": record.get("name"),
                "commissioningtime": commissioning_time,
                "blueprintnaturalid": record.get("blueprintNaturalId"),
                "addresssystemid": addressSystemId,
                "addressplanetid": addressPlanetId,
                "addressstationid": addressStationId,
                "flightid": record.get("flightId"),
                "acceleration": record.get("acceleration"),
                "thrust": record.get("thrust"),
                "mass": record.get("mass"),
                "operatingemptymass": record.get("operatingEmptyMass"),
                "volume": record.get("volume"),
                "reactorpower": record.get("reactorPower"),
                "emitterpower": record.get("emitterPower"),
                "stlfuelflowrate": record.get("stlFuelFlowRate"),
                "operatingtimestl": record.get("operatingTimeStl").get("millis"),
                "operatingtimeftl": record.get("operatingTimeFtl").get("millis"),
                "condition": record.get("condition"),
                "lastrepair": last_repair,
                "status": record.get("status"),
                "type": record.get("type"),
                "repair_materials": repair_materials,
            }
        )
    return converted_records

def convert_ship_repair_materials_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'ship_repair_materials' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "shipId": record.get("shipId"),
                "materialId": record.get("materialId"),
                "amount": record.get("amount"),
            }
        )
    return converted_records

def convert_flight_records(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    records = raw_records["payload"]
    converted_records = []
    for record in records["flights"]:
        converted_record = convert_flight_record(record)
        converted_records.append(converted_record)
    return converted_records

def convert_flight_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converts a single raw flight data record into a flat object with lowercase keys
    based on the desired schema, calculating aggregate values from segments.
    """

    record = record.get("payload", record)

    # 1. Arrival/Departure Timestamps and Datetime Objects
    arrival_ts_ms = record.get("arrival", {}).get("timestamp")
    departure_ts_ms = record.get("departure", {}).get("timestamp")

    arrival = None
    if arrival_ts_ms is not None:
        arrival = datetime.fromtimestamp(arrival_ts_ms / 1000)
    else:
        arrival = None

    departure = None
    if departure_ts_ms is not None:
        departure = datetime.fromtimestamp(departure_ts_ms / 1000)
    else:
        departure = None

    # 2. Extract Origin/Destination IDs
    origin_lines = record.get("origin", {}).get("lines", [])
    destination_lines = record.get("destination", {}).get("lines", [])

    origin_system_id = get_entity_id(origin_lines, "SYSTEM")
    origin_planet_id = get_entity_id(origin_lines, "PLANET")
    origin_station_id = get_entity_id(origin_lines, "STATION")

    destination_system_id = get_entity_id(destination_lines, "SYSTEM")
    destination_planet_id = get_entity_id(destination_lines, "PLANET")
    destination_station_id = get_entity_id(destination_lines, "STATION")

    # 3. Aggregate Segment Data
    segments = record.get("segments", [])

    stl_total_consumption = get_total_fuel_consumption(segments, "stl")
    ftl_total_consumption = get_total_fuel_consumption(segments, "ftl")
    total_damage = get_total_damage(segments)

    # Note: stlDistance and ftlDistance are taken from the top level,
    # as per the structure, which likely represents the total distance.

    # 4. Construct the converted record with lowercase keys
    converted_record = {
        "id": record.get("id"),
        "aborted": record.get("aborted"),
        "damage": total_damage,
        "shipid": record.get("shipId"),
        # Origin/Destination IDs
        "originsystemid": origin_system_id,
        "originplanetid": origin_planet_id,
        "originstationid": origin_station_id,
        "destinationsystemid": destination_system_id,
        "destinationplanetid": destination_planet_id,
        "destinationstationid": destination_station_id,
        "currentsegmentindex": record.get("currentSegmentIndex"),
        "segments": [
            convert_segment(segment, record.get("id"), index)
            for index, segment in enumerate(segments)
            if convert_segment(segment, record.get("id"), index) is not None
        ],
        # Timestamps FIX THIS ITS INVERTED!!!!
        "departuretimestamp": arrival,
        "arrivaltimestamp": departure,
        # Distance and Fuel Consumption
        "stldistance": record.get("stlDistance"),
        "ftldistance": record.get("ftlDistance"),
        "stltotalconsumption": stl_total_consumption,
        "ftltotalconsumption": ftl_total_consumption,
    }

    return converted_record

def convert_flight_ended_record(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    record = raw_record.get("payload", {})

    def get_entity_id(lines, entity_type):
        if not lines:
            return None
        for line in lines:
            if line.get("type") == entity_type and "entity" in line:
                return line["entity"].get("id")
        return None

    # FIX: Generating a timezone-aware UTC datetime
    return {
        "id": record.get("id"),
        "shipId": record.get("shipId"),
        "destinationSystemId": get_entity_id(record.get("destination", {}).get("lines", []), "SYSTEM"),
        "destinationPlanetId": get_entity_id(record.get("destination", {}).get("lines", []), "PLANET"),
        "destinationStationId": get_entity_id(record.get("destination", {}).get("lines", []), "STATION"),
        "ended_at": datetime.now(timezone.utc)
    }

def convert_segment(raw_segment: Dict[str, Any], flight_id: str, segment_index: int) -> Optional[Dict[str, Any]]:
    """
    Converts a raw flight segment JSON object into a standardized dictionary
    for database upsert, handling different segment types and nested data.

    Args:
        raw_segment: The raw JSON dictionary for a single segment.
        flight_id: The ID of the parent flight (to link the segment).
        segment_index: The order of the segment within the flight.

    Returns:
        A standardized dictionary of segment data, or None if the segment is invalid.
    """
    if not raw_segment or not isinstance(raw_segment, dict):
        return None

    segment_type = raw_segment.get("type")

    # --- Helper to extract entity details from a location dictionary ---
    def extract_location_details(location: Dict[str, Any]) -> Dict[str, Any]:
        details = {
            "system_id": None,
            "station_id": None,
            "planet_id": None,
            "orbit_semi_major_axis": None,
            "orbit_eccentricity": None,
            "orbit_inclination": None,
            "orbit_periapsis": None,
            "orbit_right_ascension": None,
            "location_type": None,  # e.g., 'STATION' or 'ORBIT'
        }

        lines = location.get("lines", [])
        if not lines:
            return details

        for line in lines:
            entity = line.get("entity")
            line_type = line.get("type")

            if entity and line_type == "SYSTEM":
                details["system_id"] = entity.get("id")

            elif entity and line_type == "STATION":
                details["station_id"] = entity.get("id")
                details["location_type"] = "STATION"

            elif entity and line_type == "PLANET":
                details["station_id"] = entity.get("id")
                details["location_type"] = "PLANET"

            elif line.get("orbit") and line_type == "ORBIT":
                orbit = line["orbit"]
                details["orbit_semi_major_axis"] = orbit.get("semiMajorAxis")
                details["orbit_eccentricity"] = orbit.get("eccentricity")
                details["orbit_inclination"] = orbit.get("inclination")
                details["orbit_periapsis"] = orbit.get("periapsis")
                details["orbit_right_ascension"] = orbit.get("rightAscension")
                details["location_type"] = "ORBIT"

        return details

    # --- Extract core fields ---
    departure_ts_ms = raw_segment.get("departure", {}).get("timestamp")
    arrival_ts_ms = raw_segment.get("arrival", {}).get("timestamp")

    # Calculate duration in seconds
    duration_s = None
    if departure_ts_ms and arrival_ts_ms:
        duration_s = (arrival_ts_ms - departure_ts_ms) / 1000

    # Extract Origin/Destination details
    origin_details = extract_location_details(raw_segment.get("origin", {}))
    destination_details = extract_location_details(raw_segment.get("destination", {}))

    origin_orbit_data = {
        "semimajoraxis": origin_details["orbit_semi_major_axis"],
        "eccentricity": origin_details["orbit_eccentricity"],
        "inclination": origin_details["orbit_inclination"],
        "periapsis": origin_details["orbit_periapsis"],
        "rightascension": origin_details["orbit_right_ascension"],
    }

    destination_orbit_data = {
        "semimajoraxis": destination_details["orbit_semi_major_axis"],
        "eccentricity": destination_details["orbit_eccentricity"],
        "inclination": destination_details["orbit_inclination"],
        "periapsis": destination_details["orbit_periapsis"],
        "rightascension": destination_details["orbit_right_ascension"],
    }
    transferEllipse_raw = raw_segment.get("transferEllipse")
    transferEllipse = {}
    if transferEllipse_raw:
        transferEllipse = {
            "alpha": transferEllipse_raw.get("alpha"),
            "semimajoraxis": transferEllipse_raw.get("semiMajorAxis"),
            "semiminoraxis": transferEllipse_raw.get("semiMinorAxis"),
            "centerx": transferEllipse_raw.get("center").get("x"),
            "centery": transferEllipse_raw.get("center").get("y"),
            "centerz": transferEllipse_raw.get("center").get("z"),
            "startpositionx": transferEllipse_raw.get("startPosition").get("x"),
            "startpositiony": transferEllipse_raw.get("startPosition").get("y"),
            "startpositionz": transferEllipse_raw.get("startPosition").get("z"),
            "targetpositionx": transferEllipse_raw.get("targetPosition").get("x"),
            "targetpositiony": transferEllipse_raw.get("targetPosition").get("y"),
            "targetpositionz": transferEllipse_raw.get("targetPosition").get("z"),
        }

    # --- Standardized Segment Record ---
    segment_record = {
        # Linkage/Metadata
        "flight_id": flight_id,
        "segment_index": segment_index,
        "segment_type": segment_type,
        # Timing
        "departure": departure_ts_ms,
        "arrival": arrival_ts_ms,
        "duration": duration_s,
        # Origin
        "origin_system_id": origin_details["system_id"],
        "origin_location_id": origin_details["station_id"],
        "origin_orbit_data": json.dumps(origin_orbit_data),
        "origin_location_type": origin_details["location_type"],
        # Destination
        "destination_system_id": destination_details["system_id"],
        "destination_location_id": destination_details["station_id"],
        "destination_orbit_data": json.dumps(destination_orbit_data),
        "destination_location_type": destination_details["location_type"],
        "stl_distance": raw_segment.get("stlDistance"),
        "stl_fuel": raw_segment.get("stlFuelConsumption"),
        "ftl_distance": raw_segment.get("ftlDistance"),
        "ftl_fuel": raw_segment.get("ftlFuelConsumption"),
        "damage": raw_segment.get("damage"),
        "transferEllipse": json.dumps(transferEllipse),
    }

    # --- Type-Specific Adjustments ---

    # 1. JUMP/CHARGE Segments (Focus on FTL stats)
    if segment_type in ["JUMP", "CHARGE"]:
        pass

    # 2. APPROACH/LANDING Segments (Similar to DEPARTURE, focused on STL)
    elif segment_type in ["APPROACH", "LANDING"]:
        pass

    # 3. DEPARTURE Segment (Focus on STL/Transfer Ellipse)
    elif segment_type == "DEPARTURE":
        pass

    return segment_record
