# converters/workforce.py
import datetime
import hashlib
import json
import string
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union
import data_converter


def convert_workforces_data(raw_records: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'workforces' table schema."""
    payload = raw_records["payload"]
    converted_records = []
    for record in payload["workforces"]:
        site_id = payload.get("siteId")
        level = record.get("level")

        # 1. CREATE PRIMARY KEY for 'workforces' table
        workforce_id = f"{site_id}-{level}"

        needs = convert_workforce_needs_data(record.get("needs", []), workforce_id)

        converted_records.append(
            {
                "workforceid": workforce_id,
                "siteid": site_id,
                "level": level,
                "population": record.get("population"),
                "reserve": record.get("reserve"),
                "capacity": record.get("capacity"),
                "required": record.get("required"),
                "satisfaction": record.get("satisfaction"),
                "needs": needs,
            }
        )
    return converted_records

def convert_workforce_needs_data(raw_records: List[Dict[str, Any]], workforce_id: str) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'workforceNeeds' table schema."""
    converted_records = []
    for record in raw_records:
        material_id = record.get("material", {}).get("id")
        category = record.get("category")

        # 1. CREATE PRIMARY KEY for 'workforceNeeds' table (using three components)
        # This ensures the specific material need is unique for the specific workforce level.
        workforce_need_id = f"{workforce_id}-{material_id}-{category}"

        converted_records.append(
            {
                "workforceneedid": workforce_need_id,
                "workforceid": workforce_id,
                "materialid": record.get("material").get("id"),
                "category": record.get("category"),
                "essential": record.get("essential"),
                "satisfaction": record.get("satisfaction"),
                "unitsperinterval": record.get("unitsPerInterval"),
                "unitsper100": record.get("unitsPer100"),
            }
        )
    return converted_records

def convert_site_available_population_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    record = raw_records["payload"]
    workforce = record.get("availableReserveWorkforce")
    converted_data = {
        "siteid": record.get("siteId"),
        "pioneer": workforce.get("PIONEER"),
        "settler": workforce.get("SETTLER"),
        "engineer": workforce.get("ENGINEER"),
        "scientist": workforce.get("SCIENTIST"),
        "technician": workforce.get("TECHNICIAN"),
    }
    return converted_data
