# converters/production.py
import datetime
import hashlib
import json
import string
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union


def convert_production_lines_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'production_lines' table schema."""
    converted_records = []
    payload = raw_records.get("payload") if isinstance(raw_records, dict) else {}
    if not isinstance(payload, dict):
        payload = {}
    lines = payload.get("productionLines") or []
    for record in lines:
        if not isinstance(record, dict):
            continue
        converted_records.append(
            {
                "productionlineid": record.get("id"),
                "siteid": record.get("siteId"),
                "type": record.get("type"),
                "capacity": record.get("capacity"),
                "slots": record.get("slots"),
                "efficiency": record.get("efficiency"),
                "condition": record.get("condition"),
                "orders": convert_production_line_orders_data(record.get("orders") or []),
                "production_templates": convert_production_line_order_production_templates_data(
                    record.get("productionTemplates") or [], record.get("id")
                ),
                "efficiency_factors": convert_production_line_efficiency_factors(
                    record.get("efficiencyFactors") or [], record.get("id")
                ),
                "workforces": convert_production_workforces_data(record.get("workforces") or [], record.get("id")),
            }
        )
    return {
        "siteid": payload.get("siteId"),
        "production_lines": converted_records,
    }

def convert_production_workforces_data(
    raw_records: List[Dict[str, Any]], production_line_id: str
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'production_workforces' table schema."""
    converted_records = []
    for record in (raw_records or []):
        if not isinstance(record, dict):
            continue
        converted_records.append(
            {
                "productionlineid": production_line_id,
                "level": record.get("level"),
                "efficiency": record.get("efficiency"),
            }
        )
    return converted_records

def convert_production_line_orders_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'production_line_orders' table schema."""
    converted_records = []
    for record in (raw_records or []):
        if not isinstance(record, dict):
            continue
        # Handle created timestamp
        created = record.get("created")
        if created is not None and created.get("timestamp") is not None:
            created = datetime.fromtimestamp(created["timestamp"] / 1000)
        else:
            created = None

        started = record.get("started")
        if started is not None and started.get("timestamp") is not None:
            started = datetime.fromtimestamp(started["timestamp"] / 1000)
        else:
            started = None

        completion = record.get("completion")
        if completion is not None and completion.get("timestamp") is not None:
            completion = datetime.fromtimestamp(completion["timestamp"] / 1000)
        else:
            completion = None

        lastupdated = record.get("lastUpdated")
        if lastupdated is not None and lastupdated.get("timestamp") is not None:
            lastupdated = datetime.fromtimestamp(lastupdated["timestamp"] / 1000)
        else:
            lastupdated = None

        duration = record.get("duration")
        if duration is not None and duration.get("millis") is not None:
            duration = duration.get("millis")
        else:
            duration = None

        prod_fee = record.get("productionFee") or {}

        converted_records.append(
            {
                "orderid": record.get("id"),
                "productionlineid": record.get("productionLineId"),
                "recipeid": record.get("recipeId"),
                "created": created,
                "started": started,
                "completion": completion,
                "duration": duration,
                "lastupdated": lastupdated,
                "completed": bool(record.get("completed")),
                "halted": record.get("halted"),
                "recurring": record.get("recurring"),
                "productionfeeamount": prod_fee.get("amount"),
                "productionfeecurrency": prod_fee.get("currency"),
                "inputs": convert_production_line_order_materials_data(record.get("inputs") or [], record.get("id"), "input"),
                "outputs": convert_production_line_order_materials_data(
                    record.get("outputs") or [], record.get("id"), "output"
                ),
            }
        )
    return converted_records

def convert_production_line_order_materials_data(
    raw_records: List[Dict[str, Any]], order_id: str, material_type: str
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'production_line_order_materials' table schema."""
    converted_records = []
    for record in (raw_records or []):
        if not isinstance(record, dict):
            continue
        mat = record.get("material") or {}
        val = record.get("value") or {}
        converted_records.append(
            {
                "orderid": order_id,
                "materialId": mat.get("id"),
                "type": material_type,
                "amount": record.get("amount"),
                "valueAmount": val.get("amount"),
                "valueCurrency": val.get("currency"),
            }
        )
    return converted_records

def convert_production_line_order_production_templates_data(
    raw_records: List[Dict[str, Any]], production_line_id: str
) -> List[Dict[str, Any]]:
    converted_records = []
    for record in (raw_records or []):
        if not isinstance(record, dict):
            continue
        duration = record.get("duration")
        if duration is not None and duration.get("millis") is not None:
            duration = duration.get("millis")
        else:
            duration = None

        fee_factor = record.get("productionFeeFactor") or {}

        converted_records.append(
            {
                "productiontemplateid": record.get("id"),
                "productionlineid": production_line_id,
                "name": record.get("name"),
                "duration": duration,
                "efficiency": record.get("efficiency"),
                "effortfactor": record.get("effortFactor"),
                "experience": record.get("experience"),
                "productionfee": fee_factor.get("amount"),
                "productionfeecurrency": fee_factor.get("currency"),
                "input_factors": convert_templates_factors_data(
                    record.get("inputFactors") or [],
                    record.get("id"),
                    "input",
                    production_line_id,
                ),
                "output_factors": convert_templates_factors_data(
                    record.get("outputFactors") or [],
                    record.get("id"),
                    "output",
                    production_line_id,
                ),
            }
        )
    return converted_records

def convert_templates_factors_data(
    raw_records: List[Dict[str, Any]],
    production_template_id: str,
    material_type: str,
    production_line_id: str,
) -> List[Dict[str, Any]]:
    converted_records = []
    for record in (raw_records or []):
        if not isinstance(record, dict):
            continue
        mat = record.get("material") or {}
        converted_records.append(
            {
                "productiontemplateid": production_template_id,
                "productionlineid": production_line_id,
                "materialid": mat.get("id"),
                "factor": record.get("factor"),
            }
        )
    return converted_records

def convert_production_line_efficiency_factors(
    raw_records: List[Dict[str, Any]], production_line_id: string
) -> List[Dict[str, Any]]:
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "productionlineid": production_line_id,
                "expertisecategory": record.get("expertiseCategory", None),
                "type": record.get("type"),
                "effectivity": record.get("effectivity"),
                "value": record.get("value"),
            }
        )
    return converted_records

def convert_production_line_added(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    record = raw_record["payload"]
    order = convert_production_line_orders_data([record])[0]
    return order

def convert_production_line_updated(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    record = raw_record["payload"]
    order = convert_production_line_orders_data([record])[0]
    return order

def convert_production_line_removed(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    payload = raw_record.get("payload") if isinstance(raw_record, dict) else {}
    if not isinstance(payload, dict):
        payload = {}

    order_id = payload.get("orderId") or payload.get("orderid") or payload.get("id")
    production_line_id = (
        payload.get("productionLineId") or payload.get("productionlineid") or payload.get("lineId")
    )

    return {
        "orderid": order_id,
        "productionlineid": production_line_id,
    }

def convert_single_production_line_updated(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converts raw payload from PRODUCTION_PRODUCTION_LINE_UPDATED message.
    Payload contains a single production line object with its orders, templates, and efficiency factors.
    """
    payload = raw_record.get("payload") if isinstance(raw_record, dict) else {}
    if not isinstance(payload, dict):
        payload = {}

    line_id = payload.get("id")
    site_id = payload.get("siteId")

    converted_line = {
        "productionlineid": line_id,
        "siteid": site_id,
        "type": payload.get("type"),
        "capacity": payload.get("capacity"),
        "slots": payload.get("slots"),
        "efficiency": payload.get("efficiency"),
        "condition": payload.get("condition"),
        "orders": convert_production_line_orders_data(payload.get("orders") or []),
        "production_templates": convert_production_line_order_production_templates_data(
            payload.get("productionTemplates") or [], line_id
        ),
        "efficiency_factors": convert_production_line_efficiency_factors(
            payload.get("efficiencyFactors") or [], line_id
        ),
        "workforces": convert_production_workforces_data(payload.get("workforces") or [], line_id),
    }

    return {
        "siteid": site_id,
        "production_line": converted_line,
    }


def convert_multi_site_production_lines(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converts raw payload from PRODUCTION_PRODUCTION_LINES message.
    Payload contains productionLines across multiple sites. Groups them by siteId.
    Returns: { "sites": { site_id: { "siteid": site_id, "production_lines": [...] } } }
    """
    payload = raw_record.get("payload") if isinstance(raw_record, dict) else {}
    if not isinstance(payload, dict):
        payload = {}

    lines = payload.get("productionLines") or []
    grouped_by_site = {}

    for record in lines:
        if not isinstance(record, dict):
            continue
        site_id = record.get("siteId")
        if not site_id:
            continue

        if site_id not in grouped_by_site:
            grouped_by_site[site_id] = []

        line_id = record.get("id")
        grouped_by_site[site_id].append(
            {
                "productionlineid": line_id,
                "siteid": site_id,
                "type": record.get("type"),
                "capacity": record.get("capacity"),
                "slots": record.get("slots"),
                "efficiency": record.get("efficiency"),
                "condition": record.get("condition"),
                "orders": convert_production_line_orders_data(record.get("orders") or []),
                "production_templates": convert_production_line_order_production_templates_data(
                    record.get("productionTemplates") or [], line_id
                ),
                "efficiency_factors": convert_production_line_efficiency_factors(
                    record.get("efficiencyFactors") or [], line_id
                ),
                "workforces": convert_production_workforces_data(record.get("workforces") or [], line_id),
            }
        )

    return {
        "sites": {
            site_id: {"siteid": site_id, "production_lines": line_list}
            for site_id, line_list in grouped_by_site.items()
        }
    }


