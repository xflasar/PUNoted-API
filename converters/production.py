# converters/production.py
import datetime
import hashlib
import json
import string
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union
import data_converter


def convert_production_lines_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'production_lines' table schema."""
    converted_records = []
    for record in raw_records["payload"]["productionLines"]:
        converted_records.append(
            {
                "productionlineid": record.get("id"),
                "siteid": record.get("siteId"),
                "type": record.get("type"),
                "capacity": record.get("capacity"),
                "slots": record.get("slots"),
                "efficiency": record.get("efficiency"),
                "condition": record.get("condition"),
                "orders": convert_production_line_orders_data(record.get("orders")),
                "production_templates": convert_production_line_order_production_templates_data(
                    record.get("productionTemplates"), record.get("id")
                ),
                "efficiency_factors": convert_production_line_efficiency_factors(
                    record.get("efficiencyFactors"), record.get("id")
                ),
                "workforces": convert_production_workforces_data(record.get("workforces"), record.get("id")),
            }
        )
    return {
        "siteid": raw_records["payload"].get("siteId"),
        "production_lines": converted_records,
    }

def convert_production_workforces_data(
    raw_records: List[Dict[str, Any]], production_line_id: string
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'production_workforces' table schema."""
    converted_records = []
    for record in raw_records:
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
    for record in raw_records:
        # Handle created timestamp
        created = record.get("created")
        if created is not None and created.get("timestamp") is not None:
            created = datetime.fromtimestamp(created["timestamp"] / 1000)
        else:
            created = None

        # Handle created timestamp
        started = record.get("started")
        if started is not None and started.get("timestamp") is not None:
            started = datetime.fromtimestamp(started["timestamp"] / 1000)
        else:
            started = None

        # Handle created timestamp
        completion = record.get("completion")
        if completion is not None and completion.get("timestamp") is not None:
            completion = datetime.fromtimestamp(completion["timestamp"] / 1000)
        else:
            completion = None

        # Handle created timestamp
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
                "productionfeeamount": record.get("productionFee").get("amount"),
                "productionfeecurrency": record.get("productionFee").get("currency"),
                "inputs": convert_production_line_order_materials_data(record.get("inputs"), record.get("id"), "input"),
                "outputs": convert_production_line_order_materials_data(
                    record.get("outputs"), record.get("id"), "output"
                ),
            }
        )
    return converted_records

def convert_production_line_order_materials_data(
    raw_records: List[Dict[str, Any]], order_id: string, material_type: string
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'production_line_order_materials' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "orderid": order_id,
                "materialId": record.get("material").get("id"),
                "type": material_type,
                "amount": record.get("amount"),
                "valueAmount": record.get("value").get("amount"),
                "valueCurrency": record.get("value").get("currency"),
            }
        )
    return converted_records

def convert_production_line_order_production_templates_data(
    raw_records: List[Dict[str, Any]], production_line_id: str
) -> List[Dict[str, Any]]:
    converted_records = []
    for record in raw_records:
        duration = record.get("duration")
        if duration is not None and duration.get("millis") is not None:
            duration = duration.get("millis")
        else:
            duration = None

        converted_records.append(
            {
                "productiontemplateid": record.get("id"),
                "productionlineid": production_line_id,
                "name": record.get("name"),
                "duration": duration,
                "efficiency": record.get("efficiency"),
                "effortfactor": record.get("effortFactor"),
                "experience": record.get("experience"),
                "productionfee": record.get("productionFeeFactor").get("amount"),
                "productionfeecurrency": record.get("productionFeeFactor").get("currency"),
                "input_factors": convert_templates_factors_data(
                    record.get("inputFactors"),
                    record.get("id"),
                    "input",
                    production_line_id,
                ),
                "output_factors": convert_templates_factors_data(
                    record.get("outputFactors"),
                    record.get("id"),
                    "output",
                    production_line_id,
                ),
            }
        )
    return converted_records

def convert_templates_factors_data(
    raw_records: List[Dict[str, Any]],
    production_template_id: string,
    material_type: string,
    production_line_id: str,
) -> List[Dict[str, Any]]:
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "productiontemplateid": production_template_id,
                "productionlineid": production_line_id,
                "materialid": record.get("material").get("id"),
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
