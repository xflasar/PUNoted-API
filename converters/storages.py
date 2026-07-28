# converters/storages.py
import datetime
import hashlib
import json
import string
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union


def convert_storages_data(raw_records: List[Dict[str, Any]], full_refresh: bool = False) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'storages' table schema."""
    storages = []
    for record in raw_records["payload"]["stores"]:
        storages_items = []
        storage = {
            "storageid": record.get("id"),
            "addressableid": record.get("addressableId"),
            "name": record.get("name") if record.get("name") is not None else "null",
            "weightload": record.get("weightLoad"),
            "weightcapacity": record.get("weightCapacity"),
            "volumeload": record.get("volumeLoad"),
            "volumecapacity": record.get("volumeCapacity"),
            "fixed": record.get("fixed"),
            "tradestore": record.get("tradeStore"),
            "rank": record.get("rank"),
            "locked": record.get("locked"),
            "type": record.get("type"),
            "xata_updatedat": datetime.fromtimestamp(datetime.now().timestamp()),
        }
        for item in record.get("items", []):
            # Skip items of type 'BLOCKED'
            if item.get("type") == "BLOCKED":
                storages_items.append(
                    {
                        "storageid": record.get("id"),
                        "materialid": item.get("id"),
                        "quantity": None,
                        "totalweight": item.get("weight"),
                        "totalvolume": item.get("volume"),
                        "currencyamount": None,
                        "currencytype": None,
                        "type": item.get("type"),
                    }
                )
                continue

            # Skip the item if the 'quantity' key is missing
            if item.get("quantity") is None:
                storages_items.append(
                    {
                        "storageid": record.get("id"),
                        "materialid": item.get("id"),
                        "quantity": None,
                        "totalweight": item.get("weight"),
                        "totalvolume": item.get("volume"),
                        "currencyamount": None,
                        "currencytype": None,
                        "type": item.get("type"),
                    }
                )
                continue

            quantity_data = item.get("quantity")
            currency_value = quantity_data.get("value", {})

            storages_items.append(
                {
                    "storageid": record.get("id"),
                    "materialid": item.get("id"),
                    "quantity": quantity_data.get("amount"),
                    "totalweight": item.get("weight"),
                    "totalvolume": item.get("volume"),
                    "currencyamount": currency_value.get("amount"),
                    "currencytype": currency_value.get("currency"),
                    "type": item.get("type"),
                }
            )
        storage["storage_items"] = storages_items
        storages.append(storage)
    return {"full_refresh": full_refresh, "storages": storages}

def convert_full_refresh_storage_data(
    raw_records: List[Dict[str, Any]],
) -> [Dict[str, Any]]:
    """Converts raw data to match the 'storages' table schema for full refresh."""
    return convert_storages_data(raw_records, full_refresh=True)

def convert_storage_removed(raw_record: Dict[str, Any]) -> List[Dict[str, Any]]:
    converted_record = []
    for storeid in raw_record["payload"]["storeIds"]:
        converted_record.append({"storageid": storeid, "removed": True})
    return converted_record

def convert_warehouses_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'warehouses' table schema."""
    converted_records = []
    for record in raw_records["payload"]["storages"]:
        # Handle founded timestamp
        next_payment = record.get("nextPayment")
        if next_payment is not None and next_payment.get("timestamp") is not None:
            next_payment = datetime.fromtimestamp(next_payment["timestamp"] / 1000)
        else:
            next_payment = None

        converted_records.append(
            {
                "warehouseid": record.get("warehouseId"),
                "storeid": record.get("storeId"),
                "units": record.get("units"),
                "weightcapacity": record.get("weightCapacity"),
                "volumecapacity": record.get("volumeCapacity"),
                "nextpayment": next_payment,
                "feeamount": record.get("fee").get("amount"),
                "feecurrency": record.get("fee").get("currency"),
                "status": record.get("status"),
                "addresssystem": record.get("address").get("lines")[0].get("entity").get("id"),
                "addressplanet": record.get("address").get("lines")[1].get("entity").get("id"),
            }
        )
    return converted_records

def convert_storage_items_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'storage_items' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "storageId": record.get("storageId"),
                "materialId": record.get("materialId"),
                "quantity": record.get("quantity"),
                "totalWeight": record.get("totalWeight"),
                "totalVolume": record.get("totalVolume"),
                "currencyAmount": record.get("currencyAmount"),
                "currencyType": record.get("currencyType"),
            }
        )
    return converted_records
