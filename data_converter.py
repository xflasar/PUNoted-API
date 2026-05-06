# data_converter.py
import datetime
import hashlib
import json
import string
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

from converters.gateway import convert_gateway_data

# ==============================================================================
# CONVERSION FUNCTIONS FOR EACH DATABASE TABLE
# Each function takes raw JSON data and transforms it into a list of dictionaries
# formatted for a specific table.
# ==============================================================================

# FIXME: ALL OF THESE NEEDS TO GO INTO CONVERTERS FOLDER SEPARATELY MOST OF THEM ARE TEMPLATES AND WHOLE FILE IS JUST MESS NEED REFACTORING PRIORITY 1

def convert_users_data_table(raw_records: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Converts raw data to match the 'users_data' table schema, handling
    None values by replacing them with a specific default value.
    """
    converted_records = []

    # Safely get the payload, defaulting to an empty dict if not found
    payload = raw_records.get("payload", {})

    # Helper function to get a value and replace None with a specified default
    def get_value_or_default(key: str, default: Any = None):
        value = payload.get(key)
        return value if value is not None else default

    # Handle subscription expiry timestamp
    subscription_expiry_data = payload.get("subscriptionExpiry")
    if subscription_expiry_data is not None and subscription_expiry_data.get("timestamp") is not None:
        subscription_expiry = datetime.fromtimestamp(subscription_expiry_data["timestamp"] / 1000)
    else:
        subscription_expiry = None

    # Handle created timestamp
    created_data = payload.get("created")
    if created_data is not None and created_data.get("timestamp") is not None:
        created = datetime.fromtimestamp(created_data["timestamp"] / 1000)
    else:
        created = None

    converted_records.append(
        {
            "userid": get_value_or_default("id", "null"),
            "displayname": get_value_or_default("username", "null"),
            "companyid": get_value_or_default("companyId", "null"),
            "subscriptionlevel": get_value_or_default("subscriptionLevel", "null"),
            "subscriptionexpiry": subscription_expiry,
            "created": created,
            "preferredlocale": get_value_or_default("preferredLocale", "null"),
            "highesttier": get_value_or_default("highestTier", "null"),
            "ispayinguser": get_value_or_default("isPayingUser", "null"),
            "ismuted": get_value_or_default("isMuted", "null"),
            "preferredlocale": get_value_or_default("preferredLocale", "null"),
        }
    )

    return converted_records


def convert_user_gifts_received_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'user_gifts_received' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "id": record.get("id"),
                "userId": record.get("userId"),
                "giftId": record.get("giftId"),
            }
        )
    return converted_records


def convert_user_gifts_sent_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'user_gifts_sent' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "id": record.get("id"),
                "userId": record.get("userId"),
                "giftId": record.get("giftId"),
            }
        )
    return converted_records


def convert_user_starting_profiles_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Converts raw data to match the 'user_starting_profiles' table schema.
    Note: 'baseMaterials', 'buildingTickers', 'workforce', and 'commodities' are JSON columns.
    """
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "name": record.get("name"),
                "ships": record.get("ships"),
                "baseMaterials": record.get("baseMaterials"),
                "buildingTickers": record.get("buildingTickers"),
                "workforce": record.get("workforce"),
                "commodities": record.get("commodities"),
            }
        )
    return converted_records

def convert_public_user_data(raw_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Converts raw user data to match the 'public_users_data' table schema.
    Accepts either a single dictionary or a list of dictionaries.
    """
    payload = raw_payload["payload"]
    if isinstance(payload, dict):
        raw_records = [payload]
    else:
        raw_records = payload

    converted_records = []
    for record in raw_records:
        # Extract the payload whether it is nested under 'body' or passed directly
        data = record
        
        company = data.get("company") or {}
        created = data.get("created") or {}
        gifts = data.get("gifts") or {}

        converted_records.append(
            {
                "id": data.get("id"),
                "username": data.get("username"),
                "company_id": company.get("id"),
                "company_name": company.get("name"),
                "company_code": company.get("code"),
                "subscription_level": data.get("subscriptionLevel"),
                "highest_tier": data.get("highestTier"),
                "pioneer": data.get("pioneer", False),
                "moderator": data.get("moderator", False),
                "team": data.get("team", False),
                "translator": data.get("translator", False),
                "active_days_per_week": data.get("activeDaysPerWeek"),
                "created_timestamp": created.get("timestamp"),
                # Serialize JSONB payload for database insertion
                "gifts": json.dumps(gifts) if gifts else None 
            }
        )
    return converted_records

def convert_user_tokens_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'user_tokens' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "id": record.get("id"),
                "userId": record.get("userId"),
                "token": record.get("token"),
                "refreshToken": record.get("refreshToken"),
                "expiresAt": record.get("expiresAt"),
            }
        )
    return converted_records


def convert_user_data_tokens_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'user_data_tokens' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "id": record.get("id"),
                "userId": record.get("userId"),
                "token": record.get("token"),
                "permissions": record.get("permissions"),
                "status": record.get("status"),
                "createdAt": record.get("createdAt"),
                "expiresAt": record.get("expiresAt"),
            }
        )
    return converted_records


def convert_company_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'company_data' table schema."""
    converted_records = []
    company_data = {}
    representation = {}
    representationContrubutors = []
    rating_report = {}
    headquarters = {}
    headquarters_upgrade_items = []
    headquarters_efficiency_gains = []
    headquarters_efficiency_gains_next_level = []

    record = raw_records["payload"]

    representation = {
        "representationid": uuid.uuid4(),
        "contributednextlevelamount": record.get("representation").get("contributedNextLevel").get("amount"),
        "contributednextlevelcurrency": record.get("representation").get("contributedNextLevel").get("currency"),
        "contributedtotalamount": record.get("representation").get("contributedTotal").get("amount"),
        "contributedtotalcurrency": record.get("representation").get("contributedTotal").get("currency"),
        "currentlevel": record.get("representation").get("currentLevel"),
        "costnextlevelamount": record.get("representation").get("costNextLevel").get("amount"),
        "costnextlevelcurrency": record.get("representation").get("costNextLevel").get("currency"),
        "leftnextlevelamount": record.get("representation").get("leftNextLevel").get("amount"),
        "leftnextlevelcurrency": record.get("representation").get("leftNextLevel").get("currency"),
    }
    for contributor in record.get("representation").get("contributors"):
        print("Company data representation contributors - Fail not finished!!")

    # Handle earliest contract timestamp
    earliest_contract = record.get("ratingReport").get("earliestContract")
    if earliest_contract is not None and earliest_contract.get("timestamp") is not None:
        earliest_contract = datetime.fromtimestamp(earliest_contract["timestamp"] / 1000)
    else:
        earliest_contract = None  # Or None, 0, etc. based on your needs

    rating_report = {
        "contractcount": record.get("ratingReport").get("contractCount"),
        "earliestcontract": earliest_contract,
        "overallrating": record.get("ratingReport").get("overallRating"),
    }

    # Handle subscription expiry timestamp
    next_relocation_time = record.get("headquarters").get("nextRelocationTime")
    if next_relocation_time is not None and next_relocation_time.get("timestamp") is not None:
        next_relocation_time = datetime.fromtimestamp(next_relocation_time["timestamp"] / 1000)
    else:
        next_relocation_time = None  # Or None, 0, etc. based on your needs

    headquarters = {
        "addresssystemid": record.get("headquarters").get("address").get("lines")[0].get("entity").get("id"),
        "addressplanetid": record.get("headquarters").get("address").get("lines")[1].get("entity").get("id"),
        "headquarterslevel": record.get("headquarters").get("level"),
        "nextrelocationtime": next_relocation_time,
        "relocationlocked": record.get("headquarters").get("relocationLocked"),
        "basepermits": record.get("headquarters").get("basePermits"),
        "usedbasepermits": record.get("headquarters").get("usedBasePermits"),
        "additionalbasepermits": record.get("headquarters").get("additionalBasePermits"),
        "additionalproductionqueueslots": record.get("headquarters").get("additionalProductionQueueSlots"),
    }

    for item in record.get("headquarters").get("inventory").get("items"):
        headquarters_upgrade_items.append(
            {
                "materialid": item.get("material").get("id"),
                "amount": item.get("amount"),
                "amountlimit": item.get("limit"),
            }
        )

    for efficiency_gain in record.get("headquarters").get("efficiencyGains"):
        headquarters_efficiency_gains.append(
            {
                "category": efficiency_gain.get("category"),
                "gain": efficiency_gain.get("gain"),
            }
        )

    for efficiency_gain in record.get("headquarters").get("efficiencyGainsNextLevel"):
        headquarters_efficiency_gains_next_level.append(
            {
                "category": efficiency_gain.get("category"),
                "gain": efficiency_gain.get("gain"),
            }
        )

    converted_records = {
        "company_data": {
            "companyid": record.get("id"),
            "companyname": record.get("name"),
            "companycode": record.get("code"),
            "startinglocationsystemid": record.get("startingLocation").get("lines")[0].get("entity").get("id"),
            "startinglocationplanetid": record.get("startingLocation").get("lines")[1].get("entity").get("id"),
            "startingprofile": record.get("startingProfile"),
            "countryid": record.get("countryId"),
        },
        "representation": representation,
        "representationContributors": representationContrubutors,
        "ratingReport": rating_report,
        "headquarters": headquarters,
        "headquartersUpgradeItems": headquarters_upgrade_items,
        "headquarters_efficiency_gains": headquarters_efficiency_gains,
        "headquarters_efficiency_gains_next_level": headquarters_efficiency_gains_next_level,
    }
    return converted_records


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


def convert_headquarters_upgrade_items_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'headquarters_upgrade_items' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "id": record.get("id"),
                "headquartersId": record.get("headquartersId"),
                "materialId": record.get("materialId"),
                "amount": record.get("amount"),
                "limit": record.get("limit"),
            }
        )
    return converted_records


def convert_storage_removed(raw_record: Dict[str, Any]) -> List[Dict[str, Any]]:
    converted_record = []
    for storeid in raw_record["payload"]["storeIds"]:
        converted_record.append({"storageid": storeid, "removed": True})
    return converted_record


def convert_full_refresh_storage_data(
    raw_records: List[Dict[str, Any]],
) -> [Dict[str, Any]]:
    """Converts raw data to match the 'storages' table schema for full refresh."""
    return convert_storages_data(raw_records, full_refresh=True)


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
            # Skip items of type 'BLOCKED' as per your original logic
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

            # New check: Skip the item if the 'quantity' key is missing
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


def convert_gateway_data_wrapper(raw_payload: Dict[str, Any]) -> Dict[str, Any]:
    """Converts raw data to match the 'gateways' table schema."""
    return convert_gateway_data(raw_payload)


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
    converted_record = {}

    record = raw_record["payload"]

    converted_record = {
        "orderid": record.get("orderId"),
        "productionlineid": record.get("productionLineId"),
    }

    return converted_record


# ship flight
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
        "damage": total_damage,  # Use aggregated damage from segments
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

def convert_flight_records(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    records = raw_records["payload"]
    converted_records = []
    for record in records["flights"]:
        converted_record = convert_flight_record(record)
        converted_records.append(converted_record)
    return converted_records


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
            # 🌟 FIX: Pass the datetime object directly to the query
            last_repair = datetime.fromtimestamp(last_repair["timestamp"] / 1000)
        else:
            last_repair = None  # Use None instead of 'null' for a null value

        # Handle created timestamp
        commissioning_time = record.get("commissioningTime")
        if commissioning_time is not None and commissioning_time.get("timestamp") is not None:
            # 🌟 FIX: Pass the datetime object directly to the query
            commissioning_time = datetime.fromtimestamp(commissioning_time["timestamp"] / 1000)
        else:
            commissioning_time = None  # Use None instead of 'null' for a null value

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


# --- HELPER: DATE/TIME CONVERSION ---


def _convert_millis_to_datetime(
    millis: Union[int, float, None],
) -> Union[datetime, None]:
    """
    Converts a UNIX timestamp in milliseconds (int or float) to a datetime object.
    Returns None if the input is falsy or invalid.
    """
    if not millis or not isinstance(millis, (int, float)):
        return None
    try:
        # Convert milliseconds to seconds before using fromtimestamp
        return datetime.fromtimestamp(millis / 1000)
    except ValueError:
        return None


# --- HELPER: ADDRESS PARSING ---


def _parse_address_lines(address_lines: List[Dict[str, Any]], address_type: str) -> Dict[str, Any]:
    """
    Helper to extract entity data from the complex 'address.lines' array.
    """
    system_data = next((line["entity"] for line in address_lines if line["type"] == "SYSTEM"), {})
    planet_data = next((line["entity"] for line in address_lines if line["type"] == "PLANET"), {})
    station_data = next((line["entity"] for line in address_lines if line["type"] == "STATION"), {})

    prefix = "destination" if address_type == "DESTINATION" else "address"

    data = {}
    data[f"{prefix}systemid"] = system_data.get("id")
    data[f"{prefix}planetid"] = planet_data.get("id")
    data[f"{prefix}stationid"] = station_data.get("id") if station_data else None

    return data


# --- CONVERTER 1: MAIN CONTRACT ---


def _convert_contract_main(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Converts raw contract data to match the 'contracts' table schema.
    Applies DATETIME conversion to all timestamp fields.
    """
    converted_records = []
    for record in raw_records:
        partner = record.get("partner", {})

        date = record.get("date")
        if date is not None and date.get("timestamp") is not None:
            date = datetime.fromtimestamp(date["timestamp"] / 1000)
        else:
            date = None

        due_date = record.get("dueDate")
        if due_date is not None and due_date.get("timestamp") is not None:
            due_date = datetime.fromtimestamp(due_date["timestamp"] / 1000)
        else:
            due_date = None

        extension_deadline = record.get("extensionDeadline")
        if extension_deadline is not None and extension_deadline.get("timestamp") is not None:
            extension_deadline = datetime.fromtimestamp(extension_deadline["timestamp"] / 1000)
        else:
            extension_deadline = None

        converted_records.append(
            {
                "id": record.get("id"),
                "localid": record.get("localId"),
                "date": date,
                "party": record.get("party"),
                "partnerid": partner.get("id") or partner.get("agentId"),
                "partnername": partner.get("name"),
                "partnercode": partner.get("code"),
                "status": record.get("status"),
                "duedate": due_date,
                "name": record.get("name"),
                "preamble": record.get("preamble"),
                "extensiondeadline": extension_deadline,
                "relatedcontracts": json.dumps(record.get("relatedContracts", [])),
                "contracttype": record.get("contractType"),
                "terminationreceived": record.get("terminationReceived"),
                "terminationsent": record.get("terminationSent"),
                "agentcontract": record.get("agentContract"),
                "canextend": record.get("canExtend"),
                "canrequesttermination": record.get("canRequestTermination"),
            }
        )
    return converted_records


# --- CONVERTER 2: CONDITIONS ---


def _convert_contract_conditions(
    raw_records: List[Dict[str, Any]], contract_id: str, party: str
) -> List[Dict[str, Any]]:
    """
    Converts raw condition data to match the 'contract_conditions' table schema.
    Applies DATETIME conversion and extracts the raw 'millis' value.
    """
    converted_records = []

    CONDITION_KEYS_CAMEL = [
        "id",
        "index",
        "type",
        "party",
        "status",
        "autoProvisionStoreId",
        "reputationChange",
        "blockId",
        "shipmentItemId",
    ]

    for record in raw_records:
        amount_money = record.get("amount", {})
        address_lines = record.get("address", {}).get("lines", [])
        destination_lines = record.get("destination", {}).get("lines", [])

        deadline_duration_data = record.get("deadlineDuration") or {}

        address_data = _parse_address_lines(address_lines, "ADDRESS")
        destination_data = _parse_address_lines(destination_lines, "DESTINATION")

        deadline = record.get("deadline")
        if deadline is not None and deadline.get("timestamp") is not None:
            deadline = datetime.fromtimestamp(deadline["timestamp"] / 1000)
        else:
            deadline = None

        new_record = {
            "contractid": contract_id,
            # --- CHANGE HERE: Add the contractparty field ---
            "contractparty": party,
            # ------------------------------------------------
            "deadline": deadline,
            "deadlineduration_millis": deadline_duration_data.get("millis"),
            "amountmoney": amount_money.get("amount"),
            "currencymoney": amount_money.get("currency"),
            "dependencies": json.dumps(record.get("dependencies", [])),
            **address_data,
            **destination_data,
        }

        # Add simple fields (Note: this adds the condition specific 'party' as well)
        for key in CONDITION_KEYS_CAMEL:
            new_record[key.lower()] = record.get(key)

        converted_records.append(new_record)

    return converted_records


# --- CONVERTER 3: MATERIALS (From Conditions) ---


def _convert_contract_materials(raw_conditions: List[Dict[str, Any]], contract_party: str) -> List[Dict[str, Any]]:
    """
    Converts materials. Must include 'contractparty' to link to the specific condition row.
    """
    converted_records = []
    for condition in raw_conditions:
        condition_id = condition.get("id")
        quantity = condition.get("quantity")

        if not isinstance(quantity, dict):
            continue

        material_data = quantity.get("material", {})

        if material_data and condition_id:
            picked_up = condition.get("pickedUp", {})

            converted_records.append(
                {
                    "contractconditionid": condition_id,
                    # 🌟 IMPORTANT: Grandchild needs party to find the parent Condition
                    "contractparty": contract_party,
                    "materialid": material_data.get("id"),
                    "amount": quantity.get("amount"),
                    "pickedupamount": picked_up.get("amount"),
                }
            )

    return converted_records


# --- CONVERTER 4: LOAN INSTALLMENTS (From Conditions) ---


def _convert_contract_loan_installments(
    raw_conditions: List[Dict[str, Any]], contract_party: str
) -> List[Dict[str, Any]]:
    """
    Converts loan installments. Must include 'contractparty'.
    """
    converted_records = []

    for record in raw_conditions:
        if record.get("type") == "LOAN_INSTALLMENT":
            interest_data = record.get("interest", {})
            repayment_data = record.get("repayment", {})
            total_data = record.get("total", {})

            currency = total_data.get("currency") or interest_data.get("currency")

            converted_records.append(
                {
                    "conditionid": record.get("id"),
                    "contractparty": contract_party,
                    "interestamount": interest_data.get("amount"),
                    "repaymentamount": repayment_data.get("amount"),
                    "totalamount": total_data.get("amount"),
                    "currency": currency,
                }
            )

    return converted_records


# --- MAIN CASCADING CONVERTER ---


def convert_contracts_payload(
    raw_data: Dict[str, Any],
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Main function. Extracts 'party' from the root contract and waterfalls it down to all children.
    """

    raw_contracts = []
    working_data = raw_data.get("payload", raw_data)

    if "contracts" in working_data and isinstance(working_data["contracts"], list):
        raw_contracts = working_data["contracts"]
    elif working_data.get("id"):
        raw_contracts = [working_data]

    all_contract_records: List[Dict[str, Any]] = []
    all_condition_records: List[Dict[str, Any]] = []
    all_material_records: List[Dict[str, Any]] = []
    all_installment_records: List[Dict[str, Any]] = []

    for contract_record in raw_contracts:
        contract_id = contract_record.get("id")

        # 1. EXTRACT PARTY (The key to the composite ID)
        contract_party = contract_record.get("party")

        if not contract_id or not contract_party:
            continue

        # 2. CONVERT MAIN
        all_contract_records.extend(_convert_contract_main([contract_record]))

        # 3. CONVERT CHILDREN (Pass party down)
        raw_conditions = contract_record.get("conditions", [])

        # Conditions
        all_condition_records.extend(_convert_contract_conditions(raw_conditions, contract_id, contract_party))

        # Materials
        all_material_records.extend(_convert_contract_materials(raw_conditions, contract_party))

        # Installments
        all_installment_records.extend(_convert_contract_loan_installments(raw_conditions, contract_party))

    return {
        "contracts": all_contract_records,
        "conditions": all_condition_records,
        "materials": all_material_records,
        "installments": all_installment_records,
    }


def convert_site_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'sites' table schema."""
    converted_record = {}

    record = raw_records["payload"]
    platforms = []
    building_options = []
    buildingOptionsIds = []

    for build_option in record.get("buildOptions").get("options"):
        build_option_materials = []

        for material in build_option.get("materials").get("quantities"):
            build_option_materials.append(
                {
                    "buildingid": build_option.get("id"),
                    "materialid": material.get("material").get("id"),
                    "amount": material.get("amount"),
                }
            )

        build_option_workforce_capacities = []

        for workfoce_capacity in build_option.get("workforceCapacities"):
            build_option_workforce_capacities.append(
                {
                    "buildingid": build_option.get("id"),
                    "workforcelevel": workfoce_capacity.get("level"),
                    "capacity": workfoce_capacity.get("capacity"),
                }
            )

        buildingOptionsIds.append(build_option.get("id"))

        building_options.append(
            {
                "buildingid": build_option.get("id"),
                "name": build_option.get("name"),
                "ticker": build_option.get("ticker"),
                "type": build_option.get("type"),
                "area": build_option.get("area"),
                "expertisecategory": build_option.get("expertiseCategory"),
                "needsfertilesoil": build_option.get("needsFertileSoil"),
                "materials": build_option_materials,
                "workforcecapacities": build_option_workforce_capacities,
            }
        )

    for platform in record.get("platforms"):
        reclaimable_materials = []

        for material in platform.get("reclaimableMaterials"):
            reclaimable_materials.append(
                {
                    "platformid": platform.get("id").replace("\x00", ""),
                    "materialid": material.get("material").get("id"),
                    "amount": material.get("amount"),
                    "materialtype": "reclaimable",
                }
            )

        repair_materials = []

        for material in platform.get("repairMaterials"):
            repair_materials.append(
                {
                    "platformid": platform.get("id").replace("\x00", ""),
                    "materialid": material.get("material").get("id"),
                    "amount": material.get("amount"),
                    "materialtype": "repair",
                }
            )

            # Handle founded timestamp
        creation_time = platform.get("creationTime")
        if creation_time is not None and creation_time.get("timestamp") is not None:
            creation_time = datetime.fromtimestamp(creation_time["timestamp"] / 1000)
        else:
            creation_time = None

        # Handle founded timestamp
        last_repair = platform.get("lastRepair")
        if last_repair is not None and last_repair.get("timestamp") is not None:
            last_repair = datetime.fromtimestamp(last_repair["timestamp"] / 1000)
        else:
            last_repair = None

        platforms.append(
            {
                "platformid": platform.get("id").replace("\x00", ""),
                "siteid": platform.get("siteId"),
                "creationtime": creation_time,
                "bookvalueamount": platform.get("bookValue").get("amount"),
                "bookvaluecurrency": platform.get("bookValue").get("currency"),
                "area": platform.get("area"),
                "condition": platform.get("condition"),
                "buildingid": platform.get("module").get("reactorId"),
                "lastrepair": last_repair,
                "reclaimable_materials": reclaimable_materials,
                "repair_materials": repair_materials,
            }
        )

    # Handle founded timestamp
    founded_timestamp = record.get("founded")
    if founded_timestamp is not None and founded_timestamp.get("timestamp") is not None:
        founded_timestamp = datetime.fromtimestamp(founded_timestamp["timestamp"] / 1000)
    else:
        founded_timestamp = None

    converted_record = {
        "siteid": record.get("siteId"),
        "addresssystemid": record.get("address").get("lines")[0].get("entity").get("id"),
        "addressplanetid": record.get("address").get("lines")[1].get("entity").get("id"),
        "foundedtimestamp": founded_timestamp,
        "area": record.get("area"),
        "investedpermits": record.get("investedPermits"),
        "maximumpermits": record.get("maximumPermits"),
        "buildingoptions": buildingOptionsIds,
        "building_options": building_options,
        "platforms": platforms,
    }
    return converted_record


def convert_sites_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'sites' table schema."""
    converted_records = []
    records = []
    if raw_records["payload"].get("sites") is not None:
        records = raw_records["payload"]["sites"]
    elif raw_records["payload"].get("siteId") is not None:
        records = [raw_records["payload"]]

    for record in records:
        platforms = []
        building_options = []
        buildingOptionsIds = []

        for build_option in record.get("buildOptions").get("options"):
            build_option_materials = []

            for material in build_option.get("materials").get("quantities"):
                build_option_materials.append(
                    {
                        "buildingid": build_option.get("id"),
                        "materialid": material.get("material").get("id"),
                        "amount": material.get("amount"),
                    }
                )

            build_option_workforce_capacities = []

            for workfoce_capacity in build_option.get("workforceCapacities"):
                build_option_workforce_capacities.append(
                    {
                        "buildingid": build_option.get("id"),
                        "workforcelevel": workfoce_capacity.get("level"),
                        "capacity": workfoce_capacity.get("capacity"),
                    }
                )

            buildingOptionsIds.append(build_option.get("id"))

            building_options.append(
                {
                    "buildingid": build_option.get("id"),
                    "name": build_option.get("name"),
                    "ticker": build_option.get("ticker"),
                    "type": build_option.get("type"),
                    "area": build_option.get("area"),
                    "expertisecategory": build_option.get("expertiseCategory"),
                    "needsfertilesoil": build_option.get("needsFertileSoil"),
                    "materials": build_option_materials,
                    "workforcecapacities": build_option_workforce_capacities,
                }
            )

        for platform in record.get("platforms"):
            reclaimable_materials = []

            for material in platform.get("reclaimableMaterials"):
                reclaimable_materials.append(
                    {
                        "platformid": platform.get("id").replace("\x00", ""),
                        "materialid": material.get("material").get("id"),
                        "amount": material.get("amount"),
                        "materialtype": "reclaimable",
                    }
                )

            repair_materials = []

            for material in platform.get("repairMaterials"):
                repair_materials.append(
                    {
                        "platformid": platform.get("id").replace("\x00", ""),
                        "materialid": material.get("material").get("id"),
                        "amount": material.get("amount"),
                        "materialtype": "repair",
                    }
                )

                # Handle founded timestamp
            creation_time = platform.get("creationTime")
            if creation_time is not None and creation_time.get("timestamp") is not None:
                creation_time = datetime.fromtimestamp(creation_time["timestamp"] / 1000)
            else:
                creation_time = None

            # Handle founded timestamp
            last_repair = platform.get("lastRepair")
            if last_repair is not None and last_repair.get("timestamp") is not None:
                last_repair = datetime.fromtimestamp(last_repair["timestamp"] / 1000)
            else:
                last_repair = None

            platforms.append(
                {
                    "platformid": platform.get("id").replace("\x00", ""),
                    "siteid": platform.get("siteId"),
                    "creationtime": creation_time,
                    "bookvalueamount": platform.get("bookValue").get("amount"),
                    "bookvaluecurrency": platform.get("bookValue").get("currency"),
                    "area": platform.get("area"),
                    "condition": platform.get("condition"),
                    "buildingid": platform.get("module").get("reactorId"),
                    "lastrepair": last_repair,
                    "reclaimable_materials": reclaimable_materials,
                    "repair_materials": repair_materials,
                }
            )

        # Handle founded timestamp
        founded_timestamp = record.get("founded")
        if founded_timestamp is not None and founded_timestamp.get("timestamp") is not None:
            founded_timestamp = datetime.fromtimestamp(founded_timestamp["timestamp"] / 1000)
        else:
            founded_timestamp = None

        converted_records.append(
            {
                "siteid": record.get("siteId"),
                "addresssystemid": record.get("address").get("lines")[0].get("entity").get("id"),
                "addressplanetid": record.get("address").get("lines")[1].get("entity").get("id"),
                "foundedtimestamp": founded_timestamp,
                "area": record.get("area"),
                "investedpermits": record.get("investedPermits"),
                "maximumpermits": record.get("maximumPermits"),
                "buildingoptions": buildingOptionsIds,
                "building_options": building_options,
                "platforms": platforms,
            }
        )
    return converted_records


def convert_site_platforms_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'site_platforms' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "id": record.get("id"),
                "siteId": record.get("siteId"),
                "buildingPlatformId": record.get("buildingPlatformId"),
                "area": record.get("area"),
                "creationTimestamp": record.get("creationTimestamp"),
                "bookValueAmount": record.get("bookValueAmount"),
                "bookValueCurrency": record.get("bookValueCurrency"),
                "condition": record.get("condition"),
                "lastRepairTimestamp": record.get("lastRepairTimestamp"),
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


def convert_platform_materials_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'platform_materials' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "platformId": record.get("platformId"),
                "materialType": record.get("materialType"),
                "materialId": record.get("materialId"),
                "amount": record.get("amount"),
            }
        )
    return converted_records


def convert_buildings_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'buildings' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "id": record.get("id"),
                "name": record.get("name"),
                "ticker": record.get("ticker"),
                "type": record.get("type"),
                "area": record.get("area"),
                "expertiseCategory": record.get("expertiseCategory"),
                "needsFertileSoil": record.get("needsFertileSoil"),
                "workfoceCapacitiesId": record.get("workfoceCapacitiesId"),
                "buildMaterialsId": record.get("buildMaterialsId"),
            }
        )
    return converted_records


def convert_building_build_materials_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'building_build_materials' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "buildingId": record.get("buildingId"),
                "materialId": record.get("materialId"),
                "amount": record.get("amount"),
            }
        )
    return converted_records


def convert_corporation_shareholder_holdings_data(
    raw_records: Dict[str, Any],
) -> Dict[str, Any]:
    """Converts raw data to match the 'corporation_shareholder_holdings' table schema."""
    record = raw_records["payload"]
    if len(record.get("holdings", [])) == 0:
        return None

    converted_record = {"corporationid": record.get("holdings")[0].get("corporation").get("id")}
    return converted_record


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


import json
import logging
from datetime import datetime
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

def convert_planets_data(raw_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Converts raw planet data into separate lists for multiple tables.
    Accepts raw_data where 'payload' is either a single planet dict or a list of planet dicts.
    """
    # Initialize aggregated lists for DB insertion
    all_planets = []
    all_resources = []
    all_build_options = []
    all_projects = []
    all_production_fees = []
    all_orbit_data = []
    all_celestial_bodies = []
    all_physical_data = []

    # 1. Normalize Payload
    payload = raw_data.get("payload")
    
    if not payload:
        return {}

    items_to_process = []

    # Logic: If payload is a list, process it. If it's a dict, wrap it in a list.
    if isinstance(payload, list):
        items_to_process = payload
    elif isinstance(payload, dict):
        items_to_process = [payload]

    for item in items_to_process:
        # --- Safe Data Extraction ---
        planet_id = item.get("planetId")
        
        physical_data = item.get("data") or {}
        country = item.get("country") or {}
        
        # Skip invalid records without an ID
        if not planet_id:
            continue

        # --- 1. Orbit Data ---
        orbit_raw = physical_data.get("orbit")
        if isinstance(orbit_raw, str):
            try:
                orbit_raw = json.loads(orbit_raw.replace("'", '"'))
            except:
                orbit_raw = {}
        orbit_raw = orbit_raw or {}

        all_orbit_data.append({
            "planetid": planet_id,
            "orbitindex": physical_data.get("orbitIndex"),
            "semimajoraxis": orbit_raw.get("semiMajorAxis"),
            "eccentricity": orbit_raw.get("eccentricity"),
            "inclination": orbit_raw.get("inclination"),
            "rightascension": orbit_raw.get("rightAscension"),
            "periapsis": orbit_raw.get("periapsis"),
        })

        # --- 2. Naming Date ---
        naming_date = None
        n_date = item.get("namingDate")
        if isinstance(n_date, dict) and "timestamp" in n_date:
            try:
                naming_date = datetime.fromtimestamp(n_date["timestamp"] / 1000)
            except (ValueError, TypeError):
                pass

        # --- 3. System ID Extraction ---
        address_lines = item.get("address", {}).get("lines", [])
        
        # FIX: Use (l.get("entity") or {}) to handle explicit nulls safely
        system_id = next((
            (l.get("entity") or {}).get("id") 
            for l in address_lines if l.get("type") == "SYSTEM"
        ), None)

        # --- 4. Main Planet Record ---
        # FIX: Use safe chaining for 'namer' as well
        namer_username = (item.get("namer") or {}).get("username")

        all_planets.append({
            "planetid": planet_id,
            "naturalid": item.get("naturalId"),
            "name": item.get("name"),
            "namer": namer_username,
            "namingdate": naming_date,
            "nameable": item.get("nameable"),
            "systemid": system_id,
            "sunlight": physical_data.get("sunlight"),
            "surface": physical_data.get("surface"),
            "temperature": physical_data.get("temperature"),
            "plots": physical_data.get("plots"),
            "fertility": physical_data.get("fertility"),
            "populationid": item.get("populationId"),
            "admincenterid": item.get("adminCenterId"),
            "countrycode": country.get("code"),
            "countryname": country.get("name"),
            "mass": physical_data.get("mass"),
            "cogc": item.get("cogcProgramType"),
            "xata_updatedat": datetime.utcnow()
        })

        # --- 5. Resources ---
        for r in physical_data.get("resources", []):
            all_resources.append({
                "planetid": planet_id,
                "materialid": r.get("materialId"),
                "type": r.get("type"),
                "factor": r.get("factor", 0.0),
            })

        # Build physical data for planets
        all_physical_data.append({
            "planetId": planet_id,
            "fertility": physical_data.get("fertility"),
            "gravity": physical_data.get("gravity"),
            "magneticField": physical_data.get("magneticField"),
            "mass": physical_data.get("mass"),
            "massEarth": physical_data.get("massEarth"),
            "pressure": physical_data.get("pressure"),
            "radiation": physical_data.get("radiation"),
            "radius": physical_data.get("radius"),
            "surface": physical_data.get("surface"),
            "sunlight": physical_data.get("sunlight"),
            "temperature": physical_data.get("temperature")
        })


        # --- 6. Build Options ---
        for opt in item.get("buildOptions", {}).get("options", []):
            bill_of_material = json.dumps(opt.get("billOfMaterial", {}))
            all_build_options.append({
                "planetid": planet_id,
                "sitetype": opt.get("siteType"),
                "billofmaterial": bill_of_material,
            })

        # --- 7. Projects ---
        for p in item.get("projects", []):
            all_projects.append({
                "planetid": planet_id,
                "type": p.get("type"),
                "entityid": p.get("entityId"),
            })

        # --- 8. Production Fees ---
        local_rules = item.get("localRules", {})
        fees_container = local_rules.get("productionFees", {}) if local_rules else {}
        fees_list = fees_container.get("fees", []) if fees_container else []

        for f in fees_list:
            # FIX: explicit check for 'fee' object
            fee_obj = f.get("fee") or {}
            all_production_fees.append({
                "planetid": planet_id,
                "category": f.get("category"),
                "workforcelevel": f.get("workforceLevel"),
                "feeamount": fee_obj.get("amount", 0),
                "feecurrency": fee_obj.get("currency"),
            })

        # --- 9. Celestial Bodies ---
        for cbody in item.get("celestialBodies", []):
            c_address = cbody.get("address", {}).get("lines", [])

            c_system_id = None
            found_planet_id = None
            c_sat_id = None

            # 1. Single pass extraction (3x faster than multiple next() generators)
            for line in c_address:
                ent = line.get("entity") or {}
                etype = line.get("type")

                if etype == "SYSTEM":
                    c_system_id = ent.get("id")
                elif etype == "PLANET":
                    found_planet_id = ent.get("id")
                elif etype in ("SATELLITE", "MOON"):
                    c_sat_id = ent.get("id")

            # 2. Safe Fallbacks
            c_planet_id = found_planet_id or planet_id
            c_orbit = cbody.get("orbit") or {}
            natural_id = cbody.get("naturalId")

            # 3. Bulletproof ID mapping
            # We grab the ID you found, but keep the hashlib fallback JUST IN CASE
            # the game sends a broken record in the future.
            cb_id = cbody.get("id")
            if not cb_id:
                if not natural_id:
                    continue # Skip corrupted records completely
                cb_id = hashlib.md5(f"celestial_{natural_id}".encode()).hexdigest()

            # 4. Append as a Tuple for executemany
            all_celestial_bodies.append({
                "id": cb_id,
                "planetid": c_planet_id,
                "systemid": c_system_id,
                "satelliteid": c_sat_id,
                "name": cbody.get("name"),
                "naturalid": natural_id,
                "semimajoraxis": c_orbit.get("semiMajorAxis", 0),
                "eccentricity": c_orbit.get("eccentricity", 0),
                "inclination": c_orbit.get("inclination", 0),
                "rightascension": c_orbit.get("rightAscension", 0),
                "periapsis": c_orbit.get("periapsis", 0)
            })

    return {
        "planets": all_planets,
        "planet_resources": all_resources,
        "planet_build_options": all_build_options,
        "planet_projects": all_projects,
        "planet_production_fees": all_production_fees,
        "planet_orbit": all_orbit_data,
        "planet_celestial_bodies": all_celestial_bodies,
        "planet_physical_data": all_physical_data
    }

def convert_leaderboard_scores(raw_payload: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Dict[str, list]:
    """
    Parses LEADERBOARD_SCORES payloads and flattens them into a list of dictionaries.
    Handles both single payloads and lists of payloads safely.
    """
    items_to_process = raw_payload if isinstance(raw_payload, list) else [raw_payload]

    data = {
        "leaderboard_scores": []
    }

    for item in items_to_process:
        # Safely extract the core payload wrapper
        payload = item.get("payload", item.get("body", item))
        
        scores = payload.get("scores", [])
        if not scores:
            continue

        # Extract context fields that apply to all scores in this batch
        category = payload.get("type", "UNKNOWN")
        time_range = payload.get("range", "ALL_TIME")
        
        # Safe fallback for materials (e.g., categories like WEALTH have no material)
        if category == "PRODUCTION":
            material_data = payload.get("material") or {}
            material_ticker = material_data.get("ticker", "NONE")
        else:
            material_ticker = "NONE"

        # Map each individual score into a flat dictionary
        for s in scores:
            entity_id = s.get("entityId")
            if not entity_id:
                continue

            data["leaderboard_scores"].append({
                "category": category,
                "time_range": time_range,
                "material_ticker": material_ticker,
                "company_id": entity_id,
                "rank": s.get("rank"),
                "score": s.get("score")
            })

    return data

def generate_recipe_hash(reactor_id: str, duration_ms: int, inputs: List[Dict], outputs: List[Dict]) -> str:
    """
    Creates a deterministic MD5 hash to identify a unique recipe.
    Sorts inputs and outputs by Material ID to ensure order independence.
    """
    # Sort by Material ID to ensure A+B = B+A
    sorted_inputs = sorted(inputs, key=lambda x: x['material']['id'])
    sorted_outputs = sorted(outputs, key=lambda x: x['material']['id'])

    # Format: "ID-Amount,ID-Amount"
    input_str = ",".join([f"{i['material']['id']}-{i['amount']}" for i in sorted_inputs])
    output_str = ",".join([f"{o['material']['id']}-{o['amount']}" for o in sorted_outputs])
    
    # Structure: Reactor|Duration|IN:...|OUT:...
    unique_string = f"{reactor_id}|{duration_ms}|IN:{input_str}|OUT:{output_str}"
    
    return hashlib.md5(unique_string.encode('utf-8')).hexdigest()

def normalize_recipe_object(raw_recipe: Dict[str, Any], reactor_id: str) -> Dict[str, Any]:
    """Helper to convert a raw recipe node into our flat internal structure."""
    inputs = raw_recipe.get('inputs', [])
    outputs = raw_recipe.get('outputs', [])
    duration = raw_recipe.get('duration', {}).get('millis', 0)

    # Generate the unique ID based on content
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

def determine_building_type(ticker: str, name: str, expertise: str) -> str:
    """
    Determines building type based on specific user rules.
    Priority: Habitation -> Storage -> Core -> Manufacturing (if expertise exists).
    """
    ticker_u = ticker.upper()
    name_l = name.lower()
    
    # 1. HABITATION Rule
    if ticker_u.startswith("HB") or "habitation" in name_l:
        return "HABITATION"
        
    # 2. STORAGE Rule
    if "storage" in name_l:
        return "STORAGE"
        
    # 3. CORE Rule
    if ticker_u == "CM":
        return "CORE"
        
    # 4. MANUFACTURING Rule (Anything with an expertise category)
    if expertise:
        return "MANUFACTURING"
        
    # Default Fallback (e.g. Corporate HQ, Local Market)
    return "INFRASTRUCTURE"

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

def convert_planet_physical_data_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'planet_physical_data' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "planetId": record.get("planetId"),
                "gravity": record.get("gravity"),
                "magneticField": record.get("magneticField"),
                "mass": record.get("mass"),
                "massEarth": record.get("massEarth"),
                "pressure": record.get("pressure"),
                "radiation": record.get("radiation"),
                "radius": record.get("radius"),
            }
        )
    return converted_records


def convert_planet_orbit_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'planet_orbit' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "planetId": record.get("planetId"),
                "orbitIndex": record.get("orbitIndex"),
                "semiMajorAxis": record.get("semiMajorAxis"),
                "eccentricity": record.get("eccentricity"),
                "inclination": record.get("inclination"),
                "rightAscension": record.get("rightAscension"),
                "periapsis": record.get("periapsis"),
            }
        )
    return converted_records


def convert_planet_resources_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'planet_resources' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "planetId": record.get("planetId"),
                "materialId": record.get("materialId"),
                "type": record.get("type"),
                "factor": record.get("factor"),
            }
        )
    return converted_records


def convert_planetWorkforceFees_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'planetWorkforceFees' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "planetId": record.get("planetId"),
                "category": record.get("category"),
                "workforceLevel": record.get("workforceLevel"),
                "feeAmount": record.get("feeAmount"),
                "feeCurrency": record.get("feeCurrency"),
            }
        )
    return converted_records


def convert_planetMarketFees_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'planetMarketFees' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "planetId": record.get("planetId"),
                "productionFeeLimitFactors": record.get("productionFeeLimitFactors"),
                "localMarketFeeBase": record.get("localMarketFeeBase"),
                "localMarketFeeTimeFactor": record.get("localMarketFeeTimeFactor"),
                "warehouseFee": record.get("warehouseFee"),
                "siteEstablishmentFee": record.get("siteEstablishmentFee"),
            }
        )
    return converted_records


def convert_planetBuildOptions_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'planetBuildOptions' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "planetId": record.get("planetId"),
                "siteType": record.get("siteType"),
                "costsAmount": record.get("costsAmount"),
                "costsCurrency": record.get("costsCurrency"),
                "feeReceiver": record.get("feeReceiver"),
            }
        )
    return converted_records


def convert_planetBuildOptionMaterials_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'planetBuildOptionMaterials' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "planetId": record.get("planetId"),
                "siteType": record.get("siteType"),
                "materialId": record.get("materialId"),
                "amount": record.get("amount"),
            }
        )
    return converted_records


def convert_planet_infrastructure_project(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    converted_record = {}
    if raw_record.get("payload") is not None:
        upkeeps = raw_record["payload"].get("upkeeps", [])
        upgrade_costs = raw_record["payload"].get("upgradeCosts", [])
        contributions = raw_record["payload"].get("contributions", [])

        converted_record["upgrade_costs"] = []
        converted_record["upkeep"] = []
        converted_record["contributions"] = []
        if upkeeps is not None:
            for upkeep in upkeeps:
                converted_record["upkeep"].append(
                    {
                        "amount": upkeep.get("amount"),
                        "currentamount": upkeep.get("currentAmount"),
                        "duration": upkeep.get("duration"),
                        "materialid": upkeep.get("material").get("id"),
                        "nexttick": upkeep.get("nextTick").get("timestamp"),
                        "storecapacity": upkeep.get("storeCapacity"),
                        "stored": upkeep.get("stored"),
                    }
                )

        if upgrade_costs is not None:
            for upgrade_cost in upgrade_costs:
                converted_record["upgrade_costs"].append(
                    {
                        "materialid": upgrade_cost.get("material").get("id"),
                        "amount": upgrade_cost.get("amount"),
                        "currentamount": upgrade_cost.get("currentAmount"),
                    }
                )

        if contributions is not None:
            for contribution in contributions:
                contributor = contribution.get("contributor")
                materials = contribution.get("materials", [])

                for material in materials:
                    converted_record["contributions"].append(
                        {
                            "contributorid": contributor.get("id"),
                            "contributorname": contributor.get("name"),
                            "contributorcode": contributor.get("code"),
                            "amount": material.get("amount"),
                            "materialid": material.get("material").get("id"),
                            "timestamp": contribution.get("time").get("timestamp"),
                        }
                    )
        converted_record.update(
            {
                "populationid": raw_record["payload"].get("populationid"),
                "projectid": raw_record["payload"].get("id"),
            }
        )
    return converted_record


def convert_planet_population_data(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    converted_record = {}
    converted_record["infrastructures"] = []
    converted_record["populations"] = []
    infrastructure_data = raw_record["payload"].get("infrastructure")
    population_data_reports = raw_record["payload"].get("reports")
    populationid = raw_record["payload"].get("id")

    for infrastructure in infrastructure_data:
        converted_record["infrastructures"].append(
            {
                "populationid": populationid,
                "type": infrastructure.get("type"),
                "ticker": infrastructure.get("ticker"),
                "projectid": infrastructure.get("projectId"),
                "projectname": infrastructure.get("projectName"),
                "level": infrastructure.get("level"),
                "activelevel": infrastructure.get("activeLevel"),
                "currentlevel": infrastructure.get("currentLevel"),
                "upkeepstatus": infrastructure.get("upkeepStatus"),
                "upgradestatus": infrastructure.get("upgradeStatus"),
            }
        )

    for report in population_data_reports:
        time = report.get("time")
        if time is not None and time.get("timestamp") is not None:
            time = datetime.fromtimestamp(time["timestamp"] / 1000)
        else:
            time = None  # Or None, 0, etc. based on your needs
        converted_record["populations"].append(
            {
                "populationid": populationid,
                "time": time,
                "simulationperiod": report.get("simulationPeriod"),
                "explorersgraceenabled": report.get("explorersGraceEnabled"),
                "nextpopulationpioneer": report.get("nextPopulation").get("PIONEER"),
                "nextpopulationsettler": report.get("nextPopulation").get("SETTLER"),
                "nextpopulationtechnician": report.get("nextPopulation").get("TECHNICIAN"),
                "nextpopulationengineer": report.get("nextPopulation").get("ENGINEER"),
                "nextpopulationscientist": report.get("nextPopulation").get("SCIENTIST"),
                "populationdifferencepioneer": report.get("populationDifference").get("PIONEER"),
                "populationdifferencesettler": report.get("populationDifference").get("SETTLER"),
                "populationdifferencetechnician": report.get("populationDifference").get("TECHNICIAN"),
                "populationdifferenceengineer": report.get("populationDifference").get("ENGINEER"),
                "populationdifferencescientist": report.get("populationDifference").get("SCIENTIST"),
                "openjobspioneer": report.get("openJobs").get("PIONEER"),
                "openjobssettler": report.get("openJobs").get("SETTLER"),
                "openjobstechnician": report.get("openJobs").get("TECHNICIAN"),
                "openjobsengineer": report.get("openJobs").get("ENGINEER"),
                "openjobsscientist": report.get("openJobs").get("SCIENTIST"),
                "unemploymentratepioneer": report.get("unemploymentRate").get("PIONEER"),
                "unemploymentratesettler": report.get("unemploymentRate").get("SETTLER"),
                "unemploymentratetechnician": report.get("unemploymentRate").get("TECHNICIAN"),
                "unemploymentrateengineer": report.get("unemploymentRate").get("ENGINEER"),
                "unemploymentratescientist": report.get("unemploymentRate").get("SCIENTIST"),
                "averagehappinesspioneer": report.get("averageHappiness").get("PIONEER"),
                "averagehappinesssettler": report.get("averageHappiness").get("SETTLER"),
                "averagehappinesstechnician": report.get("averageHappiness").get("TECHNICIAN"),
                "averagehappinessengineer": report.get("averageHappiness").get("ENGINEER"),
                "averagehappinessscientist": report.get("averageHappiness").get("SCIENTIST"),
                "governmentprogramtype": report.get("governmentProgramType"),
            }
        )

    return converted_record


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


def convert_countries_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'countries' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "id": record.get("id"),
                "code": record.get("code"),
                "name": record.get("name"),
                "currencyName": record.get("currencyName"),
                "currencyCode": record.get("currencyCode"),
                "currencyNumericCode": record.get("currencyNumericCode"),
                "currencyDecimals": record.get("currencyDecimals"),
            }
        )
    return converted_records


def convert_commodity_exchanges_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'commodity_exchanges' table schema."""
    converted_records = []
    for record in raw_records['payload']:
        # 1. Safely extract nested objects
        operator = record.get("operator", {})
        currency = record.get("currency", {})
        address_lines = record.get("address", {}).get("lines", [])

        # 2. Iterate through address lines to find System and Station IDs
        system_id = None
        station_id = None
        
        for line in address_lines:
            line_type = line.get("type")
            entity = line.get("entity", {})
            
            if line_type == "SYSTEM":
                system_id = entity.get("id")
            elif line_type == "STATION":
                station_id = entity.get("id")

        # 3. Build the flat record
        converted_records.append(
            {
                "id": record.get("id"),
                "name": record.get("name"),
                "code": record.get("code"),  # e.g. "AI1", "IC1"
                "operatorid": operator.get("id"),
                "currencyname": currency.get("name"),
                "currencycode": currency.get("code"), # e.g. "AIC", "ICA"
                "currencynumericcode": currency.get("numericCode"),
                "currencydecimals": currency.get("decimals"),
                "systemid": system_id,
                "stationid": station_id,
            }
        )
        
    return converted_records


def convert_population_available_reserve_workforce_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'population_available_reserve_workforce' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "siteId": record.get("siteId"),
                "workforceAmountPioneer": record.get("workforceAmountPioneer"),
                "workforceAmountSettler": record.get("workforceAmountSettler"),
                "workforceAmountTechnician": record.get("workforceAmountTechnician"),
                "workforceAmountEngineer": record.get("workforceAmountEngineer"),
                "workforceAmountScientist": record.get("workforceAmountScientist"),
            }
        )
    return converted_records


def convert_comex_broker_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'comex_trade_orders' table schema."""
    converted_records = []
    record = raw_records["payload"]
    buyOrders = []
    sellOrders = []
    for buy in record.get("buyingOrders"):
        buyOrders.append(
            {
                "orderid": buy.get("id"),
                "amount": buy.get("amount"),
                "priceamount": buy.get("limit").get("amount"),
                "pricecurrency": buy.get("limit").get("currency"),
                "traderid": buy.get("trader").get("id"),
                "tradername": buy.get("trader").get("name"),
                "tradercode": buy.get("trader").get("code"),
            }
        )

    for sell in record.get("sellingOrders"):
        sellOrders.append(
            {
                "orderid": sell.get("id"),
                "amount": sell.get("amount"),
                "priceamount": sell.get("limit").get("amount"),
                "pricecurrency": sell.get("limit").get("currency"),
                "traderid": sell.get("trader").get("id"),
                "tradername": sell.get("trader").get("name"),
                "tradercode": sell.get("trader").get("code"),
            }
        )
    # Handle earliest contract timestamp
    price_time = record.get("priceTime")
    if price_time is not None and price_time.get("timestamp") is not None:
        price_time = datetime.fromtimestamp(price_time["timestamp"] / 1000)
    else:
        price_time = None  # Or None, 0, etc. based on your needs

    converted_records.append(
        {
            "brokermaterialid": record.get("id"),
            "addresssystemid": record.get("address", {}).get("lines", [{}, {}])[0].get("entity", {}).get("id"),
            "addressstationid": record.get("address", {}).get("lines", [{}, {}])[1].get("entity", {}).get("id"),
            "exchangeid": record.get("exchange" or {}).get("id"),
            "currencyid": record.get("currency" or {}).get("code"),
            "demand": record.get("demand"),
            "supply": record.get("supply"),
            "traded": record.get("traded"),
            "ticker": record.get("ticker"),
            "askamount": (record.get("ask") or {}).get("amount"),
            "askprice": (record.get("ask") or {}).get("price", {}).get("amount"),
            "bidamount": (record.get("bid") or {}).get("amount"),
            "bidprice": (record.get("bid") or {}).get("price", {}).get("amount"),
            "high": (record.get("high") or {}).get("amount"),
            "low": (record.get("low") or {}).get("amount"),
            "materialid": record.get("material", {}).get("id"),
            "narrowpricebandhigh": (record.get("narrowPriceBand") or {}).get("high"),
            "narrowpricebandlow": (record.get("narrowPriceBand") or {}).get("low"),
            "price": (record.get("price") or {}).get("amount"),
            "priceaverage": (record.get("price") or {}).get("amount"),
            "pricetime": price_time,
            "volume": (record.get("volume") or {}).get("amount"),
            "widepricebandhigh": (record.get("widePriceBand") or {}).get("high"),
            "widepricebandlow": (record.get("widePriceBand") or {}).get("low"),
            "alltimehigh": (record.get("allTimeHigh") or {}).get("amount"),
            "alltimelow": (record.get("allTimeLow") or {}).get("amount"),
            "buy": buyOrders,
            "sell": sellOrders,
        }
    )
    return converted_records


def convert_comex_trade_order_update_data(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    converted_record = handle_comex_trade_order_data(raw_record)
    return converted_record


def convert_comex_trade_order_added_data(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    converted_record = handle_comex_trade_order_data(raw_record)
    return converted_record


def convert_comex_trade_order_remove(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    """Converts raw data to match the 'comex_trade_order_remove' table schema."""
    record = raw_record["payload"]
    converted_record = {"orderid": record.get("orderId")}
    return converted_record


def handle_comex_trade_order_data(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    converted_record = {}
    trades = []

    record = raw_record["payload"]

    for trade in record.get("trades"):
        # Handle earliest contract timestamp
        trade_time = trade.get("time")
        if trade_time is not None and trade_time.get("timestamp") is not None:
            trade_time = datetime.fromtimestamp(trade_time["timestamp"] / 1000)
        else:
            trade_time = None  # Or None, 0, etc. based on your needs

        trades.append(
            {
                "tradeid": trade.get("id"),
                "amount": trade.get("amount"),
                "priceamount": trade.get("price").get("amount"),
                "pricecurrency": trade.get("price").get("currency"),
                "tradetime": trade_time,
                "partnerid": trade.get("partner").get("id"),
                "partnername": trade.get("partner").get("name"),
                "partnercode": trade.get("partner").get("code"),
            }
        )

    # Handle earliest contract timestamp
    created = record.get("created")
    if created is not None and created.get("timestamp") is not None:
        created = datetime.fromtimestamp(created["timestamp"] / 1000)
    else:
        created = None  # Or None, 0, etc. based on your needs

    converted_record = {
        "orderid": record.get("id"),
        "exchangeid": record.get("exchange").get("id"),
        "brokerid": record.get("brokerId"),
        "type": record.get("type"),
        "materialid": record.get("material").get("id"),
        "amount": record.get("amount"),
        "initialamount": record.get("initialAmount"),
        "limitamount": record.get("limit").get("amount"),
        "limitcurrency": record.get("limit").get("currency"),
        "status": record.get("status"),
        "created": created,
        "trades": trades,
    }
    return converted_record


def convert_comex_trade_orders_data(
    raw_records: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'comex_trade_orders' table schema."""
    converted_records = []
    for record in raw_records["payload"]["orders"]:
        trades = []

        for trade in record.get("trades"):
            # Handle earliest contract timestamp
            trade_time = trade.get("time")
            if trade_time is not None and trade_time.get("timestamp") is not None:
                trade_time = datetime.fromtimestamp(trade_time["timestamp"] / 1000)
            else:
                trade_time = None  # Or None, 0, etc. based on your needs

            trades.append(
                {
                    "tradeid": trade.get("id"),
                    "amount": trade.get("amount"),
                    "priceamount": trade.get("price").get("amount"),
                    "pricecurrency": trade.get("price").get("currency"),
                    "tradetime": trade_time,
                    "partnerid": trade.get("partner").get("id"),
                    "partnername": trade.get("partner").get("name"),
                    "partnercode": trade.get("partner").get("code"),
                }
            )

        # Handle earliest contract timestamp
        created = record.get("created")
        if created is not None and created.get("timestamp") is not None:
            created = datetime.fromtimestamp(created["timestamp"] / 1000)
        else:
            created = None  # Or None, 0, etc. based on your needs

        converted_records.append(
            {
                "orderid": record.get("id"),
                "exchangeid": record.get("exchange").get("id"),
                "brokerid": record.get("brokerId"),
                "type": record.get("type"),
                "materialid": record.get("material").get("id"),
                "amount": record.get("amount"),
                "initialamount": record.get("initialAmount"),
                "limitamount": record.get("limit").get("amount"),
                "limitcurrency": record.get("limit").get("currency"),
                "status": record.get("status"),
                "created": created,
                "trades": trades,
            }
        )
    return converted_records


def convert_comex_trade_orders_trades_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'comex_trade_orders_trades' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "id": record.get("id"),
                "orderId": record.get("orderId"),
                "amount": record.get("amount"),
                "priceAmount": record.get("priceAmount"),
                "priceCurrency": record.get("priceCurrency"),
                "timeTimestamp": record.get("timeTimestamp"),
                "partnerId": record.get("partnerId"),
            }
        )
    return converted_records


def convert_shipyard_projects_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'shipyard_projects' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "id": record.get("id"),
                "creationTimestamp": record.get("creationTimestamp"),
                "startTimestamp": record.get("startTimestamp"),
                "endTimestamp": record.get("endTimestamp"),
                "blueprintNaturalId": record.get("blueprintNaturalId"),
                "originBlueprintNaturalId": record.get("originBlueprintNaturalId"),
                "shipyardId": record.get("shipyardId"),
                "status": record.get("status"),
                "canStart": record.get("canStart"),
                "shipId": record.get("shipId"),
            }
        )
    return converted_records


def convert_shipyard_project_materials_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'shipyard_project_materials' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "projectId": record.get("projectId"),
                "materialId": record.get("materialId"),
                "amount": record.get("amount"),
                "limit": record.get("limit"),
            }
        )
    return converted_records


def convert_shipyards_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'shipyards' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "id": record.get("id"),
                "systemId": record.get("systemId"),
                "planetId": record.get("planetId"),
                "currencyId": record.get("currencyId"),
                "operatorType": record.get("operatorType"),
                "createdProjectsTotal": record.get("createdProjectsTotal"),
                "activeProjectsTotal": record.get("activeProjectsTotal"),
                "finishedProjectsTotal": record.get("finishedProjectsTotal"),
                "finishedProjectsWeek": record.get("finishedProjectsWeek"),
                "finishedProjectsMonth": record.get("finishedProjectsMonth"),
                "finishedProjectsSemiannually": record.get("finishedProjectsSemiannually"),
            }
        )
    return converted_records


def convert_ship_blueprints_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'ship_blueprints' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "id": record.get("id"),
                "naturalId": record.get("naturalId"),
                "createdTimestamp": record.get("createdTimestamp"),
                "name": record.get("name"),
                "buildTime": record.get("buildTime"),
                "status": record.get("status"),
            }
        )
    return converted_records


def convert_ship_blueprint_bill_of_materials_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'ship_blueprint_bill_of_materials' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "blueprintId": record.get("blueprintId"),
                "materialId": record.get("materialId"),
                "amount": record.get("amount"),
            }
        )
    return converted_records


def convert_ship_blueprint_components_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'ship_blueprint_components' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "id": record.get("id"),
                "blueprintId": record.get("blueprintId"),
                "type": record.get("type"),
                "cardinality": record.get("cardinality"),
                "option": record.get("option"),
                "optionMaterialId": record.get("optionMaterialId"),
                "amount": record.get("amount"),
            }
        )
    return converted_records


def convert_blueprint_components_modifiers_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'blueprint_components_modifiers' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "componentId": record.get("componentId"),
                "type": record.get("type"),
                "value": record.get("value"),
            }
        )
    return converted_records


def convert_blueprint_performance_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'blueprint_performance' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "blueprintId": record.get("blueprintId"),
                "acceleration": record.get("acceleration"),
                "accelerationMax": record.get("accelerationMax"),
                "emitterChargeTime": record.get("emitterChargeTime"),
                "fltFuelCapacity": record.get("fltFuelCapacity"),
                "fltMaxSpeed": record.get("fltMaxSpeed"),
                "maxGFactor": record.get("maxGFactor"),
                "maxOverchargeTime": record.get("maxOverchargeTime"),
                "minReactorUsage": record.get("minReactorUsage"),
                "operatingEmptyMass": record.get("operatingEmptyMass"),
                "stlFuelCapacity": record.get("stlFuelCapacity"),
                "storeCapacityVolume": record.get("storeCapacityVolume"),
                "storeCapacityMass": record.get("storeCapacityMass"),
                "totalVolume": record.get("totalVolume"),
            }
        )
    return converted_records


def convert_ship_blueprints_component_options_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'ship_blueprints_component_options' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "type": record.get("type"),
                "option": record.get("option"),
                "materialName": record.get("materialName"),
            }
        )
    return converted_records


def convert_ship_blueprints_component_types_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'ship_blueprints_component_types' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "type": record.get("type"),
                "cardinality": record.get("cardinality"),
                "selectable": record.get("selectable"),
            }
        )
    return converted_records


def convert_site_experts_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'site_experts' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "siteId": record.get("siteId"),
                "category": record.get("category"),
                "current": record.get("current"),
                "limit": record.get("limit"),
                "available": record.get("available"),
                "efficiencyGain": record.get("efficiencyGain"),
                "progress": record.get("progress"),
            }
        )
    return converted_records


def convert_cocg_programs_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'cocg_programs' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append({"id": record.get("id"), "category": record.get("category")})
    return converted_records


def convert_corporations_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'corporations' table schema."""
    converted_record = {}
    record = raw_records["payload"]

    # Handle earliest contract timestamp
    foundedtimestamp = record.get("founded")
    if foundedtimestamp is not None and foundedtimestamp.get("timestamp") is not None:
        foundedtimestamp = datetime.fromtimestamp(foundedtimestamp["timestamp"] / 1000)
    else:
        foundedtimestamp = None  # Or None, 0, etc. based on your needs

    converted_record = {
        "id": record.get("id"),
        "name": record.get("name"),
        "code": record.get("code"),
        "countryid": record.get("country").get("id"),
        "currencycode": record.get("currency").get("code"),
        "foundedtimestamp": foundedtimestamp,
        "totalshares": record.get("totalShares"),
        "shareholders": convert_shareholders(record.get("shareholders")),
    }

    return converted_record


def convert_shareholders(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    converted_records = []
    for shareholder in raw_records:
        converted_records.append(
            {
                "companyid": shareholder.get("company").get("id"),
                "companycode": shareholder.get("company").get("code"),
                "companyname": shareholder.get("company").get("name"),
                "relativeshare": shareholder.get("relativeShare"),
                "shares": shareholder.get("shares"),
            }
        )
    return converted_records


def convert_corporation_shareholders_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'corporation_shareholders' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "corporationId": record.get("corporationId"),
                "userId": record.get("userId"),
                "relativeShare": record.get("relativeShare"),
                "shares": record.get("shares"),
            }
        )
    return converted_records


def convert_corporation_projects_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'corporation_projects' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "id": record.get("id"),
                "naturalId": record.get("naturalId"),
                "type": record.get("type"),
                "corporationId": record.get("corporationId"),
                "systemId": record.get("systemId"),
                "planetId": record.get("planetId"),
                "completionDate": record.get("completionDate"),
            }
        )
    return converted_records


def convert_corporation_project_bill_of_materials_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'corporation_project_bill_of_materials' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "projectId": record.get("projectId"),
                "materialId": record.get("materialId"),
                "amount": record.get("amount"),
                "currentAmount": record.get("currentAmount"),
            }
        )
    return converted_records


def convert_corporation_project_bill_contributions_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'corporation_project_bill_contributions' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "projectId": record.get("projectId"),
                "userId": record.get("userId"),
                "materialId": record.get("materialId"),
                "amount": record.get("amount"),
                "timestamp": record.get("timestamp"),
            }
        )
    return converted_records


def convert_currencies_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'currencies' table schema."""
    converted_records = []
    for record in raw_records:
        converted_records.append(
            {
                "numericCode": record.get("numericCode"),
                "code": record.get("code"),
                "name": record.get("name"),
                "decimals": record.get("decimals"),
            }
        )
    return converted_records


def convert_user_currency_accounts_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'user_currency_accounts' table schema."""
    converted_records = []
    for record in raw_records["payload"].get("currencyAccounts"):
        converted_records.append(
            {
                "category": record.get("category"),
                "type": record.get("type"),
                "number": record.get("number"),
                "bookbalanceamount": record.get("bookBalance").get("amount"),
                "bookbalancecurrencycode": record.get("bookBalance").get("currency"),
                "balanceamount": record.get("currencyBalance").get("amount"),
                "balancecurrencycode": record.get("currencyBalance").get("currency"),
            }
        )
    return converted_records


def convert_accounting_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'user_currency_accounts' table schema."""
    converted_records = []
    for record in raw_records["payload"].get("items"):
        if record.get("accountCategory") == "LIQUID_ASSETS":
            converted_records.append(
                {
                    "category": record.get("accountCategory"),
                    "type": record.get("accountType"),
                    "number": record.get("account"),
                    "bookbalanceamount": record.get("bookBalance").get("amount"),
                    "balanceamount": record.get("balance").get("amount"),
                }
            )
    return converted_records


def convert_recipe_io(raw_data: List[Dict[str, Any]], process_id: str, io_type: str) -> List[Dict[str, Any]]:
    """
    Converts the inner 'inputs'/'outputs' arrays into flat process_material_io records,
    using SQL column names (processid, materialid, iotype).
    """
    converted_records = []
    for record in raw_data:
        converted_records.append(
            {
                # SQL Column Names:
                "processid": process_id,
                "materialid": record.get("material").get("id"),  # Corresponds to SQL 'materialid'
                "iotype": io_type,  # Corresponds to SQL 'iotype'
                "amount": record.get("amount"),
            }
        )
    return converted_records


def convert_io_recipes(
    raw_data: List[Dict[str, Any]], process_type: str
) -> Tuple[List[str], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Converts a list of raw recipes into flat records for material_processes and
    process_material_io tables, using SQL column names.
    Returns: (list of process_ids, material_processes records, process_material_io records)
    """
    process_ids = []
    material_processes_data = []
    process_material_io_data = []

    for record in raw_data:
        # Generate a unique ID for this specific process variant
        process_id = str(uuid.uuid4())
        process_ids.append(process_id)

        duration_data = record.get("duration")
        duration = duration_data.get("millis") if duration_data else None

        # 1. Prepare material_processes record
        material_processes_data.append(
            {
                # SQL Column Names:
                "processid": process_id,
                "reactorid": record.get("reactorId"),  # Corresponds to SQL 'reactorid'
                "durationmillis": duration,  # Corresponds to SQL 'durationmillis'
                "processtype": process_type,  # Corresponds to SQL 'processtype'
            }
        )

        # 2. Prepare process_material_io records (Inputs and Outputs)
        inputs = convert_recipe_io(record.get("inputs", []), process_id, "INPUT")
        process_material_io_data.extend(inputs)

        outputs = convert_recipe_io(record.get("outputs", []), process_id, "OUTPUT")
        process_material_io_data.extend(outputs)

    return process_ids, material_processes_data, process_material_io_data


# --- Main Orchestration Function (Reworked to match SQL schema column names) ---


def convert_material_data_recipes(raw_records: Dict[str, Any], *args, **kwargs) -> Dict[str, Any]:
    """
    Transforms the raw material recipe JSON into flat records for database insertion,
    ensuring all keys match the final SQL schema and applying the 'Wrought Product' filter.
    """
    record = raw_records.get("payload", {})
    material_id = record.get("material", {}).get("id")

    if not material_id:
        raise ValueError("Cannot process recipe: missing material ID.")

    # -----------------------------------------------------------
    # 1. APPLY FILTER TO RAW INPUT RECIPES (Wrought Product Filter)
    # -----------------------------------------------------------

    raw_input_recipes = record.get("inputRecipes", [])
    filtered_raw_input_recipes = []

    for recipe in raw_input_recipes:
        is_wrought_product_recipe = False

        for input_item in recipe.get("inputs", []):
            input_material_id = input_item.get("material", {}).get("id")

            if input_material_id == material_id:
                is_wrought_product_recipe = True
                break

        if not is_wrought_product_recipe:
            filtered_raw_input_recipes.append(recipe)

    # -----------------------------------------------------------
    # 2. Convert Data to Flat Records
    # -----------------------------------------------------------

    # Process production recipes
    output_recipe_ids, output_processes, output_io = convert_io_recipes(record.get("outputRecipes", []), "OUTPUT")

    # Process the filtered consumption/cost recipes
    input_recipe_ids, input_processes, input_io = convert_io_recipes(filtered_raw_input_recipes, "INPUT")

    # Aggregate flat lists
    material_processes_data = output_processes + input_processes
    process_material_io_data = output_io + input_io

    # Prepare the single 'recipes' main table record
    recipes_data = {
        # SQL Column Names:
        "materialid": material_id,  # Corresponds to SQL 'materialid'
        "input_recipe_ids": json.dumps(input_recipe_ids),
        "output_recipe_ids": json.dumps(output_recipe_ids),
    }

    # Return the structure required by the database handler
    return {
        "recipes": [recipes_data],
        "material_processes": material_processes_data,
        "process_material_io": process_material_io_data,
    }


# ==============================================================================
# A central mapping for easy access to conversion functions.
# This makes it easy for a main script to find the right function.
# ==============================================================================

CONVERSION_FUNCTIONS = {
    "users_data": convert_users_data_table,
    "user_gifts_received": convert_user_gifts_received_data,
    "user_gifts_sent": convert_user_gifts_sent_data,
    "user_starting_profiles": convert_user_starting_profiles_data,
    "user_tokens": convert_user_tokens_data,
    "user_data_tokens": convert_user_data_tokens_data,
    "company_data": convert_company_data,
    "world_material_categories": convert_world_materials_data,
    "headquarters_upgrade_items": convert_headquarters_upgrade_items_data,
    "storages": convert_storages_data,
    "warehouses": convert_warehouses_data,
    "storage_items": convert_storage_items_data,
    "production_lines": convert_production_lines_data,
    "production_workforces": convert_production_workforces_data,
    "production_line_orders": convert_production_line_orders_data,
    "production_line_order_materials": convert_production_line_order_materials_data,
    "flights": convert_flight_records,
    "ships": convert_ships_data,
    "ship_repair_materials": convert_ship_repair_materials_data,
    "workforces": convert_workforces_data,
    "workforceNeeds": convert_workforce_needs_data,
    "contracts": convert_contracts_payload,
    "sites": convert_sites_data,
    "site_platforms": convert_site_platforms_data,
    "platform_materials": convert_platform_materials_data,
    "buildings": convert_buildings_data,
    "building_build_materials": convert_building_build_materials_data,
    "corporation_shareholder_holdings": convert_corporation_shareholder_holdings_data,
    "sectors": convert_sectors_data,
    "systems": convert_systems_data,
    "system_connections": convert_system_connections_data,
    "planets": convert_planets_data,
    "planet_physical_data": convert_planet_physical_data_data,
    "planet_orbit": convert_planet_orbit_data,
    "planet_resources": convert_planet_resources_data,
    "planetWorkforceFees": convert_planetWorkforceFees_data,
    "planetMarketFees": convert_planetMarketFees_data,
    "planetBuildOptions": convert_planetBuildOptions_data,
    "planetBuildOptionMaterials": convert_planetBuildOptionMaterials_data,
    "stations": convert_stations_data,
    "countries": convert_countries_data,
    "commodity_exchanges": convert_commodity_exchanges_data,
    "population_available_reserve_workforce": convert_population_available_reserve_workforce_data,
    "comex_trade_orders": convert_comex_trade_orders_data,
    "comex_trade_orders_trades": convert_comex_trade_orders_trades_data,
    "shipyard_projects": convert_shipyard_projects_data,
    "shipyard_project_materials": convert_shipyard_project_materials_data,
    "shipyards": convert_shipyards_data,
    "ship_blueprints": convert_ship_blueprints_data,
    "ship_blueprint_bill_of_materials": convert_ship_blueprint_bill_of_materials_data,
    "ship_blueprint_components": convert_ship_blueprint_components_data,
    "blueprint_components_modifiers": convert_blueprint_components_modifiers_data,
    "blueprint_performance": convert_blueprint_performance_data,
    "ship_blueprints_component_options": convert_ship_blueprints_component_options_data,
    "ship_blueprints_component_types": convert_ship_blueprints_component_types_data,
    "site_experts": convert_site_experts_data,
    "cocg_programs": convert_cocg_programs_data,
    "corporations": convert_corporations_data,
    "corporation_shareholders": convert_corporation_shareholders_data,
    "corporation_projects": convert_corporation_projects_data,
    "corporation_project_bill_of_materials": convert_corporation_project_bill_of_materials_data,
    "corporation_project_bill_contributions": convert_corporation_project_bill_contributions_data,
    "currencies": convert_currencies_data,
    "user_currency_accounts": convert_user_currency_accounts_data,
}
