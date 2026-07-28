# converters/company.py
import datetime
import hashlib
import json
import string
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union


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
