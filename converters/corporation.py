# converters/corporation.py
import datetime
from datetime import datetime
from typing import Any, Dict, List

def convert_shareholders(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    converted_records = []
    if raw_records:
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

def convert_corporations_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'corporations' table schema."""
    converted_record = {}
    record = raw_records["payload"]

    foundedtimestamp = record.get("founded")
    if foundedtimestamp is not None and foundedtimestamp.get("timestamp") is not None:
        foundedtimestamp = datetime.fromtimestamp(foundedtimestamp["timestamp"] / 1000)
    else:
        foundedtimestamp = None

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

def convert_corporation_shareholder_holdings_data(
    raw_records: Dict[str, Any],
) -> Dict[str, Any]:
    """Converts raw data to match the 'corporation_shareholder_holdings' table schema."""
    record = raw_records["payload"]
    if len(record.get("holdings", [])) == 0:
        return None

    converted_record = {"corporationid": record.get("holdings")[0].get("corporation").get("id")}
    return converted_record

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
