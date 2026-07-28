# converters/contracts.py
import datetime
import hashlib
import json
import string
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union
import data_converter


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
