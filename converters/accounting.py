# converters/accounting.py
import datetime
import hashlib
import json
import string
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union


def convert_user_currency_accounts_data(
    raw_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'user_currency_accounts' table schema."""
    converted_records = []
    payload = raw_records.get("payload", {})
    currency_accounts = payload.get("currencyAccounts", []) if isinstance(payload, dict) else []
    for record in currency_accounts:
        converted_records.append(
            {
                "category": record.get("category"),
                "type": record.get("type"),
                "number": record.get("number"),
                "bookbalanceamount": record.get("bookBalance", {}).get("amount") if record.get("bookBalance") else None,
                "bookbalancecurrencycode": record.get("bookBalance", {}).get("currency") if record.get("bookBalance") else None,
                "balanceamount": record.get("currencyBalance", {}).get("amount") if record.get("currencyBalance") else None,
                "balancecurrencycode": record.get("currencyBalance", {}).get("currency") if record.get("currencyBalance") else None,
                "address": record.get("address") or payload.get("address"),
            }
        )
    return converted_records

def convert_accounting_data(raw_records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Converts raw data to match the 'user_currency_accounts' table schema."""
    converted_records = []
    payload = raw_records.get("payload", {})
    items = payload.get("items", []) if isinstance(payload, dict) else []
    for record in items:
        if record.get("accountCategory") == "LIQUID_ASSETS":
            converted_records.append(
                {
                    "category": record.get("accountCategory"),
                    "type": record.get("accountType"),
                    "number": record.get("account"),
                    "bookbalanceamount": record.get("bookBalance", {}).get("amount") if record.get("bookBalance") else None,
                    "balanceamount": record.get("balance", {}).get("amount") if record.get("balance") else None,
                    "address": record.get("address") or payload.get("address"),
                }
            )
    return converted_records
