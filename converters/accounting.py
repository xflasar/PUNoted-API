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
