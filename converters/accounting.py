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


def convert_cash_bookings_data(raw_records: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Converts raw data from ACCOUNTING_CASH_BOOKINGS to match 'user_cash_transactions' table schema.
    Payload contains items: List[Dict] with transaction entries.
    """
    converted_records = []
    payload = raw_records.get("payload", {}) if isinstance(raw_records, dict) else {}
    items = payload.get("items", []) if isinstance(payload, dict) else []

    for record in items:
        if not isinstance(record, dict):
            continue

        amount_obj = record.get("amount") or record.get("bookAmount") or {}
        book_bal_obj = record.get("bookBalance") or record.get("balance") or {}

        time_obj = record.get("time")
        if time_obj and isinstance(time_obj, dict) and time_obj.get("timestamp"):
            tx_time = datetime.fromtimestamp(time_obj["timestamp"] / 1000, tz=timezone.utc)
        else:
            tx_time = datetime.now(timezone.utc)

        converted_records.append(
            {
                "period": record.get("period"),
                "number": record.get("number"),
                "item": record.get("item", 0),
                "type": record.get("type"),
                "account_category": record.get("accountCategory"),
                "account_type": record.get("accountType"),
                "account": record.get("account"),
                "debit": bool(record.get("debit", True)),
                "amount": float(amount_obj.get("amount") or 0.0),
                "currency": amount_obj.get("currency") or "ICA",
                "book_balance": float(book_bal_obj.get("amount")) if book_bal_obj.get("amount") is not None else None,
                "timestamp": tx_time,
            }
        )

    return converted_records

