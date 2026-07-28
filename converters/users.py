# converters/users.py
import datetime
import hashlib
import json
import string
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union


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
