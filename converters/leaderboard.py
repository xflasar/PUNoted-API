# converters/leaderboard.py
import datetime
import hashlib
import json
import string
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union


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

        # Safe fallback for materials
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
