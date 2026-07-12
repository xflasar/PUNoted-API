import logging
from decimal import Decimal

from endpoints.Public.repositories.corp_repo import fetch_corp_prices

logger = logging.getLogger(__name__)

CSV_HEADERS = [
    "ticker", "price"
]

async def generate_json_data(db) -> list:
    try:
        records = await fetch_corp_prices(db)
        json_data = []

        for record in records:
            # Using the walrus operator (:=) to fetch, assign, and type-check in one optimized step.
            json_data.append({
                header: float(val) if isinstance(val := record.get(header, 0), Decimal) else val
                for header in CSV_HEADERS
            })

        return json_data
    except Exception as e:
        logger.error(f"Failed to generate JSON data for market data: {e}", exc_info=True)
        raise

async def get_ship_presets(db, corporation_id: str, user_id: str = None) -> list:
    import json
    from endpoints.Public.repositories.corp_repo import resolve_corp_id, fetch_ship_presets
    resolved_corp_id = await resolve_corp_id(db, corporation_id)
    records = await fetch_ship_presets(db, resolved_corp_id, user_id)
    presets = []
    for r in records:
        presets.append({
            "id": r["id"],
            "name": r["name"],
            "price": float(r["price"]),
            "priceCorp": float(r["price_corp"]),
            "parts": json.loads(r["parts"]) if isinstance(r["parts"], str) else r["parts"],
            "is_admin_preset": r["is_admin_preset"],
            "created_by": r["created_by"],
            "created_at": r["created_at"]
        })
    return presets

def format_order_row(r, requesting_user_id: str = None, is_admin: bool = False):
    import json
    config = json.loads(r["ship_config"]) if isinstance(r["ship_config"], str) else r["ship_config"]
    return {
        "id": r["id"],
        "corporation_id": r["corporation_code"],
        "customer": r["customer_username"],
        "customer_company_code": r["customer_company_code"],
        "shipType": config,
        "price": float(r["price"]),
        "waitTimeDays": r["wait_time_days"],
        "status": r["status"],
        "notes": r["notes"],
        "completionDate": r["completed_at"],
        "created_at": r["created_at"],
        "isOwner": (requesting_user_id is not None) and (r["owner_id"] == requesting_user_id),
        "isAdmin": is_admin
    }

async def get_ship_orders(db, corporation_id: str, user_id: str = None) -> list:
    import json
    from endpoints.Public.repositories.corp_repo import resolve_corp_id, is_corp_admin, fetch_ship_orders
    resolved_corp_id = await resolve_corp_id(db, corporation_id)
    if user_id:
        is_admin = await is_corp_admin(db, user_id, resolved_corp_id)
        records = await fetch_ship_orders(db, resolved_corp_id)
        return [format_order_row(r, user_id, is_admin) for r in records]
    else:
        records = await fetch_ship_orders(db, resolved_corp_id)
        results = []
        for r in records:
            config = json.loads(r["ship_config"]) if isinstance(r["ship_config"], str) else r["ship_config"]
            results.append({
                "id": r["id"],
                "corporation_id": r["corporation_code"],
                "customer": r["customer_username"],
                "customer_company_code": r["customer_company_code"],
                "shipType": {
                    "name": config.get("name"),
                    "parts": config.get("parts", [])
                },
                "price": float(r["price"]),
                "waitTimeDays": r["wait_time_days"],
                "status": r["status"],
                "notes": r["notes"],
                "completionDate": r["completed_at"],
                "created_at": r["created_at"],
                "isOwner": False,
                "isAdmin": False
            })
        return results

async def get_ship_order_by_pin(db, corporation_id: str, pin: str) -> dict:
    from fastapi import HTTPException
    from endpoints.Public.repositories.corp_repo import resolve_corp_id, fetch_ship_order_by_pin
    resolved_corp_id = await resolve_corp_id(db, corporation_id)
    row = await fetch_ship_order_by_pin(db, resolved_corp_id, pin)
    if not row:
        raise HTTPException(status_code=404, detail="Order not found or PIN is invalid")
    res = format_order_row(row, requesting_user_id=None, is_admin=False)
    res["isOwner"] = True
    return res

async def get_user_role(db, corporation_id: str, user_id: str = None) -> dict:
    if not user_id:
        return {"role": "GUEST"}
    from endpoints.Public.repositories.corp_repo import resolve_corp_id, is_corp_admin
    resolved_corp_id = await resolve_corp_id(db, corporation_id)
    is_admin = await is_corp_admin(db, user_id, resolved_corp_id)
    return {"role": "ADMIN" if is_admin else "USER"}
