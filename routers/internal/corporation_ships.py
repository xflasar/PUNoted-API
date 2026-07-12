import json
import logging
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.encoders import jsonable_encoder

from app.core.security import require_internal_origin
from auth import get_current_user_id
from models.ship_management_models import (
    ShipOrder, ShipOrderCreate, ShipOrderUpdate,
    ShipTypePreset, ShipTypePresetCreate, ShipTypePresetUpdate
)

logger = logging.getLogger(__name__)

corp_ships_internal_router = APIRouter(dependencies=[Depends(require_internal_origin)])

# Helper to resolve corporation ID from corporation code (e.g., COSM)
async def resolve_corp_id(conn, input_id: str) -> str:
    row = await conn.fetchrow("SELECT id FROM corporations WHERE code = $1 OR id = $1", input_id)
    if row:
        return row["id"]
    return input_id

# Helper to check if user is admin (checking founder or officers array in corporations)
async def is_corp_admin(pool, user_id: str, corp_id: str) -> bool:
    async with pool.acquire() as conn:
        # Check developer/default fallback: if founder/officers are empty/null for this corp, treat user as admin
        row = await conn.fetchrow("SELECT founder, officers FROM corporations WHERE id = $1 OR code = $1", corp_id)
        if row and (row["founder"] is None and not row["officers"]):
            return False  # No founder/officers, treat as non-admin for safety

        admin_check = await conn.fetchrow("""
            SELECT 1 FROM corporations c
            LEFT JOIN corporation_shareholders cs ON c.id = cs.corporationid
            LEFT JOIN users_data u ON cs.userid = u.userid
            LEFT JOIN users usr ON u.userid = usr.userdataid
            WHERE (c.id = $1 OR c.code = $1)
              AND usr.accountid = $2
              AND (
                c.founder = usr.accountid::text
                OR usr.accountid::text = ANY(c.officers)
              )
        """, corp_id, user_id)
        return admin_check is not None

# ==========================================================
# PRESETS
# ==========================================================
# GET endpoints have been moved to Public and Protected (/v1) routes.

@corp_ships_internal_router.post("/ship-presets", response_model=ShipTypePreset)
async def create_ship_preset(
    request: Request,
    preset: ShipTypePresetCreate,
    user_id: str = Depends(get_current_user_id)
):
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        resolved_corp_id = await resolve_corp_id(conn, preset.corporation_id)
        if preset.is_admin_preset:
            admin = await is_corp_admin(pool, user_id, resolved_corp_id)
            if not admin:
                raise HTTPException(status_code=403, detail="Not authorized to create corp presets.")

        parts_json = json.dumps([p.dict(exclude={'isAvailable'}) for p in preset.parts])
        
        row = await conn.fetchrow("""
            INSERT INTO ship_build_presets (corporation_id, name, price, price_corp, parts, is_admin_preset, created_by)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id, created_at
        """, resolved_corp_id, preset.name, preset.price, preset.priceCorp, parts_json, preset.is_admin_preset, user_id)
        
        return {**preset.dict(), "id": row["id"], "created_by": user_id, "created_at": row["created_at"]}

@corp_ships_internal_router.put("/ship-presets/{preset_id}")
async def update_ship_preset(
    preset_id: int,
    request: Request,
    update_data: ShipTypePresetUpdate,
    user_id: str = Depends(get_current_user_id)
):
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        record = await conn.fetchrow("SELECT is_admin_preset, created_by, corporation_id FROM ship_build_presets WHERE id = $1", preset_id)
        if not record:
            raise HTTPException(status_code=404, detail="Preset not found")
        
        if record["is_admin_preset"]:
            if not await is_corp_admin(pool, user_id, record["corporation_id"]):
                raise HTTPException(status_code=403, detail="Not authorized to edit corp presets")
        else:
            if record["created_by"] != user_id:
                raise HTTPException(status_code=403, detail="Not authorized to edit this preset")
        
        updates = []
        args = []
        idx = 1
        
        if update_data.name is not None:
            updates.append(f"name = ${idx}")
            args.append(update_data.name)
            idx += 1
        if update_data.price is not None:
            updates.append(f"price = ${idx}")
            args.append(update_data.price)
            idx += 1
        if update_data.priceCorp is not None:
            updates.append(f"price_corp = ${idx}")
            args.append(update_data.priceCorp)
            idx += 1
        if update_data.parts is not None:
            updates.append(f"parts = ${idx}")
            args.append(json.dumps([p.dict(exclude={'isAvailable'}) for p in update_data.parts]))
            idx += 1
            
        if not updates:
            return {"success": True}
            
        updates.append("updated_at = CURRENT_TIMESTAMP")
        args.append(preset_id)
        query = f"UPDATE ship_build_presets SET {', '.join(updates)} WHERE id = ${idx}"
        await conn.execute(query, *args)
        return {"success": True}

@corp_ships_internal_router.delete("/ship-presets/{preset_id}")
async def delete_ship_preset(
    preset_id: int,
    request: Request,
    user_id: str = Depends(get_current_user_id)
):
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        record = await conn.fetchrow("SELECT is_admin_preset, created_by, corporation_id FROM ship_build_presets WHERE id = $1", preset_id)
        if not record:
            raise HTTPException(status_code=404, detail="Preset not found")
            
        if record["is_admin_preset"]:
            if not await is_corp_admin(pool, user_id, record["corporation_id"]):
                raise HTTPException(status_code=403, detail="Not authorized")
        else:
            if record["created_by"] != user_id:
                raise HTTPException(status_code=403, detail="Not authorized")
                
        await conn.execute("DELETE FROM ship_build_presets WHERE id = $1", preset_id)
        return {"success": True}

# ==========================================================
#  ORDERS
# ==========================================================
def format_order_row(r, corp_code: str):
    return {
        "id": r["id"],
        "corporation_id": corp_code,
        "customer": r["customer_username"],
        "customer_company_code": r["customer_company_code"],
        "shipType": json.loads(r["ship_config"]),
        "price": float(r["price"]),
        "waitTimeDays": r["wait_time_days"],
        "status": r["status"],
        "notes": r["notes"],
        "completionDate": r["completed_at"],
        "created_at": r["created_at"],
        "isOwner": True,
        "isAdmin": True
    }

@corp_ships_internal_router.post("/ship-orders", response_model=ShipOrder)
async def create_ship_order(
    request: Request,
    order: ShipOrderCreate,
    user_id: str = Depends(get_current_user_id)
):
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        resolved_corp_id = await resolve_corp_id(conn, order.corporation_id)
        
        # Strip isAvailable from parts and serialize datetime safely
        ship_type_dict = jsonable_encoder(order.shipType)
        for part in ship_type_dict.get("parts", []):
            part.pop("isAvailable", None)
        config_json = json.dumps(ship_type_dict)

        row = await conn.fetchrow("""
            INSERT INTO corp_ship_orders (corporation_id, customer_username, customer_company_code, owner_type, owner_id, guest_pin, ship_config, price, wait_time_days, status, notes)
            VALUES ($1, $2, $3, 'USER', $4, $5, $6, $7, $8, 'QUEUED', $9)
            RETURNING *
        """, resolved_corp_id, order.customer, order.customer_company_code, user_id, order.guestPin, config_json, order.price, order.waitTimeDays, order.notes)
        
        return format_order_row(row, order.corporation_id)

@corp_ships_internal_router.post("/guest/ship-orders", response_model=ShipOrder)
async def create_guest_ship_order(
    request: Request,
    order: ShipOrderCreate
):
    if not order.guestPin:
        raise HTTPException(status_code=400, detail="Guest PIN required")
        
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        resolved_corp_id = await resolve_corp_id(conn, order.corporation_id)
        
        # Strip isAvailable from parts and serialize datetime safely
        ship_type_dict = jsonable_encoder(order.shipType)
        for part in ship_type_dict.get("parts", []):
            part.pop("isAvailable", None)
        config_json = json.dumps(ship_type_dict)

        row = await conn.fetchrow("""
            INSERT INTO corp_ship_orders (corporation_id, customer_username, customer_company_code, owner_type, guest_pin, ship_config, price, wait_time_days, status, notes)
            VALUES ($1, $2, $3, 'GUEST', $4, $5, $6, $7, 'QUEUED', $8)
            RETURNING *
        """, resolved_corp_id, order.customer, order.customer_company_code, order.guestPin, config_json, order.price, order.waitTimeDays, order.notes)
        
        return format_order_row(row, order.corporation_id)

@corp_ships_internal_router.put("/ship-orders/{order_id}")
async def update_ship_order(
    order_id: int,
    request: Request,
    update_data: ShipOrderUpdate,
    user_id: str = Depends(get_current_user_id)
):
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        record = await conn.fetchrow("SELECT owner_id, corporation_id, status FROM corp_ship_orders WHERE id = $1", order_id)
        if not record:
            raise HTTPException(status_code=404, detail="Order not found")
            
        admin = await is_corp_admin(pool, user_id, record["corporation_id"])
        is_owner = record["owner_id"] == user_id
        
        if not admin and not is_owner:
            raise HTTPException(status_code=403, detail="Not authorized")
            
        updates = []
        args = []
        idx = 1
        
        if update_data.notes is not None:
            updates.append(f"notes = ${idx}")
            args.append(update_data.notes)
            idx += 1
            
        if admin:
            if update_data.status is not None:
                updates.append(f"status = ${idx}")
                args.append(update_data.status)
                idx += 1
                if update_data.status == "COMPLETED" and record["status"] != "COMPLETED":
                    updates.append("completed_at = CURRENT_TIMESTAMP")
            if update_data.price is not None:
                updates.append(f"price = ${idx}")
                args.append(update_data.price)
                idx += 1
                
        if not updates:
            return {"success": True}
            
        args.append(order_id)
        query = f"UPDATE corp_ship_orders SET {', '.join(updates)} WHERE id = ${idx}"
        await conn.execute(query, *args)
        return {"success": True}

@corp_ships_internal_router.put("/guest/ship-orders/{order_id}")
async def update_guest_ship_order(
    order_id: int,
    request: Request,
    update_data: ShipOrderUpdate
):
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        record = await conn.fetchrow("SELECT guest_pin FROM corp_ship_orders WHERE id = $1", order_id)
        if not record or record["guest_pin"] != update_data.guestPin:
            raise HTTPException(status_code=403, detail="Invalid PIN or order not found")
            
        if update_data.notes is not None:
            await conn.execute("UPDATE corp_ship_orders SET notes = $1 WHERE id = $2", update_data.notes, order_id)
            
        return {"success": True}

@corp_ships_internal_router.delete("/ship-orders/{order_id}")
async def delete_ship_order(
    order_id: int,
    request: Request,
    user_id: str = Depends(get_current_user_id)
):
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        record = await conn.fetchrow("SELECT owner_id, corporation_id FROM corp_ship_orders WHERE id = $1", order_id)
        if not record:
            raise HTTPException(status_code=404, detail="Order not found")
            
        admin = await is_corp_admin(pool, user_id, record["corporation_id"])
        if not admin and record["owner_id"] != user_id:
            raise HTTPException(status_code=403, detail="Not authorized")
            
        await conn.execute("DELETE FROM corp_ship_orders WHERE id = $1", order_id)
        return {"success": True}

@corp_ships_internal_router.delete("/guest/ship-orders/{order_id}")
async def delete_guest_ship_order(
    order_id: int,
    request: Request,
    update_data: ShipOrderUpdate # Using update model to carry the guestPin
):
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        record = await conn.fetchrow("SELECT guest_pin FROM corp_ship_orders WHERE id = $1", order_id)
        if not record or record["guest_pin"] != update_data.guestPin:
            raise HTTPException(status_code=403, detail="Invalid PIN or order not found")
            
        await conn.execute("DELETE FROM corp_ship_orders WHERE id = $1", order_id)
        return {"success": True}
