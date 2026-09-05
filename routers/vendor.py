import uuid
import json
import logging
from typing import Any, Dict, List, Optional
from decimal import Decimal
from datetime import datetime

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.core.security import require_internal_origin
from auth import get_current_user_id

logger = logging.getLogger(__name__)

vendor_router = APIRouter(dependencies=[Depends(require_internal_origin)])

class Location(BaseModel):
    id: str
    amount: int = 0

class OrderToUpdate(BaseModel):
    orderid: Optional[str] = None
    materialticker: str
    materialid: str
    ordertype: str
    fixedprice: float
    reserved: int = 0
    location: List[Location] = Field(default_factory=list)

class VendorToUpdate(BaseModel):
    companyName: str
    gameName: str
    companyCode: str
    corpName: Optional[str] = None
    cx: str

class EditOrdersRequest(BaseModel):
    vendorid: str
    vendor_to_update: VendorToUpdate
    orders_to_update: List[OrderToUpdate] = Field(default_factory=list)
    order_ids_to_delete: List[str] = Field(default_factory=list)

@vendor_router.post("/create_vendor_store")
async def create_vendor_store(
    payload: Dict[str, Any],
    request: Request,
    user_id: str = Depends(get_current_user_id),
):
    try:
        pool = request.app.state.db.pool
        vendor_data = payload.get("vendor_data", {})
        orders_data = payload.get("materials", [])

        vendor_id = str(uuid.uuid4())

        company_name = vendor_data.get("companyname")
        game_name = vendor_data.get("gamename")
        company_code = vendor_data.get("companycode")
        corp_name = vendor_data.get("corpname")
        is_active = vendor_data.get("isactive", True)
        cx = vendor_data.get("cx")

        required_fields = {
            "Company Name": company_name,
            "Game Name": game_name,
            "Company Code": company_code,
            "CX": cx,
        }

        for name, value in required_fields.items():
            cleaned_value = value.strip() if isinstance(value, str) else value
            if not cleaned_value:
                return JSONResponse(
                    status_code=400,
                    content={"success": False, "message": f"Vendor {name} is required."},
                )

        company_name = company_name.strip() if company_name else None
        game_name = game_name.strip() if game_name else None
        company_code = company_code.strip() if company_code else None
        corp_name = corp_name.strip() if corp_name else None
        cx = cx.strip() if cx else None

        async with pool.acquire() as conn:
            async with conn.transaction():
                vendor_query = """
                INSERT INTO user_vendors (
                    vendorid, userid, companyname, gamename, companycode, isactive, corpname, cx
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
                """
                await conn.execute(
                    vendor_query, vendor_id, user_id, company_name, game_name, company_code, is_active, corp_name, cx
                )

                order_records = []
                created_orders = []

                for order in orders_data:
                    order_id = str(uuid.uuid4())
                    fixed_price = order.get("fixedprice")
                    reserved_quantity = order.get("reserved")
                    material_id = order.get("materialid")
                    ticker = order.get("ticker")
                    order_type = order.get("orderType")

                    order_records.append(
                        (order_id, vendor_id, material_id, ticker, order_type, fixed_price, reserved_quantity)
                    )
                    created_orders.append(
                        {
                            "orderid": order_id,
                            "vendorid": vendor_id,
                            "materialid": material_id,
                            "materialticker": ticker,
                            "ordertype": order_type,
                            "fixedprice": fixed_price,
                            "reserved": reserved_quantity,
                        }
                    )

                if order_records:
                    orders_query = """
                    INSERT INTO user_vendor_orders (
                        orderid, vendorid, materialid, materialticker, ordertype, fixedprice, reserved
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7);
                    """
                    await conn.executemany(orders_query, order_records)

        response_vendor = {
            "vendorid": vendor_id,
            "userid": user_id,
            "companyname": company_name,
            "gamename": game_name,
            "companycode": company_code,
            "isactive": is_active,
            "corpname": corp_name,
            "cx": cx,
        }

        return JSONResponse(
            content={
                "success": True,
                "message": "Vendor store and orders created.",
                "vendor_store": {"vendor": response_vendor, "orders": created_orders},
            }
        )

    except asyncpg.UniqueViolationError:
        return JSONResponse(
            status_code=409,
            content={"success": False, "message": "A vendor store already exists for this user."},
        )
    except Exception as e:
        logger.error(f"Failed to create vendor store: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": "An unexpected server error occurred."},
        )

@vendor_router.get("/user_vendor_store")
async def get_user_vendor_stores(request: Request, user_id: str = Depends(get_current_user_id)):
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as con:
            vendor_record = await con.fetchrow(
                "SELECT vendorid, userid, companyname, gamename, companycode, isactive, corpname, cx FROM user_vendors WHERE userid = $1",
                user_id,
            )

            if not vendor_record:
                return JSONResponse(status_code=200, content={"success": True, "data": None})

            inventory_records = await con.fetch(
                """
                SELECT m.ticker, COALESCE(st.stationid, pl.planetid)::text AS location_id, SUM(si.quantity) AS quantity
                FROM storages s
                JOIN storage_items si ON si.storageid = s.storageid
                JOIN materials m ON m.materialid = si.materialid
                JOIN warehouses w ON w.warehouseid = s.addressableid
                LEFT JOIN stations st ON st.warehouseid = w.warehouseid
                LEFT JOIN sites site ON site.siteid = s.addressableid
                LEFT JOIN planets pl ON pl.planetid = site.addressplanetid
                INNER JOIN users u ON u.userdataid = s.userid
                WHERE u.accountid = $1
                GROUP BY m.ticker, location_id
                """,
                user_id,
            )
            inventory_map = {
                (r["ticker"], r["location_id"]): float(r["quantity"]) for r in inventory_records if r["location_id"]
            }

            orders_records = await con.fetch(
                """
                SELECT 
                    uvo.orderid, uvo.materialticker, uvo.ordertype, uvo.fixedprice, uvo.reserved, uvo.materialid,
                    COALESCE(uvo.location, '[]'::jsonb) AS locations,
                    mp.price AS corpprice,
                    CASE WHEN uvo.ordertype = 'buy' THEN cxb.askprice ELSE cxb.bidprice END AS cxprice
                FROM user_vendor_orders AS uvo
                LEFT JOIN material_prices AS mp ON mp.ticker = uvo.materialticker
                LEFT JOIN cx_brokers AS cxb ON cxb.ticker = (uvo.materialticker || '.' || $2)
                WHERE uvo.vendorid = $1;
                """,
                vendor_record["vendorid"],
                vendor_record["cx"],
            )

            all_location_ids = set()
            parsed_orders = []

            for r in orders_records:
                r_dict = dict(r)
                raw = r_dict["locations"]
                locs = json.loads(raw) if isinstance(raw, str) else (raw or [])
                r_dict["parsed_locs"] = locs
                parsed_orders.append(r_dict)
                for l in locs:
                    if "id" in l:
                        all_location_ids.add(l["id"])

            loc_lookup = {}
            if all_location_ids:
                loc_rows = await con.fetch(
                    """
                    SELECT stationid::text as id, name, naturalid FROM stations WHERE stationid::text = ANY($1)
                    UNION ALL
                    SELECT planetid::text as id, name, naturalid FROM planets WHERE planetid::text = ANY($1)
                    """,
                    list(all_location_ids),
                )
                for row in loc_rows:
                    loc_lookup[row["id"]] = {"name": row["name"], "code": row["naturalid"]}

            orders_list = []
            for r in parsed_orders:
                final_locations = []
                total_in_store = 0.0

                for loc in r["parsed_locs"]:
                    lid = loc.get("id")
                    details = loc_lookup.get(lid)
                    storage_qty = inventory_map.get((r["materialticker"], lid), 0.0)
                    total_in_store += storage_qty

                    final_locations.append(
                        {
                            "id": lid,
                            "amount": loc.get("amount", 0),
                            "location_name": details["name"] if details else "Unknown",
                            "location_code": details["code"] if details else "???",
                            "storage_amount": storage_qty,
                        }
                    )

                orders_list.append(
                    {
                        "orderid": str(r["orderid"]),
                        "materialid": r["materialid"],
                        "materialticker": r["materialticker"],
                        "ordertype": r["ordertype"],
                        "reserved": int(r["reserved"]),
                        "location": final_locations,
                        "quantity": total_in_store,
                        "price": {
                            "fixedprice": float(r["fixedprice"]) if r["fixedprice"] else 0.0,
                            "corpprice": float(r["corpprice"]) if r["corpprice"] else 0.0,
                            "cxprice": float(r["cxprice"]) if r["cxprice"] else 0.0,
                        },
                    }
                )

            return JSONResponse(
                content={
                    "success": True,
                    "data": {"vendor": dict(vendor_record), "orders": orders_list},
                }
            )

    except Exception as e:
        logger.error(f"Failed user vendor fetch: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"success": False, "message": "Server error."})

@vendor_router.post("/vendor_stores/edit_orders")
@vendor_router.post("/edit_orders")
async def edit_vendor_orders(
    request: Request,
    payload: EditOrdersRequest,
    user_id: str = Depends(get_current_user_id),
):
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as con:
            async with con.transaction():
                vendor_record = await con.fetchrow(
                    "SELECT userid, gamename, cx FROM user_vendors WHERE vendorid = $1",
                    payload.vendorid,
                )

                if not vendor_record or vendor_record["userid"] != user_id:
                    raise HTTPException(status_code=403, detail="Not authorized to edit this vendor store.")

                vendor_data = payload.vendor_to_update
                await con.execute(
                    """
                    UPDATE user_vendors
                    SET companyname = $1, companycode = $2, corpname = $3, gamename = $4, cx = $5
                    WHERE vendorid = $6;
                    """,
                    vendor_data.companyName,
                    vendor_data.companyCode,
                    vendor_data.corpName,
                    vendor_data.gameName,
                    vendor_data.cx,
                    payload.vendorid,
                )

                if payload.order_ids_to_delete:
                    await con.executemany(
                        "DELETE FROM user_vendor_orders WHERE orderid = $1 AND vendorid = $2",
                        [(order_id, payload.vendorid) for order_id in payload.order_ids_to_delete],
                    )

                for order in payload.orders_to_update:
                    order_id = order.orderid if order.orderid else str(uuid.uuid4())
                    locations_json = json.dumps([loc.dict() for loc in order.location]) if order.location else "[]"

                    await con.execute(
                        """
                        INSERT INTO user_vendor_orders (
                            orderid, vendorid, materialticker, materialid, ordertype, fixedprice, location
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                        ON CONFLICT (orderid) DO UPDATE SET
                            materialticker = EXCLUDED.materialticker,
                            materialid = EXCLUDED.materialid,
                            ordertype = EXCLUDED.ordertype,
                            fixedprice = EXCLUDED.fixedprice,
                            location = EXCLUDED.location
                        """,
                        order_id,
                        payload.vendorid,
                        order.materialticker,
                        order.materialid,
                        order.ordertype,
                        order.fixedprice,
                        locations_json,
                    )

            updated_vendor = await con.fetchrow(
                "SELECT vendorid, companycode, companyname, corpname, cx, gamename, isactive FROM user_vendors WHERE vendorid = $1",
                payload.vendorid,
            )

            orders_records = await con.fetch(
                """
                SELECT 
                    uvo.orderid, uvo.materialticker, uvo.ordertype, uvo.fixedprice, uvo.reserved, uvo.materialid,
                    COALESCE(uvo.location, '[]'::jsonb) AS locations,
                    mp.price AS corpprice,
                    CASE WHEN uvo.ordertype = 'buy' THEN cxb.askprice ELSE cxb.bidprice END AS cxprice
                FROM user_vendor_orders AS uvo
                LEFT JOIN material_prices AS mp ON mp.ticker = uvo.materialticker
                LEFT JOIN cx_brokers AS cxb ON cxb.ticker = (uvo.materialticker || '.' || $2)
                WHERE uvo.vendorid = $1;
                """,
                payload.vendorid,
                updated_vendor["cx"],
            )

            all_location_ids = set()
            parsed_orders = []
            for r in orders_records:
                r_dict = dict(r)
                raw = r_dict["locations"]
                locs = json.loads(raw) if isinstance(raw, str) else (raw or [])
                r_dict["parsed_locs"] = locs
                parsed_orders.append(r_dict)
                for l in locs:
                    if "id" in l:
                        all_location_ids.add(l["id"])

            loc_lookup = {}
            if all_location_ids:
                loc_rows = await con.fetch(
                    """
                    SELECT stationid::text as id, name, naturalid FROM stations WHERE stationid::text = ANY($1)
                    UNION ALL
                    SELECT planetid::text as id, name, naturalid FROM planets WHERE planetid::text = ANY($1)
                    """,
                    list(all_location_ids),
                )
                for row in loc_rows:
                    loc_lookup[row["id"]] = {"name": row["name"], "code": row["naturalid"]}

            final_orders_list = []
            for r in parsed_orders:
                enriched_locations = []
                for loc in r["parsed_locs"]:
                    lid = loc.get("id")
                    details = loc_lookup.get(lid)
                    enriched_locations.append(
                        {
                            "id": lid,
                            "amount": loc.get("amount", 0),
                            "location_name": details["name"] if details else "Unknown",
                            "location_code": details["code"] if details else "???",
                        }
                    )

                final_orders_list.append(
                    {
                        "orderid": str(r["orderid"]),
                        "materialid": r["materialid"],
                        "materialticker": r["materialticker"],
                        "ordertype": r["ordertype"],
                        "reserved": int(r["reserved"]),
                        "location": enriched_locations,
                        "price": {
                            "fixedprice": float(r["fixedprice"]) if r["fixedprice"] else 0.0,
                            "corpprice": float(r["corpprice"]) if r["corpprice"] else 0.0,
                            "cxprice": float(r["cxprice"]) if r["cxprice"] else 0.0,
                        },
                    }
                )

            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "message": "Vendor store updated successfully.",
                    "vendor_store": {
                        "vendor": dict(updated_vendor),
                        "orders": final_orders_list,
                    },
                },
            )

    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"success": False, "message": e.detail})
    except Exception as e:
        logger.error(f"Failed to edit vendor store: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"success": False, "message": "An unexpected server error occurred."})

@vendor_router.delete("/vendor_stores/{vendor_id}")
@vendor_router.delete("/{vendor_id}")
async def delete_vendor(vendor_id: str, request: Request, user_id: str = Depends(get_current_user_id)):
    pool = request.app.state.db.pool
    try:
        async with pool.acquire() as con:
            async with con.transaction():
                vendor_record = await con.fetchrow("SELECT userid FROM user_vendors WHERE vendorid = $1", vendor_id)
                if not vendor_record or vendor_record["userid"] != user_id:
                    raise HTTPException(status_code=403, detail="Not authorized to delete this vendor store.")

                await con.execute("DELETE FROM user_vendor_orders WHERE vendorid = $1;", vendor_id)
                await con.execute("DELETE FROM user_vendors WHERE vendorid = $1;", vendor_id)

                return JSONResponse(status_code=200, content={"success": True, "message": "Vendor store deleted."})
    except HTTPException as e:
        return JSONResponse(status_code=e.status_code, content={"success": False, "message": e.detail})
    except Exception as e:
        logger.error(f"Failed to delete vendor store: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"success": False, "message": "An unexpected server error occurred."})

@vendor_router.post("/materials_price_list")
async def get_materials_price_list(
    payload: Dict[str, Any],
    request: Request,
    user_id: str = Depends(get_current_user_id),
):
    try:
        pool = request.app.state.db.pool
        cx = payload.get("cx", None)

        if not cx:
            raise HTTPException(
                status_code=400,
                detail={"success": False, "message": "The 'cx' field is required in the request payload."},
            )

        current_user_id_str = str(user_id)
        query_materials = """
            SELECT mt.ticker, mt.materialid, cxb.askprice as askprice, mp.price AS corpprice
            FROM cx_brokers AS cxb
            LEFT JOIN materials AS mt ON mt.materialid = cxb.materialid
            LEFT JOIN material_prices AS mp ON mp.ticker = mt.ticker
            WHERE cxb.ticker LIKE $1
            ORDER BY cxb.ticker;
        """
        query_storage = """
            SELECT 
                si.materialid, m.ticker,
                COALESCE(st.stationid, pl_site.planetid, pl_w.planetid)::text AS location_id, 
                COALESCE(st.name, pl_site.name, pl_w.name)::text AS location_name,
                COALESCE(st.naturalid, pl_site.naturalid, pl_w.naturalid)::text AS location_code,
                COALESCE(SUM(si.quantity), 0) AS available
            FROM storages s
            JOIN storage_items si ON si.storageid = s.storageid
            LEFT JOIN materials m ON m.materialid = si.materialid
            LEFT JOIN warehouses w ON w.storeid = s.storageid AND s.type = 'WAREHOUSE_STORE'
            LEFT JOIN stations st ON st.warehouseid = w.warehouseid
            LEFT JOIN sites site ON site.siteid = s.addressableid AND s.type = 'STORE'
            LEFT JOIN planets pl_site ON pl_site.planetid = site.addressplanetid
            LEFT JOIN planets pl_w ON pl_w.planetid = w.addressplanet
            WHERE s.userid = (SELECT userdataid FROM users WHERE accountid = $1 LIMIT 1)
            AND s.type IN ('STORE', 'WAREHOUSE_STORE')
            AND si.type = 'INVENTORY' 
            GROUP BY si.materialid, m.ticker, location_id, location_name, location_code;
        """

        search_pattern = f"%.{cx}"
        async with pool.acquire() as con:
            materials_data = await con.fetch(query_materials, search_pattern)
            storage_data = await con.fetch(query_storage, current_user_id_str)

            if not materials_data:
                return JSONResponse(status_code=404, content={"success": False, "message": "No materials found for CX."})

            storage_by_material = {}
            for record in storage_data:
                row = dict(record)
                mat_id = row["materialid"]
                if mat_id not in storage_by_material:
                    storage_by_material[mat_id] = []
                if row["location_id"] is not None:
                    storage_by_material[mat_id].append({
                        "id": row["location_id"],
                        "location_name": row["location_name"],
                        "location_code": row["location_code"],
                        "available": int(row["available"]) if row["available"] is not None else 0
                    })

            data = []
            for record in materials_data:
                row = dict(record)
                mat_id = row["materialid"]
                for key, value in row.items():
                    if isinstance(value, Decimal):
                        row[key] = float(value)
                row["locations"] = storage_by_material.get(mat_id, [])
                row["quantity"] = sum(loc["available"] for loc in row["locations"])
                data.append(row)

            return JSONResponse(status_code=200, content={"success": True, "materials": data})

    except Exception as e:
        logger.error(f"Failed to get materials price list: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"success": False, "message": "An unexpected server error occurred."})

@vendor_router.get("/locations_list")
async def get_locations_list(request: Request, user_id: str = Depends(get_current_user_id)):
    try:
        pool = request.app.state.db.pool
        current_user_id_str = str(user_id)

        query = """
            WITH user_locations AS (
                SELECT st.stationid AS location_id, st.naturalid AS location_code, st.name AS location_name, 'STATION' as type
                FROM storages s
                INNER JOIN warehouses w ON w.storeid = s.storageid
                INNER JOIN stations st ON st.warehouseid = w.warehouseid
                WHERE s.userid = (SELECT userdataid FROM users WHERE accountid = $1 LIMIT 1)
                AND s.type = 'WAREHOUSE_STORE'
                UNION
                SELECT p.planetid AS location_id, p.naturalid AS location_code, p.name AS location_name, 'PLANET' as type
                FROM storages s
                INNER JOIN warehouses w ON w.storeid = s.storageid
                INNER JOIN planets p ON p.planetid = w.addressplanet
                WHERE s.userid = (SELECT userdataid FROM users WHERE accountid = $1 LIMIT 1)
                AND s.type = 'WAREHOUSE_STORE'
                UNION
                SELECT p.planetid AS location_id, p.naturalid AS location_code, p.name AS location_name, 'PLANET' as type
                FROM storages s
                INNER JOIN sites site ON site.siteid = s.addressableid
                INNER JOIN planets p ON p.planetid = site.addressplanetid
                WHERE s.userid = (SELECT userdataid FROM users WHERE accountid = $1 LIMIT 1)
                AND s.type = 'STORE'
            )
            SELECT DISTINCT location_id, location_code, location_name, type
            FROM user_locations 
            WHERE location_id IS NOT NULL
            ORDER BY location_name ASC;
        """

        async with pool.acquire() as con:
            locations_data = await con.fetch(query, current_user_id_str)
            data = [
                {
                    "id": record["location_id"],
                    "location_code": record["location_code"],
                    "location_name": record["location_name"],
                    "type": record["type"],
                }
                for record in locations_data
            ]
            return JSONResponse(status_code=200, content={"success": True, "locations": data})

    except Exception as e:
        logger.error(f"Failed to get locations list: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"success": False, "message": "An unexpected server error occurred."})