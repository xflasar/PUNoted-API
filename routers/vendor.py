import logging
from typing import Any, Dict
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from app.core.security import require_internal_origin
from auth import get_current_user_id

logger = logging.getLogger(__name__)


vendor_router = APIRouter(prefix="/vendor", tags=["Vendor"], dependencies=[Depends(require_internal_origin)])


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
                detail={
                    "success": False,
                    "message": "The 'cx' field is required in the request payload.",
                },
            )

        current_user_id_str = str(user_id)

        query_materials = """
            SELECT
                mt.ticker,
                mt.materialid,
                cxb.askprice as askprice,
                mp.price AS corpprice
            FROM
                cx_brokers AS cxb
            LEFT JOIN
                materials AS mt ON mt.materialid = cxb.materialid
            LEFT JOIN
                material_prices AS mp ON mp.ticker = mt.ticker
            WHERE
                cxb.ticker LIKE $1
            ORDER BY
                cxb.ticker;
        """

        query_storage = """
            SELECT 
                si.materialid,
                COALESCE(st.stationid, pl.planetid, pl_w.planetid)::text AS location_id, 
                COALESCE(st.name, pl.name, pl_w.name)::text AS location_name,
                COALESCE(st.naturalid, pl.naturalid, pl_w.naturalid)::text AS location_code,
                SUM(si.quantity) AS available
            FROM storages s
            JOIN storage_items si ON si.storageid = s.storageid
            JOIN materials m ON m.materialid = si.materialid
            JOIN warehouses w ON w.warehouseid = s.addressableid
            LEFT JOIN stations st ON st.warehouseid = w.warehouseid
            LEFT JOIN sites site ON site.siteid = s.addressableid
            LEFT JOIN planets pl ON pl.planetid = site.addressplanetid
            LEFT JOIN planets pl_w ON pl_w.planetid = w.addressplanet
            INNER JOIN users u ON u.userdataid = s.userid
            WHERE u.accountid = $1
            GROUP BY si.materialid, location_id, location_name, location_code;
        """

        search_pattern = f"%.{cx}"

        async with pool.acquire() as con:
            materials_data = await con.fetch(query_materials, search_pattern)
            storage_data = await con.fetch(query_storage, current_user_id_str)

            if not materials_data:
                return JSONResponse(
                    status_code=404,
                    content={
                        "success": False,
                        "message": "No materials found for the given CX code.",
                    },
                )

            # Group storage items by materialid
            storage_by_material = {}
            for record in storage_data:
                row = dict(record)
                mat_id = row["materialid"]
                if mat_id not in storage_by_material:
                    storage_by_material[mat_id] = []
                storage_by_material[mat_id].append({
                    "id": row["location_id"],
                    "location_name": row["location_name"],
                    "location_code": row["location_code"],
                    "available": int(row["available"])
                })

            # Format final response
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
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "message": "An unexpected server error occurred.",
            },
        )


@vendor_router.get("/locations_list")
async def get_locations_list(request: Request, user_id: str = Depends(get_current_user_id)):
    try:
        pool = request.app.state.db.pool

        current_user_id_str = str(user_id)

        query = """
            WITH user_locations AS (
                -- 1. Get Stations (User has Storage -> Warehouse -> Station)
                SELECT
                    st.stationid AS location_id,
                    st.naturalid AS location_code,
                    st.name AS location_name,
                    'STATION' as type
                FROM 
                    storages s
                INNER JOIN 
                    users u ON u.userdataid = s.userid
                INNER JOIN 
                    warehouses w ON w.warehouseid = s.addressableid
                INNER JOIN 
                    stations st ON st.warehouseid = w.warehouseid
                WHERE 
                    u.accountid = $1

                UNION

                -- 2. Get Planets (User has Site -> Planet)
                SELECT
                    p.planetid AS location_id,
                    p.naturalid AS location_code,
                    p.name AS location_name,
                    'PLANET' as type
                FROM 
                    sites si
                INNER JOIN 
                    users u ON u.userdataid = si.userid
                INNER JOIN 
                    planets p ON p.planetid = si.addressplanetid
                WHERE 
                    u.accountid = $1
            )
            SELECT DISTINCT
                location_id,
                location_code, 
                location_name,
                type
            FROM 
                user_locations 
            ORDER BY 
                location_name ASC;
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
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "message": "An unexpected server error occurred.",
            },
        )
