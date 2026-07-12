import logging
from typing import Any, Dict
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from app.core.security import require_internal_origin
from auth import get_current_user_id

logger = logging.getLogger(__name__)


vendor_router = APIRouter(dependencies=[Depends(require_internal_origin)])


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

        # Grouping by the resolved location IDs will automatically add Planetary Sites and Planetary Warehouses together.
        query_storage = """
            SELECT 
                si.materialid,
                m.ticker,
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
            GROUP BY 
                si.materialid, 
                m.ticker, 
                location_id, 
                location_name, 
                location_code;
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
                
                if row["location_id"] is not None:
                    available_qty = int(row["available"]) if row["available"] is not None else 0

                    storage_by_material[mat_id].append({
                        "id": row["location_id"],
                        "location_name": row["location_name"],
                        "location_code": row["location_code"],
                        "available": available_qty
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
                -- 1. Get Stations from WAREHOUSE_STORE
                SELECT
                    st.stationid AS location_id,
                    st.naturalid AS location_code,
                    st.name AS location_name,
                    'STATION' as type
                FROM storages s
                INNER JOIN warehouses w ON w.storeid = s.storageid
                INNER JOIN stations st ON st.warehouseid = w.warehouseid
                WHERE s.userid = (SELECT userdataid FROM users WHERE accountid = $1 LIMIT 1)
                AND s.type = 'WAREHOUSE_STORE'

                UNION

                -- 2. Get Planetary Warehouses from WAREHOUSE_STORE
                SELECT
                    p.planetid AS location_id,
                    p.naturalid AS location_code,
                    p.name AS location_name,
                    'PLANET' as type
                FROM storages s
                INNER JOIN warehouses w ON w.storeid = s.storageid
                INNER JOIN planets p ON p.planetid = w.addressplanet
                WHERE s.userid = (SELECT userdataid FROM users WHERE accountid = $1 LIMIT 1)
                AND s.type = 'WAREHOUSE_STORE'

                UNION

                -- 3. Get Planetary Sites from STORE
                SELECT
                    p.planetid AS location_id,
                    p.naturalid AS location_code,
                    p.name AS location_name,
                    'PLANET' as type
                FROM storages s
                INNER JOIN sites site ON site.siteid = s.addressableid
                INNER JOIN planets p ON p.planetid = site.addressplanetid
                WHERE s.userid = (SELECT userdataid FROM users WHERE accountid = $1 LIMIT 1)
                AND s.type = 'STORE'
            )
            SELECT DISTINCT
                location_id,
                location_code, 
                location_name,
                type
            FROM 
                user_locations 
            WHERE location_id IS NOT NULL
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