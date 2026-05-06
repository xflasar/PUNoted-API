import logging
from typing import Any, Dict

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

        query = """
            -- 1. Find the single storage ID for the authenticated user (if they have one).
            WITH user_single_storage AS (
                SELECT
                    s.storageid
                FROM
                    storages AS s
                INNER JOIN
                    warehouses AS w ON w.warehouseid = s.addressableid
                INNER JOIN
                    systems AS sys ON w.addresssystem = sys.systemid
                INNER JOIN
                    users_data AS ud ON ud.userid = s.userid
                INNER JOIN
                    users AS u ON u.userdataid = ud.userid
                INNER JOIN
                    stations AS st ON st.warehouseid = w.warehouseid
                WHERE
                    u.accountid = $2 
                    AND sys.name = 'Hortus'
                    AND st.name != 'Hortus'
                LIMIT 1
            )
            -- 2. Select all materials for the given CX, and optionally join the user's storage quantity.
            SELECT
                mt.ticker,
                mt.materialid,
                COALESCE(si.quantity, 0) AS quantity,
                cxb.askprice,
                mp.price AS corpprice
            FROM
                cx_brokers AS cxb
            INNER JOIN
                materials AS mt ON mt.materialid = cxb.materialid
            INNER JOIN
                material_prices AS mp ON mp.ticker = mt.ticker
            LEFT JOIN
                storage_items AS si 
                ON 
                    si.materialid = mt.materialid 
                    -- This subquery returns the storageid if found, or NULL if the CTE is empty.
                    AND si.storageid = (SELECT storageid FROM user_single_storage) 
            WHERE
                cxb.ticker LIKE $1
            ORDER BY
                cxb.ticker;
        """

        search_pattern = f"%.{cx}"

        async with pool.acquire() as con:
            materials_data = await con.fetch(query, search_pattern, current_user_id_str)

            # Check if any materials were found and return the data
            if not materials_data:
                return JSONResponse(
                    status_code=404,
                    content={
                        "success": False,
                        "message": "No materials found for the given CX code.",
                    },
                )

            data = [dict(record) for record in materials_data]

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
