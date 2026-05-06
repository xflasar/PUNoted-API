import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse

from app.core.security import require_internal_origin
from auth import get_current_user_id

storage_router = APIRouter(dependencies=[Depends(require_internal_origin)])
logger = logging.getLogger("storage_router")


@storage_router.get("/user_storage")
async def get_user_storage(request: Request, user_id: str = Depends(get_current_user_id)):
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            records = await conn.fetch(
                """
                SELECT 
                    s.storageid,
                    s.name as storage_name,
                    s.type,
                    s.volumecapacity,
                    s.volumeload,
                    s.weightcapacity,
                    s.weightload,
                    s.xata_updatedat,
                    COALESCE(p_site.name, p_warehouse.name) as planet_name,
                    COALESCE(p_site.naturalid, p_warehouse.naturalid) as planet_naturalid,
                    station.name as station_name,
                    si.quantity,
                    si.currencyamount,
                    mt.ticker
                FROM storages as s
                LEFT JOIN storage_items as si ON s.storageid = si.storageid
                LEFT JOIN materials as mt ON mt.materialid = si.materialid
                LEFT JOIN warehouses as w ON s.addressableid = w.warehouseid
                LEFT JOIN sites as st ON s.addressableid = st.siteid
                LEFT JOIN planets as p_site ON st.addressplanetid = p_site.planetid
                LEFT JOIN planets as p_warehouse ON w.addressplanet = p_warehouse.planetid
                LEFT JOIN stations as station ON station.warehouseid = w.warehouseid
                INNER JOIN users_data as ud ON s.userid = ud.userid
                INNER JOIN users as u ON u.userdataid = ud.userid
                WHERE u.accountid = $1
            """,
                user_id,
            )

            if not records:
                return JSONResponse(content={"success": True, "data": []}, status_code=200)

            grouped_storage_data: Dict[str, Any] = {}
            for record in records:
                storage_id = record["storageid"]

                if storage_id not in grouped_storage_data:
                    storage_name = record["storage_name"] or record["planet_name"]
                    storagelocation = None
                    if record["station_name"]:
                        storagelocation = record["station_name"]
                    if record["planet_name"] and record["planet_naturalid"]:
                        storagelocation = f"{record['planet_name']} ({record['planet_naturalid']})"


                    grouped_storage_data[storage_id] = {
                        "storageid": storage_id,
                        "name": storage_name,
                        "type": record["type"],
                        "volumecapacity": record["volumecapacity"],
                        "volumeload": record["volumeload"],
                        "weightcapacity": record["weightcapacity"],
                        "weightload": record["weightload"],
                        "xata_update": record["xata_updatedat"].isoformat() if record["xata_updatedat"] else None,
                        "storagelocation": storagelocation,
                        "total_worth": 0.0,
                        "items": [],
                    }

                if record["ticker"] is not None:
                    item_worth = record["currencyamount"] or 0
                    grouped_storage_data[storage_id]["items"].append(
                        {"name": record["ticker"], "quantity": record["quantity"]}
                    )
                    grouped_storage_data[storage_id]["total_worth"] += item_worth

            final_data = list(grouped_storage_data.values())

            return JSONResponse(content={"success": True, "data": final_data})

    except Exception as e:
        logger.error(f"Failed to fetch user storage data: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}",
        )
