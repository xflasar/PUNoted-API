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
                WITH
                	ME AS (
                		SELECT
                            U.ACCOUNTID::TEXT AS MY_ACCOUNTID,
                			U.USERDATAID::TEXT AS MY_UID,
                			UD.DISPLAYNAME AS USERNAME,
                			CD.COMPANYCODE
                		FROM
                			USERS U
                			LEFT JOIN USERS_DATA UD ON U.USERDATAID = UD.USERID
                			LEFT JOIN COMPANY_DATA CD ON U.USERDATAID = CD.USERDATAID
                		WHERE
                			U.ACCOUNTID = $1::UUID
                	),
                	INBOUNDLEASES AS (
                		SELECT
                			L ->> 'siteId' AS LEASED_SITEID,
                			U_LANDLORD.USERDATAID::TEXT AS LANDLORD_UID
                		FROM
                			USER_GLOBAL_SETTINGS UGS
                            JOIN USERS U_LANDLORD ON U_LANDLORD.ACCOUNTID::TEXT = UGS.USERID::TEXT
                			CROSS JOIN JSONB_ARRAY_ELEMENTS(COALESCE(UGS.INTERNAL_LEASED_SITES, '[]'::JSONB)) L
                			CROSS JOIN ME
                		WHERE
                			UGS.USERID::TEXT != ME.MY_ACCOUNTID
                			AND (
                				L ->> 'tenant' = ME.USERNAME
                				OR L ->> 'tenant' = ME.COMPANYCODE
                				OR L ->> 'tenant' = ME.USERNAME || ' (' || ME.COMPANYCODE || ')'
                			)
                	),
                	TARGETSTORAGES AS (
                		-- 1. My Own Storages
                		SELECT
                			STORAGEID
                		FROM
                			STORAGES
                		WHERE
                			USERID::TEXT = (
                				SELECT
                					MY_UID
                				FROM
                					ME
                			)
                		UNION
                		-- 2. Landlord's Site Storage (The base itself)
                		SELECT
                			ST.STORAGEID
                		FROM
                			STORAGES ST
                			JOIN INBOUNDLEASES IL ON ST.ADDRESSABLEID::TEXT = IL.LEASED_SITEID
                			AND ST.USERID::TEXT = IL.LANDLORD_UID
                		UNION
                		-- 3. Landlord's Warehouse Storage (On the same planet as the leased site)
                		SELECT
                			ST.STORAGEID
                		FROM
                			STORAGES ST
                			JOIN INBOUNDLEASES IL ON ST.USERID::TEXT = IL.LANDLORD_UID
                			JOIN WAREHOUSES W ON W.STOREID::TEXT = ST.STORAGEID::TEXT
                			AND W.USERID::TEXT = IL.LANDLORD_UID
                			JOIN SITES S ON S.SITEID::TEXT = IL.LEASED_SITEID
                			AND S.ADDRESSPLANETID = W.ADDRESSPLANET
                	)
                SELECT
                	S.STORAGEID,
                	S.ADDRESSABLEID,
                	S.NAME AS STORAGE_NAME,
                	S.TYPE,
                	S.VOLUMECAPACITY,
                	S.VOLUMELOAD,
                	S.WEIGHTCAPACITY,
                	S.WEIGHTLOAD,
                	S.XATA_UPDATEDAT,
                	COALESCE(P_SITE.NAME, P_WAREHOUSE.NAME) AS PLANET_NAME,
                	COALESCE(P_SITE.PLANETID, P_WAREHOUSE.PLANETID) AS PLANET_ID,
                	COALESCE(P_SITE.NATURALID, P_WAREHOUSE.NATURALID) AS PLANET_NATURALID,
                	STATION.NAME AS STATION_NAME,
                	STATION.STATIONID AS STATION_ID,
                	SI.QUANTITY,
                	SI.CURRENCYAMOUNT,
                	MT.TICKER,
                	UD.DISPLAYNAME AS USERNAME
                FROM
                	TARGETSTORAGES TS
                	JOIN STORAGES S ON S.STORAGEID = TS.STORAGEID
                	LEFT JOIN STORAGE_ITEMS AS SI ON S.STORAGEID = SI.STORAGEID
                	LEFT JOIN MATERIALS AS MT ON MT.MATERIALID = SI.MATERIALID
                	LEFT JOIN WAREHOUSES AS W ON W.STOREID::TEXT = S.STORAGEID::TEXT
                	LEFT JOIN SITES AS ST ON S.ADDRESSABLEID = ST.SITEID
                	LEFT JOIN PLANETS AS P_SITE ON ST.ADDRESSPLANETID = P_SITE.PLANETID
                	LEFT JOIN PLANETS AS P_WAREHOUSE ON W.ADDRESSPLANET = P_WAREHOUSE.PLANETID
                	LEFT JOIN STATIONS AS STATION ON STATION.WAREHOUSEID = W.WAREHOUSEID
                	INNER JOIN USERS_DATA AS UD ON S.USERID = UD.USERID;
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
                        "addressableid": record["addressableid"],
                        "owner": record["username"],
                        "name": storage_name,
                        "type": record["type"],
                        "volumecapacity": record["volumecapacity"],
                        "volumeload": record["volumeload"],
                        "weightcapacity": record["weightcapacity"],
                        "weightload": record["weightload"],
                        "xata_update": record["xata_updatedat"].isoformat() if record["xata_updatedat"] else None,
                        "storagelocation": storagelocation,
                        "storageplanetid": record["planet_id"],
                        "storagestationid": record["station_id"],
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