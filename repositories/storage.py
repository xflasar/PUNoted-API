from typing import Any, Dict, List

import asyncpg

async def fetch_user_storages_by_id(con: asyncpg.Connection, user_id: str, storage_id: List[str]):
    records = await con.fetch(
        """
        SELECT 
            s.storageid,
            s.addressableid,
            s.name as storage_name,
            s.type,
            s.volumecapacity,
            s.volumeload,
            s.weightcapacity,
            s.weightload,
            s.xata_updatedat,
            COALESCE(p_site.name, p_warehouse.name) as planet_name,
            COALESCE(p_site.planetid, p_warehouse.planetid) as planet_id,
            station.name as station_name,
            station.stationid as station_id,
            si.quantity,
            si.currencyamount,
            mt.ticker,
            ud.displayname as username
        FROM storages as s
        LEFT JOIN storage_items as si ON s.storageid = si.storageid
        LEFT JOIN materials as mt ON mt.materialid = si.materialid
        LEFT JOIN warehouses as w ON s.storageid::text = w.storeid::text
        LEFT JOIN sites as st ON s.addressableid = st.siteid
        LEFT JOIN planets as p_site ON st.addressplanetid = p_site.planetid
        LEFT JOIN planets as p_warehouse ON w.addressplanet = p_warehouse.planetid
        LEFT JOIN stations as station ON station.warehouseid = s.addressableid
        INNER JOIN users_data as ud ON s.userid = ud.userid
        INNER JOIN users as u ON u.userdataid = ud.userid
        WHERE u.userdataid = $1 AND s.storageid = ANY($2::text[])
    """,
        user_id,
        storage_id,
    )

    if not records:
        return []

    grouped_storage_data: Dict[str, Any] = {}
    for record in records:
        storage_id = record["storageid"]

        # Initialize storage entry if not exists
        if storage_id not in grouped_storage_data:
            storage_name = record["storage_name"] or record["planet_name"]
            storagelocation = record["station_name"] or record["planet_name"]

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
            grouped_storage_data[storage_id]["items"].append({"name": record["ticker"], "quantity": record["quantity"]})
            grouped_storage_data[storage_id]["total_worth"] += item_worth

    return list(grouped_storage_data.values())