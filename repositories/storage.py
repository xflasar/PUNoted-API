from typing import Any, Dict, List

import asyncpg

from db import Database


async def fetch_user_storages(db: Database, user_id: str):
    async with db.pool.acquire() as conn:
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
                    station.name as station_name,
                    si.quantity,
                    si.currencyamount,
                    mt.ticker
                FROM storage_items as si
                INNER JOIN storages as s ON s.storageid = si.storageid
                INNER JOIN materials as mt ON mt.materialid = si.materialid
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
            return []
        return records


async def fetch_user_storages_by_id(con: asyncpg.Connection, user_id: str, storage_id: List[str]):
    records = await con.fetch(
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
            storage_name = record["storage_name"] or record["warehouse_name"] or record["planet_name"]
            storagelocation = record["station_name"] or record["planet_name"]

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
            grouped_storage_data[storage_id]["items"].append({"name": record["ticker"], "quantity": record["quantity"]})
            grouped_storage_data[storage_id]["total_worth"] += item_worth

    return list(grouped_storage_data.values())
