from typing import Any, Dict, List, Optional

import asyncpg
import orjson


async def fetch_initial_ship_data(db, user_id: str) -> list[dict[str, Any]]:
    """Fetches initial ship data, including current flights for a given user."""

    ships = []
    # Join ships with ship_flights to get the current active flight ID
    ships_query = """
    WITH TargetUser AS (
    -- 1. Get the primary user's data ID (assuming users.userdataid links to ships.userid)
    SELECT 
        u.userdataid AS primary_userid
    FROM 
        users u
    WHERE 
        u.accountid = $1 -- Input: The current user's account ID
    ),
    TargetCorp AS (
        -- 2. Identify the Corporation ID(s) the primary user belongs to
        SELECT DISTINCT
            cs.corporationid AS corp_id
        FROM 
            TargetUser tu
        INNER JOIN corporation_shareholders cs ON cs.userid = tu.primary_userid
    ),
    CorpMembers AS (
        -- 3. Identify ALL unique User IDs (userid) belonging to the Target Corporation(s)
        SELECT DISTINCT
            cs.userid AS member_userid
        FROM 
            TargetCorp t
        INNER JOIN corporation_shareholders cs ON cs.corporationid = t.corp_id
        INNER JOIN users u ON cs.userid = u.userdataid
        WHERE u.accountid != $1 AND u.xata_updatedat > NOW() - INTERVAL '7 days'
    )
    SELECT
        s.*, -- All ship columns
        CASE 
        WHEN st.volumecapacity >= 5000 AND st.weightcapacity >= 5000 THEN 'HCB'
        WHEN st.volumecapacity = 3000 AND st.weightcapacity = 1000 THEN 'VCB'
        WHEN st.volumecapacity = 1000 AND st.weightcapacity = 3000 THEN 'WCB'
        WHEN st.volumecapacity = 2000 AND st.weightcapacity = 2000 THEN 'LCB'
        WHEN st.volumecapacity = 500  AND st.weightcapacity = 500  THEN 'TINY'
        ELSE 'UNKNOWN'
    END AS ship_type,
        st.volumeload,
        st.weightload,
        st.volumecapacity,
        st.weightcapacity,
        ud.displayname AS ship_owner_display_name,
        s.userid AS ship_owner_userid,

        -- Flag to easily identify the primary owner's ships in the application
        CASE 
            WHEN s.userid = tu.primary_userid THEN TRUE 
            ELSE FALSE 
        END AS is_owner_ship
    FROM 
        ships s
    INNER JOIN 
        users_data ud ON s.userid = ud.userid
    INNER JOIN 
        TargetUser tu ON TRUE -- Used for comparison in the CASE statement
    INNER JOIN
        storages st ON st.storageid = s.idshipstore
    WHERE 
        -- Select all ships owned by the primary user
        s.userid = tu.primary_userid

        OR 
        -- OR select all ships owned by any member of the corporation(s)
        s.userid IN (SELECT member_userid FROM CorpMembers) 

    ORDER BY is_owner_ship DESC, s.userid, s.shipid;
    """

    async with db.pool.acquire() as conn:
        records = await conn.fetch(ships_query, user_id)

        for record in records:
            # 1. Start with ship data (as a dict)
            ship_data = dict(record)

            flight_data = None
            flight_id = ship_data.get("flightid")

            # 2. Conditionally fetch and nest flight data
            if flight_id:
                # flight_data will contain segments nested within it
                flight_data = await fetch_flight_data(conn, flight_id)

            ship_data["flight"] = flight_data

            # Clean up the join field
            ship_data.pop("flightid", None)

            ships.append(ship_data)

    # Returns List[Dict[str, Any]] which is ready for final JSON serialization
    return ships


async def fetch_users_ship_data(db, user_id: str) -> list[dict[str, Any]]:
    """Fetches initial ship data, including current flights for a given user."""

    ships = []
    # Join ships with ship_flights to get the current active flight ID
    ships_query = """
    WITH TargetUser AS (
    -- 1. Get the primary user's data ID (assuming users.userdataid links to ships.userid)
    SELECT 
        u.userdataid AS primary_userid
    FROM 
        users u
    WHERE 
        u.accountid = $1 -- Input: The current user's account ID
    )
    SELECT
        s.*, -- All ship columns
        sy.name AS current_system,
        pl.naturalid AS current_planet,
        st.name AS current_station,
        ud.displayname AS ship_owner_display_name,
        s.userid AS ship_owner_userid,

        -- Flag to easily identify the primary owner's ships in the application
        CASE 
            WHEN s.userid = tu.primary_userid THEN TRUE 
            ELSE FALSE 
        END AS is_owner_ship
    FROM 
        ships s
    INNER JOIN 
        users_data ud ON s.userid = ud.userid
    INNER JOIN 
        TargetUser tu ON TRUE -- Used for comparison in the CASE statement
    LEFT JOIN
        systems sy ON sy.systemid = s.addresssystemid
    LEFT JOIN
        planets pl ON pl.planetid = s.addressplanetid
    LEFT JOIN
        stations st ON st.stationid = s.addressstationid
    WHERE 
        -- Select all ships owned by the primary user
        s.userid = tu.primary_userid

    -- ORDER BY owner and then by ship ID for better grouping in application logic
    ORDER BY is_owner_ship DESC, s.userid, s.shipid;
    """

    async with db.pool.acquire() as conn:
        records = await conn.fetch(ships_query, user_id)

        for record in records:
            # 1. Start with ship data (as a dict)
            ship_data = dict(record)

            flight_data = None
            flight_id = ship_data.get("flightid")

            # 2. Conditionally fetch and nest flight data
            if flight_id:
                # flight_data will contain segments nested within it
                flight_data = await fetch_flight_data(conn, flight_id)

            ship_data["flight"] = flight_data

            # Clean up the join field
            ship_data.pop("flightid", None)

            ships.append(ship_data)

    # Returns List[Dict[str, Any]] which is ready for final JSON serialization
    return ships


async def fetch_segments_data(conn: asyncpg.Connection, flight_id: str) -> List[Dict[str, Any]]:
    """Fetches segment data for a given flight ID."""
    segments_query = """
    SELECT
        segment_type, segment_index, "departure", "arrival", 
        duration, origin_system_id, origin_location_id, origin_location_type, 
        origin_orbit_data, destination_system_id, destination_location_id, 
        destination_location_type, destination_orbit_data, stl_distance, 
        ftl_distance, stl_fuel, ftl_fuel, damage, transferellipse
    FROM ship_flight_segments
    WHERE flight_id = $1
    ORDER BY segment_index ASC; -- Ensures correct segment order
    """
    segment_records = await conn.fetch(segments_query, flight_id)

    # Returns a list of dictionaries. The datetime objects remain raw here.
    segments_data = [dict(record) for record in segment_records]

    return segments_data


async def fetch_flight_data(conn: asyncpg.Connection, flight_id: str) -> Optional[Dict[str, Any]]:
    """Fetches flight data and associated segments for a given flight ID."""
    flight_query = """
    SELECT 
        sf.id, sf.originplanetid, sf.originstationid, sf.originsystemid, sf.shipid, 
        sf.stldistance, sf.ftldistance, sf.damage, sf.currentsegmentindex, 
        sf.destinationplanetid, sf.destinationsystemid, sf.destinationstationid, 
        sf.arrivaltimestamp, sf.departuretimestamp, 
        sf.stltotalconsumption, sf.ftltotalconsumption, plo.naturalid AS originplanet, 
        syO.name AS originsystem, stO.name AS originstation, plD.naturalid AS destinationplanet, 
        syD.name AS destinationsystem, stD.name AS destinationstation
    FROM ship_flights sf
    LEFT JOIN
        planets plO ON plO.planetid = sf.originplanetid
    LEFT JOIN
        systems syO ON syO.systemid = sf.originsystemid
    LEFT JOIN
        stations stO ON stO.stationid = sf.originstationid
    LEFT JOIN
        planets plD ON plD.planetid = sf.destinationplanetid
    LEFT JOIN
        systems syD ON syD.systemid = sf.destinationsystemid
    LEFT JOIN
        stations stD ON stD.stationid = sf.destinationstationid
    WHERE id = $1; 
    """
    flight_record = await conn.fetchrow(flight_query, flight_id)

    if not flight_record:
        return None

    # 1. Convert the record to a Python dictionary
    flight_data = dict(flight_record)

    # 2. Fetch segment data (list of dicts)
    segments_data = await fetch_segments_data(conn, flight_data["id"])

    # 3. Nest the segments list under the 'segments' key
    flight_data["segments"] = segments_data if segments_data else []

    # 4. Return the dictionary
    return flight_data


async def fetch_ship_data(conn: asyncpg.Connection, ship_id: str) -> Optional[Dict[str, Any]]:
    """Fetches ship data for a given ship ID."""
    ship_query = """
    SELECT 
        COALESCE(sh.name, sh.registration) AS shipname,
        p.naturalid AS currentplanet,
        s.name AS currentsystem,
        st.name AS currentstation,
        sh.addressplanetid,
        sh.addresssystemid,
        sh.addressstationid,
        sh.name,
        sh.shipid,
        sh.registration,
        sh.flightid
    FROM ships sh
    LEFT JOIN planets  p ON p.planetid  = sh.addressplanetid
    LEFT JOIN systems  s ON s.systemid  = sh.addresssystemid
    LEFT JOIN stations st ON st.stationid = sh.addressstationid
    WHERE sh.shipid = $1;
    """

    ship_record = await conn.fetchrow(ship_query, ship_id)

    return dict(ship_record) if ship_record else None


async def fetch_location_names(
    conn: asyncpg.Connection, planet_ids=None, system_ids=None, station_ids=None
) -> Dict[str, str]:
    """Fetch names for given planet, system, and station IDs."""
    planet_ids = list(filter(None, set(planet_ids or [])))
    system_ids = list(filter(None, set(system_ids or [])))
    station_ids = list(filter(None, set(station_ids or [])))

    if not (planet_ids or system_ids or station_ids):
        return {}

    query_parts = []
    params = []
    if planet_ids:
        query_parts.append("SELECT planetid AS id, naturalid AS name FROM planets WHERE planetid = ANY($1)")
        params.append(planet_ids)
    if system_ids:
        query_parts.append("SELECT systemid AS id, name AS name FROM systems WHERE systemid = ANY($2)")
        params.append(system_ids)
    if station_ids:
        query_parts.append("SELECT stationid AS id, name AS name FROM stations WHERE stationid = ANY($3)")
        params.append(station_ids)

    query = " UNION ALL ".join(query_parts)
    rows = await conn.fetch(query, *params)
    return {r["id"]: r["name"] for r in rows}


async def fetch_logistics_sites(db, user_id: str, production_summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Fetches site data including storage, warehouse, and production information.
    """
    sites_query = """
        SELECT
            s.siteid,
            p.name as "planetName",
            p.planetid,
            st.storageid,
            st.name as storage_name,
            st.weightcapacity, st.volumecapacity,
            st.weightload, st.volumeload,
            (SELECT json_agg(json_build_object('materialTicker', m.ticker, 'amount', si.quantity))
                FROM storage_items si
                JOIN materials m ON m.materialid = si.materialid
                WHERE si.storageid = st.storageid) as items
        FROM sites s
        JOIN users u ON u.userdataid = s.userid
        LEFT JOIN planets p ON s.addressplanetid = p.planetid
        LEFT JOIN storages st ON s.siteid = st.addressableid
        WHERE u.accountid = $1
    """

    warehouses_query = """
        SELECT
            w.warehouseid,
            p.planetid,
            st.storageid,
            st.name as storage_name,
            st.weightcapacity, st.volumecapacity,
            st.weightload, st.volumeload,
            (SELECT json_agg(json_build_object('materialTicker', m.ticker, 'amount', si.quantity))
                FROM storage_items si
                JOIN materials m ON m.materialid = si.materialid
                WHERE si.storageid = st.storageid) as items
        FROM warehouses w
        LEFT JOIN planets p ON w.addressplanet = p.planetid
        JOIN storages st ON w.warehouseid = st.addressableid
        JOIN users u ON u.userdataid = st.userid
        WHERE u.accountid = $1
    """

    async with db.pool.acquire() as conn:
        sites_records = await conn.fetch(sites_query, user_id)
        warehouses_records = await conn.fetch(warehouses_query, user_id)

        warehouses_by_planet = {}
        for r in warehouses_records:
            planet_id = r["planetid"]
            if planet_id not in warehouses_by_planet:
                warehouses_by_planet[planet_id] = []

            warehouses_by_planet[planet_id].append(
                {
                    "id": r["warehouseid"],
                    "name": r["storage_name"] or "Warehouse",
                    "maxTonnage": r["weightcapacity"] or 0,
                    "maxVolume": r["volumecapacity"] or 0,
                    "currentTonnage": r["weightload"] or 0,
                    "currentVolume": r["volumeload"] or 0,
                    "items": orjson.loads(r["items"] or "[]"),
                }
            )

        sites_list = []
        for r in sites_records:
            site_storage = None
            if r["storageid"]:
                site_storage = {
                    "id": r["storageid"],
                    "name": r["storage_name"] or "Site Storage",
                    "maxTonnage": r["weightcapacity"] or 0,
                    "maxVolume": r["volumecapacity"] or 0,
                    "currentTonnage": r["weightload"] or 0,
                    "currentVolume": r["volumeload"] or 0,
                    "items": orjson.loads(r["items"] or "[]"),
                }

            planet_warehouses = warehouses_by_planet.get(r["planetid"])
            warehouse_data = planet_warehouses[0] if planet_warehouses else None

            site_prod_cons = production_summary.get(r["siteid"], {})

            sites_list.append(
                {
                    "id": r["siteid"],
                    "name": r["planetName"],
                    "planetName": r["planetName"],
                    "siteStorage": site_storage,
                    "warehouse": warehouse_data,
                    "dailyProduction": site_prod_cons.get("dailyProduction", []),
                    "dailyConsumption": site_prod_cons.get("dailyConsumption", []),
                }
            )

        return sites_list
