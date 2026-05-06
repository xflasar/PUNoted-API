# repositories/ships_repo.py

SQL_SEARCH_SHIPS = """
WITH TargetUsers AS (
    SELECT u.username, u.accountid, u.userdataid
    FROM users u
    WHERE u.username = ANY($1::text[]) -- Filter by verified list
),
FilteredShips AS (
    SELECT 
        s.shipid, s.registration, s.name, s.flightid, 
        s.commissioningtime, s.condition, s.stlfuelflowrate,
        s.addresssystemid, s.addressplanetid, s.addressstationid,
        st_store.weightcapacity, st_store.volumecapacity,
        tu.username,
        sys.name as sys_name, sys.naturalid as sys_naturalid,
        p.name as planet_name, p.naturalid as planet_naturalid,
        st.name as station_name, st.naturalid as station_naturalid
    FROM ships s
    JOIN TargetUsers tu ON tu.userdataid = s.userid
    LEFT JOIN storages st_store ON st_store.storageid = s.idshipstore
    LEFT JOIN systems sys ON sys.systemid = s.addresssystemid
    LEFT JOIN planets p ON p.planetid = s.addressplanetid
    LEFT JOIN stations st ON st.stationid = s.addressstationid
    WHERE
      -- 1. Ship Name / Registration Filter
      ($2::text IS NULL OR (s.name ILIKE $2 OR s.registration ILIKE $2))
      
      -- 2. In Flight Filter
      AND ($3::boolean IS NULL OR (
          CASE 
            WHEN $3 IS TRUE THEN s.flightid IS NOT NULL 
            ELSE s.flightid IS NULL 
          END
      ))

      -- 3. Location Filter
      AND ($4::text IS NULL OR (
          p.name ILIKE $4 OR 
          p.naturalid ILIKE $4 OR 
          sys.name ILIKE $4 OR
          sys.naturalid ILIKE $4 OR
          st.name ILIKE $4
      ))

      -- 4. Ship Type Filter (Based on Capacity)
      AND ($5::text IS NULL OR (
          CASE 
            WHEN st_store.weightcapacity = 5000 AND st_store.volumecapacity = 5000 THEN 'HCB'
            WHEN st_store.weightcapacity = 3000 AND st_store.volumecapacity = 1000 THEN 'WCB'
            WHEN st_store.weightcapacity = 1000 AND st_store.volumecapacity = 3000 THEN 'VCB'
            WHEN st_store.weightcapacity = 2000 AND st_store.volumecapacity = 2000 THEN 'LCB'
            WHEN st_store.weightcapacity = 500 AND st_store.volumecapacity = 500 THEN 'TINY'
            ELSE 'OTHER'
          END = $5
      ))
)
SELECT 
    COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'Username', sub.username,
                'Ships', sub.ships
            )
        ),
        '[]'::jsonb
    )
FROM (
    SELECT 
        username,
        jsonb_agg(
            jsonb_build_object(
                'ShipId', shipid,
                'Registration', registration,
                'Name', name,
                'FlightId', flightid,
                'CommissioningTimeEpochMs', EXTRACT(EPOCH FROM commissioningtime) * 1000,
                'Condition', condition,
                'StlFuelFlowRate', stlfuelflowrate,
                
                'Type', CASE 
                    WHEN weightcapacity = 5000 AND volumecapacity = 5000 THEN 'HCB'
                    WHEN weightcapacity = 3000 AND volumecapacity = 1000 THEN 'WCB'
                    WHEN weightcapacity = 1000 AND volumecapacity = 3000 THEN 'VCB'
                    WHEN weightcapacity = 2000 AND volumecapacity = 2000 THEN 'LCB'
                    WHEN weightcapacity = 500 AND volumecapacity = 500 THEN 'TINY'
                    ELSE 'OTHER'
                END,

                'Location', TRIM(
                    COALESCE(sys_name, sys_naturalid) || 
                    CASE 
                        WHEN addressstationid IS NOT NULL THEN ' - ' || station_name || ' (' || station_naturalid || ')'
                        WHEN addressplanetid IS NOT NULL THEN ' - ' || planet_name || ' (' || planet_naturalid || ')'
                        ELSE ''
                    END
                ),
                
                'SystemId', addresssystemid,
                'PlanetId', addressplanetid,
                'StationId', addressstationid
            ) ORDER BY registration
        ) as ships
    FROM FilteredShips
    GROUP BY username
) sub;
"""

async def search_ships(
    conn,
    usernames_list: list,
    shipname: str = None,
    inflight: bool = None,
    location: str = None,
    ship_type: str = None,
):
    """
    Searches for ships for the provided list of users.
    Returns: [{ "Username": "x", "Ships": [...] }]
    """
    p_shipname = f"%{shipname}%" if shipname else None
    p_location = f"%{location}%" if location else None

    result = await conn.fetchval(SQL_SEARCH_SHIPS, usernames_list, p_shipname, inflight, p_location, ship_type)
    return result or "[]"