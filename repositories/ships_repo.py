import uuid
from typing import List
from asyncpg import Connection, Record

async def repo_get_all_accessible_ships(conn: Connection, user_id: str) -> List[Record]:
    """
    Fetches all ships accessible to a user (Owned + Shared via Group/Corp).
    Includes nested JSON flight plans and segments for active flights.
    """
    user_uuid = uuid.UUID(user_id)

    query = """
        WITH my_ships AS (
            -- 1. My Personal Ships
            SELECT s.*, TRUE AS is_owner, FALSE AS is_corp, cd.companycode, ud.displayname,
            CASE
                WHEN st.volumecapacity >= 5000 AND st.weightcapacity >= 5000 THEN 'HCB'
                WHEN st.volumecapacity = 3000 AND st.weightcapacity = 1000 THEN 'VCB'
                WHEN st.volumecapacity = 1000 AND st.weightcapacity = 3000 THEN 'WCB'
                WHEN st.volumecapacity = 2000 AND st.weightcapacity = 2000 THEN 'LCB'
                WHEN st.volumecapacity = 500  AND st.weightcapacity = 500  THEN 'TINY'
                ELSE 'UNKNOWN'
            END AS ship_type
            FROM ships s
            JOIN storages st ON s.idshipstore = st.storageid
            JOIN users u ON s.userid = u.userdataid
            JOIN users_data ud ON ud.userid = u.userdataid
            JOIN company_data cd ON cd.companyid = ud.companyid
            WHERE u.accountid = $1
        ),
        shared_ships AS (
            -- 2. Ships shared by others (Corporation)
            SELECT s.*, FALSE AS is_owner, TRUE AS is_corp, cs.companycode, ud.displayname,
            CASE
                WHEN st.volumecapacity >= 5000 AND st.weightcapacity >= 5000 THEN 'HCB'
                WHEN st.volumecapacity = 3000 AND st.weightcapacity = 1000 THEN 'VCB'
                WHEN st.volumecapacity = 1000 AND st.weightcapacity = 3000 THEN 'WCB'
                WHEN st.volumecapacity = 2000 AND st.weightcapacity = 2000 THEN 'LCB'
                WHEN st.volumecapacity = 500  AND st.weightcapacity = 500  THEN 'TINY'
                ELSE 'UNKNOWN'
            END AS ship_type
            FROM ships s
            JOIN storages st ON s.idshipstore = st.storageid
            JOIN corporation_shareholders cs ON s.userid = cs.userid
            JOIN users_data ud ON ud.userid = cs.userid
            WHERE cs.corporationid IN (
                SELECT cs2.corporationid 
                FROM corporation_shareholders cs2 
                JOIN users u2 ON cs2.userid = u2.userdataid 
                WHERE u2.accountid = $1
            )
            AND s.userid != (SELECT userdataid FROM users WHERE accountid = $1)

            UNION

            -- 3. Ships shared by others (Groups)
            SELECT s.*, FALSE AS is_owner, FALSE AS is_corp, cd.companycode, ud.displayname,
            CASE
                WHEN st.volumecapacity >= 5000 AND st.weightcapacity >= 5000 THEN 'HCB'
                WHEN st.volumecapacity = 3000 AND st.weightcapacity = 1000 THEN 'VCB'
                WHEN st.volumecapacity = 1000 AND st.weightcapacity = 3000 THEN 'WCB'
                WHEN st.volumecapacity = 2000 AND st.weightcapacity = 2000 THEN 'LCB'
                WHEN st.volumecapacity = 500  AND st.weightcapacity = 500  THEN 'TINY'
                ELSE 'UNKNOWN'
            END AS ship_type
            FROM ships s
            JOIN data_group_members gm_target ON s.userid = (SELECT u2.userdataid FROM users u2 WHERE u2.accountid = gm_target.user_id)
            JOIN storages st ON s.idshipstore = st.storageid
            JOIN data_group_members gm_requester ON gm_target.group_id = gm_requester.group_id
            JOIN users u ON u.accountid = gm_target.user_id
            JOIN users_data ud ON ud.userid = u.userdataid
            JOIN company_data cd ON cd.companyid = ud.companyid
            WHERE gm_requester.user_id = $1
              AND gm_requester.status = 'ACCEPTED'
              AND gm_requester.can_read_data = TRUE
              AND gm_target.status = 'ACCEPTED'
              AND s.userid != (SELECT userdataid FROM users WHERE accountid = $1)
        ),
        combined_ships AS (
            SELECT * FROM my_ships 
            UNION ALL 
            SELECT * FROM shared_ships
        )
        SELECT 
            cs.*,
            -- Aggregate flight data on the final combined dataset
            CASE 
                WHEN cs.flightid IS NOT NULL THEN (
                    SELECT row_to_json(f)::jsonb || jsonb_build_object(
                        'segments', (
                            SELECT COALESCE(json_agg(row_to_json(seg) ORDER BY seg.segment_index ASC), '[]'::json)
                            FROM ship_flight_segments seg
                            WHERE seg.flight_id = f.id
                        )
                    )
                    FROM ship_flights f 
                    WHERE f.id = cs.flightid
                )
                ELSE NULL 
            END AS plan
        FROM combined_ships cs
        ORDER BY cs.name ASC;
    """
    return await conn.fetch(query, user_uuid)

async def repo_get_owner_ships(conn: Connection, user_id: str) -> List[Record]:
    """
    Optimized repository query to fetch all ship data for a specific user.
    Uses a single efficient SELECT statement to retrieve all fields, including nested flight plans.
    """
    user_uuid = uuid.UUID(user_id)
    
    query = """
        SELECT 
            s.shipid,
            s.userid,
            s.name,
            s.registration,
            s.type,
            s.addressplanetid,
            s.addressstationid,
            s.addresssystemid,
            s.acceleration,
            s.thrust,
            s.volume,
            s.mass,
            s.operatingemptymass,
            s.reactorpower,
            s.emitterpower,
            s.stlfuelflowrate,
            s.status,
            s.condition,
            s.commissioningtime,
            s.lastrepair,
            s.flightid,
            s.idftlfuelstore,
            s.idstlfuelstore,
            s.idshipstore,
            s.operatingtimeftl,
            s.operatingtimestl,
            s.blueprintnaturalid,
            TRUE AS is_owner,
            
            CASE 
                WHEN s.flightid IS NOT NULL THEN (
                    SELECT row_to_json(f)::jsonb || jsonb_build_object(
                        'segments', (
                            SELECT COALESCE(json_agg(row_to_json(seg) ORDER BY seg.segment_index ASC), '[]'::json)
                            FROM ship_flight_segments seg
                            WHERE seg.flightid = f.id
                        )
                    )
                    FROM flights f 
                    WHERE f.id = s.flightid
                )
                ELSE NULL 
            END AS plan
            
        FROM ships s
        JOIN users u ON s.userid = u.userdataid
        WHERE u.accountid = $1
        ORDER BY s.name ASC;
    """
    return await conn.fetch(query, user_uuid)