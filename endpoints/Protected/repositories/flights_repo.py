# repositories/flights_repo.py

SQL_SEARCH_FLIGHTS = """
WITH TargetUsers AS (
    SELECT u.username, u.userdataid
    FROM users u
    -- Filter by the validated list of usernames
    WHERE u.username = ANY($1::text[])
),
FilteredFlights AS (
    -- 1. Select and LIMIT the flights for the target users
    SELECT 
        f.id, f.shipid, f.userid, f.departuretimestamp, f.arrivaltimestamp, 
        f.currentsegmentindex, f.aborted, f.stldistance, f.ftldistance,
        f.originplanetid, f.originstationid, f.originsystemid,
        f.destinationplanetid, f.destinationstationid, f.destinationsystemid,
        s.name as ship_name, s.registration as ship_registration, 
        tu.username -- Keep username for grouping later
    FROM ship_flights f
    JOIN TargetUsers tu ON tu.userdataid = f.userid
    JOIN ships s ON s.shipid = f.shipid
    WHERE 
        ($2::text IS NULL OR (s.registration = $2 OR s.name = $2))
        AND ($3::boolean IS NULL OR (
            CASE 
                WHEN $3 IS TRUE THEN (f.arrivaltimestamp > NOW() AND f.aborted = FALSE) 
                ELSE (f.arrivaltimestamp <= NOW() OR f.aborted = TRUE)
            END
        ))
    ORDER BY f.departuretimestamp DESC
    LIMIT $4 
),
SegmentDetails AS (
    -- 2. Process Segments ONLY for the limited flights above
    SELECT 
        seg.flight_id,
        seg.segment_index,
        seg.segment_type,
        seg.departure,
        seg.arrival,
        seg.stl_distance,
        seg.stl_fuel,
        seg.ftl_distance,
        seg.ftl_fuel,
        
        sys_o.systemid as o_sys_id, sys_o.name as o_sys_name, sys_o.naturalid as o_sys_nat,
        p_o.planetid as o_pl_id, p_o.name as o_pl_name, p_o.naturalid as o_pl_nat,
        st_o.stationid as o_st_id, st_o.name as o_st_name, st_o.naturalid as o_st_nat,

        sys_d.systemid as d_sys_id, sys_d.name as d_sys_name, sys_d.naturalid as d_sys_nat,
        p_d.planetid as d_pl_id, p_d.name as d_pl_name, p_d.naturalid as d_pl_nat,
        st_d.stationid as d_st_id, st_d.name as d_st_name, st_d.naturalid as d_st_nat

    FROM ship_flight_segments seg
    JOIN FilteredFlights f ON f.id = seg.flight_id 
    
    LEFT JOIN systems sys_o ON sys_o.systemid = seg.origin_system_id
    LEFT JOIN planets p_o ON p_o.planetid = seg.origin_location_id AND seg.origin_location_type = 'PLANET'
    LEFT JOIN stations st_o ON st_o.stationid = seg.origin_location_id AND seg.origin_location_type = 'STATION'
    LEFT JOIN systems sys_d ON sys_d.systemid = seg.destination_system_id
    LEFT JOIN planets p_d ON p_d.planetid = seg.destination_location_id AND seg.destination_location_type = 'PLANET'
    LEFT JOIN stations st_d ON st_d.stationid = seg.destination_location_id AND seg.destination_location_type = 'STATION'
),
AggregatedSegments AS (
    -- 3. Build JSON Arrays for Segments
    SELECT 
        sd.flight_id,
        jsonb_agg(
            jsonb_build_object(
                'Type', sd.segment_type,
                'DepartureTimeEpochMs', sd.departure,
                'ArrivalTimeEpochMs', sd.arrival,
                'StlDistance', sd.stl_distance,
                'StlFuelConsumption', sd.stl_fuel,
                'FtlDistance', sd.ftl_distance,
                'FtlFuelConsumption', sd.ftl_fuel,
                
                'OriginLines', (
                    SELECT jsonb_agg(line) FROM (
                        SELECT jsonb_build_object('Type', 'system', 'LineId', sd.o_sys_id, 'LineNaturalId', sd.o_sys_nat, 'LineName', sd.o_sys_name) WHERE sd.o_sys_id IS NOT NULL
                        UNION ALL
                        SELECT jsonb_build_object('Type', 'planet', 'LineId', sd.o_pl_id, 'LineNaturalId', sd.o_pl_nat, 'LineName', sd.o_pl_name) WHERE sd.o_pl_id IS NOT NULL
                        UNION ALL
                        SELECT jsonb_build_object('Type', 'station', 'LineId', sd.o_st_id, 'LineNaturalId', sd.o_st_nat, 'LineName', sd.o_st_name) WHERE sd.o_st_id IS NOT NULL
                    ) line
                ),

                'DestinationLines', (
                    SELECT jsonb_agg(line) FROM (
                        SELECT jsonb_build_object('Type', 'system', 'LineId', sd.d_sys_id, 'LineNaturalId', sd.d_sys_nat, 'LineName', sd.d_sys_name) WHERE sd.d_sys_id IS NOT NULL
                        UNION ALL
                        SELECT jsonb_build_object('Type', 'planet', 'LineId', sd.d_pl_id, 'LineNaturalId', sd.d_pl_nat, 'LineName', sd.d_pl_name) WHERE sd.d_pl_id IS NOT NULL
                        UNION ALL
                        SELECT jsonb_build_object('Type', 'station', 'LineId', sd.d_st_id, 'LineNaturalId', sd.d_st_nat, 'LineName', sd.d_st_name) WHERE sd.d_st_id IS NOT NULL
                    ) line
                ),

                'Origin', TRIM(
                    sd.o_sys_name || ' (' || sd.o_sys_nat || ')' || 
                    COALESCE(' - ' || sd.o_st_name || ' (' || sd.o_st_nat || ')', '') ||
                    COALESCE(' - ' || sd.o_pl_name || ' (' || sd.o_pl_nat || ')', '') ||
                    CASE WHEN sd.o_st_id IS NULL AND (sd.segment_type IN ('JUMP', 'CHARGE', 'APPROACH', 'LANDING')) THEN ' - Orbit' ELSE '' END
                ),

                'Destination', TRIM(
                    sd.d_sys_name || ' (' || sd.d_sys_nat || ')' || 
                    COALESCE(' - ' || sd.d_st_name || ' (' || sd.d_st_nat || ')', '') ||
                    COALESCE(' - ' || sd.d_pl_name || ' (' || sd.d_pl_nat || ')', '') ||
                    CASE WHEN sd.d_st_id IS NULL AND (sd.segment_type IN ('JUMP', 'CHARGE', 'APPROACH', 'DEPARTURE')) THEN ' - Orbit' ELSE '' END
                )
            ) ORDER BY sd.segment_index
        ) as segments_json
    FROM SegmentDetails sd
    GROUP BY sd.flight_id
),
ProcessedFlights AS (
    -- 4. Construct Flight Objects
    SELECT 
        f.username,
        jsonb_build_object(
            'FlightId', f.id,
            'ShipId', f.shipid,
            'ShipName', f.ship_name,
            'ShipRegistration', f.ship_registration,
            'DepartureTimeEpochMs', EXTRACT(EPOCH FROM f.departuretimestamp) * 1000,
            'ArrivalTimeEpochMs', EXTRACT(EPOCH FROM f.arrivaltimestamp) * 1000,
            'CurrentSegmentIndex', f.currentsegmentindex,
            'IsAborted', f.aborted,
            'StlDistance', f.stldistance,
            'FtlDistance', f.ftldistance,
            'Timestamp', f.departuretimestamp,
            'UserNameSubmitted', f.username,
            
            'Origin', TRIM(
                COALESCE(p_o.naturalid, s_o.name) || ' (' || COALESCE(sys_o.name, sys_o.naturalid) || ')' ||
                CASE WHEN s_o.stationid IS NOT NULL THEN ' - STATION' ELSE '' END
            ),
            
            'Destination', TRIM(
                COALESCE(p_d.naturalid, s_d.name) || ' (' || COALESCE(sys_d.name, sys_d.naturalid) || ')'
            ),

            'Segments', COALESCE(ag.segments_json, '[]'::jsonb)
        ) as flight_obj
    FROM FilteredFlights f
    LEFT JOIN AggregatedSegments ag ON ag.flight_id = f.id
    LEFT JOIN planets p_o ON p_o.planetid = f.originplanetid
    LEFT JOIN stations s_o ON s_o.stationid = f.originstationid
    LEFT JOIN systems sys_o ON sys_o.systemid = f.originsystemid
    LEFT JOIN planets p_d ON p_d.planetid = f.destinationplanetid
    LEFT JOIN stations s_d ON s_d.stationid = f.destinationstationid
    LEFT JOIN systems sys_d ON sys_d.systemid = f.destinationsystemid
)
-- 5. Final Aggregation: Group by Username
SELECT 
    COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'Username', sub.username,
                'Flights', sub.flights
            )
        ), 
        '[]'::jsonb
    )
FROM (
    SELECT username, jsonb_agg(flight_obj ORDER BY (flight_obj->>'DepartureTimeEpochMs')::numeric DESC) as flights
    FROM ProcessedFlights
    GROUP BY username
) sub;
"""

async def search_flights(conn, usernames_list, ship_identifier=None, is_current=None, limit=50):
    """
    Fetches flights for the list of validated usernames.
    Returns: [{ "Username": "x3m", "Flights": [...] }, ...]
    """
    return await conn.fetchval(SQL_SEARCH_FLIGHTS, usernames_list, ship_identifier, is_current, limit)
