from typing import List, Optional

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from app.core.security import require_internal_origin

flights_router = APIRouter(dependencies=[Depends(require_internal_origin)])


# --- Pydantic Models for Response Schema ---
class Flight(BaseModel):
    origin_label: Optional[str]
    destination_label: Optional[str]
    flight_count: int
    avg_stldistance: float
    avg_ftldistance: float
    avg_stltotalconsumption: float
    avg_ftltotalconsumption: float
    avg_duration_minutes: float


class FlightSearchResponse(Flight):
    pass

@flights_router.get("/", response_model=List[FlightSearchResponse], summary="Get Flights Dashboard Data")
async def get_flights_dashboard(request: Request):
    SQL_QUERY = """
        SELECT
            -- 2. Route Names (Joins for Display)
            COALESCE(OP.name, OS.name, OY.name) AS origin_label,
            COALESCE(DP.name, DS.name, DY.name) AS destination_label,
        
            -- 3. Flight Count (Total trips for this route)
            COUNT(*) AS flight_count,
            
            -- 4. Distance Averages (Rounded to 2 decimal places)
            ROUND(AVG(T1.stldistance)::numeric, 2) AS avg_stldistance,
            ROUND(AVG(T1.ftldistance)::numeric, 2) AS avg_ftldistance,
            
            -- 5. Consumption Averages (Rounded to 2 decimal places)
            ROUND(AVG(T1.stltotalconsumption)::numeric, 2) AS avg_stltotalconsumption,
            ROUND(AVG(T1.ftltotalconsumption)::numeric, 2) AS avg_ftltotalconsumption,
            
            -- 6. Average Duration in Minutes (Rounded to 2 decimal places)
            ROUND(AVG(EXTRACT(EPOCH FROM (T1.departuretimestamp - T1.arrivaltimestamp)) / 60)::numeric, 2) AS avg_duration_minutes
        
        FROM
            ship_flights AS T1
        
        -- --- ORIGIN LOOKUP JOINS ---
        LEFT JOIN planets AS OP ON T1.originplanetid = OP.planetid
        LEFT JOIN stations AS OS ON T1.originstationid = OS.stationid
        LEFT JOIN systems AS OY ON T1.originsystemid = OY.systemid
        
        -- --- DESTINATION LOOKUP JOINS ---
        LEFT JOIN planets AS DP ON T1.destinationplanetid = DP.planetid
        LEFT JOIN stations AS DS ON T1.destinationstationid = DS.stationid
        LEFT JOIN systems AS DY ON T1.destinationsystemid = DY.systemid
        
        GROUP BY
            -- Grouping by IDs
            T1.originplanetid,
            T1.originstationid,
            T1.originsystemid,
            T1.destinationplanetid,
            T1.destinationstationid,
            T1.destinationsystemid,
            
            -- Include the joined names in GROUP BY to display them
            OP.name,
            OS.name,
            OY.name,
            DP.name,
            DS.name,
            DY.name
        
        ORDER BY
            flight_count DESC
    """
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        records = await conn.fetch(SQL_QUERY)

    if not records:
        return []

    return [FlightSearchResponse(**record) for record in records]
