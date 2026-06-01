from typing import List
from asyncpg import Connection, Record

async def repo_get_ships_by_user(conn: Connection, user_id: str) -> List[Record]:
    """
    Optimized repository query to fetch all ship data for a specific user.
    Uses a single efficient SELECT statement to retrieve all fields required by the Ship model.
    """
    query = """
        SELECT 
            shipid,
            userid,
            name,
            registration,
            type,
            addressplanetid,
            addressstationid,
            addresssystemid,
            acceleration,
            thrust,
            volume,
            mass,
            operatingemptymass,
            reactorpower,
            emitterpower,
            stlfuelflowrate,
            status,
            condition,
            commissioningtime,
            lastrepair,
            flightid,
            idftlfuelstore,
            idstlfuelstore,
            idshipstore,
            operatingtimeftl,
            operatingtimestl,
            blueprintnaturalid
        FROM ships
        WHERE userid = $1
        ORDER BY name ASC;
    """
    return await conn.fetch(query, user_id)
