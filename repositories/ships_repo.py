from typing import List
from asyncpg import Connection, Record

async def repo_get_ships_by_user(conn: Connection, user_id: str) -> List[Record]:
    """
    Optimized repository query to fetch all ship data for a specific user.
    Uses a single efficient SELECT statement to retrieve all fields required by the Ship model.
    """
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
            s.blueprintnaturalid
        FROM ships s
        JOIN users u ON s.userid = u.userdataid
        WHERE u.accountid = $1
        ORDER BY s.name ASC;
    """
    return await conn.fetch(query, user_id)
