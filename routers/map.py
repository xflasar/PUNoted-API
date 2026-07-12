from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from app.core.security import require_internal_origin


# --- Pydantic Models for Response Schema ---
class Sector(BaseModel):
    externalsectorid: str
    name: str


class System(BaseModel):
    systemid: str
    name: str


class Planet(BaseModel):
    planetid: str
    name: str
    naturalid: Optional[str]


class PlanetSystemSearchResponse(System):
    sector: Sector


class PlanetSearchResponse(Planet):
    system: PlanetSystemSearchResponse


map_router = APIRouter(dependencies=[Depends(require_internal_origin)])


# --- 1. GET SECTORS ---
@map_router.get("/sectors", response_model=List[Sector], summary="Get all available Sectors")
async def get_sectors(request: Request):
    SQL_QUERY = """
    SELECT DISTINCT externalsectorid, name
    FROM sectors
    ORDER BY name;
    """

    if not hasattr(request.app.state, "db") or not hasattr(request.app.state.db, "pool"):
        raise HTTPException(status_code=500, detail="Database connection pool not found.")

    async with request.app.state.db.pool.acquire() as conn:
        try:
            records = await conn.fetch(SQL_QUERY)
            return [dict(record) for record in records]

        except Exception as e:
            print(f"Database error fetching sectors: {e}")
            raise HTTPException(status_code=500, detail="Internal server error while fetching data.")


# --- 2. GET SYSTEMS by Sector ID ---
@map_router.get("/systems", response_model=List[System], summary="Get Systems by Sector")
async def get_systems_by_sector(
    request: Request,
    sector_id: str = Query(..., alias="sector", description="The ID of the sector"),
):
    if not sector_id:
        raise HTTPException(status_code=400, detail="Sector ID is required.")

    SQL_QUERY = """
    SELECT DISTINCT systemid, name
    FROM systems
    WHERE sectorid = $1
    ORDER BY name;
    """

    if not hasattr(request.state, "db") or not hasattr(request.app.state.db, "pool"):
        raise HTTPException(status_code=500, detail="Database connection pool not found.")

    async with request.app.state.db.pool.acquire() as conn:
        try:
            records = await conn.fetch(SQL_QUERY, sector_id)
            return [dict(record) for record in records]

        except Exception as e:
            print(f"Database error fetching systems: {e}")
            raise HTTPException(status_code=500, detail="Internal server error.")


# --- 3. GET PLANETS by System ID ---
@map_router.get("/planets", response_model=List[Planet], summary="Get Planets by System")
async def get_planets_by_system(
    request: Request,
    system_id: str = Query(..., alias="system", description="The ID of the system"),
):
    if not system_id:
        raise HTTPException(status_code=400, detail="System ID is required.")

    SQL_QUERY = """
    SELECT DISTINCT planetid, name, naturalid
    FROM planets
    WHERE systemid = $1
    ORDER BY name;
    """

    if not hasattr(request.state, "db") or not hasattr(request.app.state.db, "pool"):
        raise HTTPException(status_code=500, detail="Database connection pool not found.")

    async with request.app.state.db.pool.acquire() as conn:
        try:
            records = await conn.fetch(SQL_QUERY, system_id)
            return [dict(record) for record in records]

        except Exception as e:
            print(f"Database error fetching planets: {e}")
            raise HTTPException(status_code=500, detail="Internal server error.")


# add pagination
@map_router.get(
    "/planets/search",
    response_model=List[PlanetSearchResponse],
    summary="Search Planets by Name or Natural ID",
)
async def search_planets(
    request: Request,
    query: str = Query(
        ...,
        min_length=2,
        description="The name or natural ID of the planet to search for.",
    ),
):
    """
    Searches for planets whose name or natural ID partially matches the provided query.

    This function performs a single, optimized database query to retrieve the Planet,
    its containing System, and the containing Sector simultaneously.

    Returns a list of matching planets.
    """

    if not hasattr(request.state, "db") or not hasattr(request.app.state.db, "pool"):
        raise HTTPException(status_code=500, detail="Database connection pool not found.")

    SQL_QUERY = """
    SELECT
        p.planetid, p.name AS planet_name, p.naturalid,
        s.systemid, s.name AS system_name,
        r.externalsectorid, r.name AS sector_name
    FROM planets p
    JOIN systems s ON p.systemid = s.systemid
    JOIN sectors r ON s.sectorid = r.externalsectorid
    WHERE p.name ILIKE $1 OR p.naturalid ILIKE $1
    ORDER BY p.name
    LIMIT 100;
    """

    search_term = f"%{query}%"

    try:
        async with request.app.state.db.pool.acquire() as conn:
            records = await conn.fetch(SQL_QUERY, search_term)

    except Exception as e:
        print(f"Database error during planet search: {e}")
        raise HTTPException(status_code=500, detail="Internal server error while searching.")

    results: List[Planet] = []
    for record in records:
        print(record)
        planet_data: Dict[str, Any] = {
            "planetid": record["planetid"],
            "name": record["planet_name"],
            "naturalid": record["naturalid"],
            "system": {
                "systemid": record["systemid"],
                "name": record["system_name"],
                "sector": {
                    "externalsectorid": record["externalsectorid"],
                    "name": record["sector_name"],
                },
            },
        }
        results.append(PlanetSearchResponse.model_validate(planet_data))

    return results
