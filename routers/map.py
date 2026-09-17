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


class GovernanceCandidate(BaseModel):
    candidate_id: Optional[str] = None
    userid: Optional[str] = None
    username: Optional[str] = None
    corporation_name: Optional[str] = None
    corporation_code: Optional[str] = None
    country_code: Optional[str] = None
    votes: Optional[int] = None
    votes_percentage: Optional[float] = None
    is_winner: Optional[bool] = None
    start_of_run: Optional[str] = None


class GovernanceTerm(BaseModel):
    termid: str
    admincenterid: str
    term_start: Optional[str] = None
    term_end: Optional[str] = None
    election_start: Optional[str] = None
    election_end: Optional[str] = None
    parliament_size: Optional[int] = None
    election_ongoing: Optional[bool] = None
    candidates: List[GovernanceCandidate] = []


class GovernanceMotion(BaseModel):
    motionid: str
    admincenterid: Optional[str] = None
    naturalid: Optional[str] = None
    motion_name: Optional[str] = None
    status: Optional[str] = None
    creator_id: Optional[str] = None
    creator_username: Optional[str] = None
    created_at: Optional[str] = None
    voting_start: Optional[str] = None
    voting_end: Optional[str] = None
    votes: List[Dict[str, Any]] = []
    components: List[Dict[str, Any]] = []


class PlanetGovernanceResponse(BaseModel):
    planetid: str
    name: str
    naturalid: Optional[str] = None
    admincenterid: Optional[str] = None
    terms: List[GovernanceTerm] = []
    motions: List[GovernanceMotion] = []


class Planet(BaseModel):
    planetid: str
    name: str
    naturalid: Optional[str] = None


class PlanetSystemSearchResponse(System):
    sector: Sector


class PlanetSearchResponse(Planet):
    system: PlanetSystemSearchResponse
    governance: Optional[Dict[str, Any]] = None


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
    if not hasattr(request.state, "db") or not hasattr(request.app.state.db, "pool"):
        raise HTTPException(status_code=500, detail="Database connection pool not found.")

    SQL_QUERY = """
    SELECT
        p.planetid, p.name AS planet_name, p.naturalid, p.admincenterid,
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

    results: List[PlanetSearchResponse] = []
    for record in records:
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
            "governance": {
                "admincenterid": record["admincenterid"],
                "has_admincenter": record["admincenterid"] is not None
            }
        }
        results.append(PlanetSearchResponse.model_validate(planet_data))

    return results


# --- 4. GET PLANET GOVERNANCE DETAILS FOR MAP ---
@map_router.get(
    "/planets/governance",
    response_model=List[PlanetGovernanceResponse],
    summary="Get Planet Government Terms (Members) & Motions for Map",
)
async def get_planet_governance(
    request: Request,
    planet: Optional[str] = Query(None, description="Planet Natural ID or Name or Admin Center ID")
):
    from endpoints.Public.repositories.governance_repo import fetch_planet_government_terms, fetch_planet_motions
    db = request.app.state.db

    terms_data = await fetch_planet_government_terms(db, planet=planet)
    motions_data = await fetch_planet_motions(db, planet=planet, full=True)

    # Group terms and motions by planet
    grouped: Dict[str, Dict[str, Any]] = {}
    
    for t in terms_data:
        key = t.get("planet_natural_id_code") or t.get("admincenterid") or "UNKNOWN"
        if key not in grouped:
            grouped[key] = {
                "planetid": key,
                "name": t.get("planet_name") or key,
                "naturalid": t.get("planet_natural_id_code"),
                "admincenterid": t.get("admincenterid"),
                "terms": [],
                "motions": []
            }
        grouped[key]["terms"].append(t)

    for m in motions_data:
        key = m.get("planet_natural_id_code") or m.get("admincenterid") or m.get("naturalid") or "UNKNOWN"
        if key not in grouped:
            grouped[key] = {
                "planetid": key,
                "name": m.get("planet_name") or key,
                "naturalid": m.get("planet_natural_id_code") or m.get("naturalid"),
                "admincenterid": m.get("admincenterid"),
                "terms": [],
                "motions": []
            }
        grouped[key]["motions"].append(m)

    return [PlanetGovernanceResponse.model_validate(val) for val in grouped.values()]

