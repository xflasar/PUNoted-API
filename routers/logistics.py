from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from app.core.security import require_internal_origin
from helpers.logistics_engine import run_logistics_pipeline
import orjson
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from auth import get_current_user_id
from helpers.fetchdb import fetch_users_ship_data
from helpers.logistics_analysis import (
    calculate_logistics_summary_and_recommendations,
    calculate_site_production_flow,
    generate_ai_logistics_strategy,
)
from helpers.production_lines import get_production_data_nested

logistics_router = APIRouter(prefix="/logistics", tags=["logistics"], dependencies=[Depends(require_internal_origin)])


async def get_ship_storage(db, ship_ids: List[str]) -> Dict[str, Any]:
    if not ship_ids:
        return {}

    query = """
        SELECT
            sh.shipid,
            st.storageid,
            st.name as storage_name,
            st.weightcapacity,
            st.volumecapacity,
            st.weightload,
            st.volumeload,
            (
                SELECT json_agg(json_build_object('materialTicker', m.ticker, 'amount', si.quantity))
                FROM storage_items si
                JOIN materials m ON m.materialid = si.materialid
                WHERE si.storageid = st.storageid
            ) as items
        FROM ships sh
        JOIN storages st ON sh.idshipstore = st.storageid
        WHERE sh.shipid = ANY($1::text[])
    """
    async with db.pool.acquire() as conn:
        records = await conn.fetch(query, ship_ids)
        storages = {}
        for r in records:
            storages[r["shipid"]] = {
                "id": r["storageid"],
                "name": r["storage_name"] or "Cargo Hold",
                "maxTonnage": r["weightcapacity"],
                "maxVolume": r["volumecapacity"],
                "currentTonnage": r["weightload"],
                "currentVolume": r["volumeload"],
                "items": orjson.loads(r["items"] or "[]"),
            }
        return storages
    
@logistics_router.post("/ai-strategy")
async def get_ai_logistics_strategy(request: Request, user_id: str = Depends(get_current_user_id)):
    """
    Generates a logistics plan using local LLM based on current site status.
    """
    if not user_id:
        return JSONResponse(content={"success": False, "data": []}, status_code=401)

    db = request.app.state.db

    # 1. Fetch World State
    sites_data = await _get_enriched_sites_data(db, user_id)
    ships_response = await get_logistics_ships(request, user_id)
    ships_data = ships_response.get("data", [])

    # 2. Run the "Analyst" (Math)
    # This identifies the problems (bottlenecks)
    summary, recommendations = calculate_logistics_summary_and_recommendations(sites_data, ships_data)

    # 3. Run the "Executive" (AI)
    # This assigns the ships to solve the problems
    ai_strategy = await generate_ai_logistics_strategy(summary, recommendations, ships_data)

    return {"success": True, "data": ai_strategy}

@logistics_router.post("/plan")
async def generate_logistics_plan(request: Request, user_id: str = Depends(get_current_user_id)):
    """
    Triggers the Full Logistics Engine:
    1. Fetches Fleet, Sites, Market Data
    2. Calculates Graph-based costs (STL/FTL)
    3. Asks AI for the optimal Strategy
    """
    if not user_id:
        return JSONResponse(content={"success": False, "message": "Unauthorized"}, status_code=401)

    db = request.app.state.db

    # Call the new orchestrator
    result = await run_logistics_pipeline(db, user_id)

    if not result.get("success", False) and "status" in result:
        # If engine returned a logic error (e.g. no bottlenecks)
        return JSONResponse(content=result, status_code=200)

    return result


import helpers.production_engine as production_engine

class ProductionLine(BaseModel):
    ticker: str
    count: int
    produces: str

class InputLine(BaseModel):
    material: str
    source: str
    amount: float
    jumps: int

class SitePlan(BaseModel):
    name: str
    system: str
    area_used: int
    workforce: Dict[str, int]
    buildings: List[ProductionLine]
    inputs: List[InputLine]

class GlobalSummary(BaseModel):
    total_area_used: int
    total_workforce: Dict[str, int]
    total_shopping_list: Dict[str, float]
    total_extraction: Dict[str, float]
    bill_of_materials: Dict[str, float]

class ProductionResponse(BaseModel):
    hub_name: str
    total_score: float
    summary: GlobalSummary
    sites: List[SitePlan]

class ProductionRequest(BaseModel):
    ship_parts: List[str]
    recipe_overrides: Optional[Dict[str, str]] = None
    hub_ticker: Optional[str] = "IC1"

@logistics_router.post("/optimize_chain")
async def optimize_ship_production(payload: ProductionRequest, request: Request):
    pool = request.app.state.db.pool
    async with pool.acquire() as conn:
        result = await production_engine.find_optimal_production_chain(
            conn, payload.ship_parts, payload.recipe_overrides, payload.hub_ticker
        )
        return result

@logistics_router.get("/ships")
async def get_logistics_ships(request: Request, user_id: str = Depends(get_current_user_id)):
    db = request.app.state.db
    if not user_id:
        return JSONResponse(content={"success": False, "data": []}, status_code=401)

    ships = await fetch_users_ship_data(db, user_id)
    ship_ids = [s.get("shipid") for s in ships]

    ship_storages = await get_ship_storage(db, ship_ids)

    mapped_ships = []
    for s in ships:
        ship_id = s.get("shipid")
        mapped_ships.append(
            {
                "id": ship_id,
                "name": s.get("name") or s.get("registration"),
                "status": "in-transit" if s.get("flight") else "idle",
                "locationId": s.get("current_planet") or s.get("current_station") or s.get("current_system"),
                "assignedSiteId": None,
                "currentFlight": s.get("flight"),
                "shipStorage": ship_storages.get(ship_id),
            }
        )
    return {"success": True, "data": mapped_ships}


async def _get_enriched_sites_data(db, user_id: str) -> List[Dict[str, Any]]:
    """Helper to fetch all site data and enrich it with production flow."""
    sites_dict = await get_production_data_nested(db.pool, user_id, include_logistics_data=True)

    enriched_sites = []
    for site_id, site_details in sites_dict.items():
        production_flow = calculate_site_production_flow(site_details)

        site_object = {
            "id": site_id,
            "name": site_details.get("planet_name"),
            "planetName": site_details.get("planet_name"),
            "siteStorage": site_details.get("siteStorage"),
            "warehouse": None,
            "dailyProduction": production_flow.get("dailyProduction", []),
            "dailyConsumption": production_flow.get("dailyConsumption", []),
            "storage_items": site_details.get("storage_items", []),
        }
        enriched_sites.append(site_object)
    return enriched_sites

@logistics_router.get("/galaxy-map")
async def get_galaxy_map_temp(request: Request):
    db = request.app.state.db

    async with db.pool.acquire() as conn:
        # Fetch rows
        sys_rows = await conn.fetch("SELECT systemid, name, positionx as x, positiony as y FROM systems WHERE positionx IS NOT NULL")
        conn_rows = await conn.fetch("SELECT systemidorigin, systemiddestination FROM system_connections")
        
        systems = [dict(row) for row in sys_rows]
        connections = [dict(row) for row in conn_rows]
        
        # Flattened response to match Streamlit's expectation
        return {
            "systems": systems, 
            "connections": connections
        }

@logistics_router.get("/sites")
async def get_logistics_sites_endpoint(request: Request, user_id: str = Depends(get_current_user_id)):
    if not user_id:
        return JSONResponse(content={"success": False, "data": []}, status_code=401)

    sites_data = await _get_enriched_sites_data(request.app.state.db, user_id)
    return {"success": True, "data": sites_data}


@logistics_router.get("/summary")
async def get_logistics_summary(request: Request, user_id: str = Depends(get_current_user_id)):
    if not user_id:
        return JSONResponse(content={"success": False, "data": []}, status_code=401)

    sites_data = await _get_enriched_sites_data(request.app.state.db, user_id)
    ships_response_content = await get_logistics_ships(request, user_id)
    ships_data = ships_response_content.get("data", [])

    summary, _ = calculate_logistics_summary_and_recommendations(sites_data, ships_data)

    return {"success": True, "data": summary}


@logistics_router.get("/cxs")
async def get_logistics_cxs(request: Request, user_id: str = Depends(get_current_user_id)):
    # Mock data
    cxs_data = [
        {
            "id": "cx-moria",
            "name": "Moria Exchange",
            "location": "Moria",
            "listings": [{"materialTicker": "H2O", "price": 150, "quantity": 100000}],
        }
    ]
    return {"success": True, "data": cxs_data}


@logistics_router.get("/recommendations")
async def get_logistics_recommendations(request: Request, user_id: str = Depends(get_current_user_id)):
    if not user_id:
        return JSONResponse(content={"success": False, "data": []}, status_code=401)

    sites_data = await _get_enriched_sites_data(request.app.state.db, user_id)
    ships_response_content = await get_logistics_ships(request, user_id)
    ships_data = ships_response_content.get("data", [])

    _, recommendations = calculate_logistics_summary_and_recommendations(sites_data, ships_data)

    return {"success": True, "data": recommendations}


@logistics_router.post("/ships/{shipId}/assign")
async def assign_ship_to_site(shipId: str, request: Request, user_id: str = Depends(get_current_user_id)):
    body = await request.json()
    site_id = body.get("siteId")
    return {"success": True, "message": f"Ship {shipId} assigned to site {site_id}"}


@logistics_router.post("/recommendations/{recommendationId}/approve")
async def approve_recommendation(recommendationId: str, user_id: str = Depends(get_current_user_id)):
    return {"success": True, "message": f"Recommendation {recommendationId} approved."}


@logistics_router.post("/recommendations/{recommendationId}/reject")
async def reject_recommendation(recommendationId: str, user_id: str = Depends(get_current_user_id)):
    return {"success": True, "message": f"Recommendation {recommendationId} rejected."}
