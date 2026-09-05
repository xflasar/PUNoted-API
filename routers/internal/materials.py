import json
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, Request, Response

from app.db.dependencies import get_db
from app.core.security import require_internal_origin
from app.repositories.materials_repository import MaterialsRepository
from app.schemas.internal_planner import InternalMaterialDTO
from app.services.materials_service import MaterialsService
from endpoints.Public.services.materials_service import generate_materials_data_json
from endpoints.Public.services.material_recipes_service import generate_recipes_json

materials_router = APIRouter(dependencies=[Depends(require_internal_origin)])

def get_materials_service(db=Depends(get_db)) -> MaterialsService:
    repo = MaterialsRepository(db)
    return MaterialsService(repo)

@materials_router.get("/", response_model=List[InternalMaterialDTO])
async def get_materials_data(service: MaterialsService = Depends(get_materials_service)):
    """
    Returns Material DTOs (with nested input recipes) 
    strictly for the internal Base Planner UI.
    """
    return await service.get_planner_materials()

@materials_router.get("/list")
async def get_internal_materials_list(request: Request):
    """
    Internal unthrottled materials list endpoint for website context.
    """
    db = request.app.state.db
    json_string = await generate_materials_data_json(db)
    return Response(content=json_string, media_type="application/json")

@materials_router.get("/recipes", response_class=Response)
async def get_internal_material_recipes(
    request: Request,
    ticker: Optional[str] = Query(None, description="If filled, find recipes for single ticker."),
    tickers: Optional[str] = Query(None, description="If filled, find recipes for multiple comma-separated tickers."),
):
    """
    Internal unthrottled material recipes endpoint for website context.
    """
    db = request.app.state.db
    json_string = await generate_recipes_json(db, ticker=ticker, tickers=tickers)
    return Response(content=json_string, media_type="application/json")
