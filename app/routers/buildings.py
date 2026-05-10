# app/routers/internal/buildings_router.py
from typing import List

from fastapi import APIRouter, Depends

from app.api.db.dependencies import get_db
from app.core.security import require_internal_origin
from app.repositories.buildings_repository import BuildingsRepository
from app.schemas.internal_planner import InternalBuildingDTO
from app.services.buildings_service import BuildingsService

buildings_router = APIRouter(dependencies=[Depends(require_internal_origin)])

def get_buildings_service(db=Depends(get_db)) -> BuildingsService:
    repo = BuildingsRepository(db)
    return BuildingsService(repo)

@buildings_router.get("/", response_model=List[InternalBuildingDTO])
async def get_buildings_data(service: BuildingsService = Depends(get_buildings_service)):
    """
    Returns building DTOs strictly for the internal Base Planner UI.
    """
    return await service.get_planner_buildings()
