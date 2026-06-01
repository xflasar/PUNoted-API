from typing import List

from fastapi import APIRouter, Depends

from app.db.dependencies import get_db
from app.core.security import require_internal_origin
from app.repositories.materials_repository import MaterialsRepository
from app.schemas.internal_planner import InternalMaterialDTO
from app.services.materials_service import MaterialsService

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
