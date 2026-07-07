from typing import Optional

from fastapi import APIRouter, Depends, Query, Request, Response

from app.core.limiter import get_auth_key, get_public_key, limiter

from auth import OptionalAuth
from endpoints.Public.services.material_recipes_service import generate_recipes_json
from endpoints.Public.services.materials_service import generate_materials_data_csv, generate_materials_data_json
from endpoints.Public.schemas.materials import Material, Recipe
from typing import List

materials_router = APIRouter()

@materials_router.get(
    "/list",
    description="Get Materials list. Public access allowed.",
    response_class=Response,
    responses={
        200: {
            "model": List[Material],
            "description": "Returns a JSON list of materials."
        }
    }
)
@limiter.limit("120/minute", key_func=get_auth_key)
@limiter.limit("60/minute", key_func=get_public_key)
async def get_materials_list(
    request: Request,
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db

    json_string = await generate_materials_data_json(db)

    return Response(
        content=json_string,
        media_type="application/json",
        headers={"Cache-Control": "public, max-age=86400"}
    )

@materials_router.get(
    "/csv",
    description="Get Materials csv data. Public access allowed.",
    response_class=Response,
    responses={
        200: {
            "content": {"text/csv": {}},
            "description": "Returns a CSV file with materials data."
        }
    }
)
@limiter.limit("120/minute", key_func=get_auth_key)
@limiter.limit("60/minute", key_func=get_public_key)
async def get_materials_csv(
    request: Request,
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db

    csv_string = await generate_materials_data_csv(db)

    return Response(
        content=csv_string,
        media_type="text/csv",
        headers={
            "Content-Disposition": "inline; filename=materials_data.csv",
            "Cache-Control": "public, max-age=86400"
        }
    )

@materials_router.get(
    "/recipes",
    description="Get Material Recipes. Public access allowed.",
    response_class=Response,
    responses={
        200: {
            "model": List[Recipe],
            "description": "Returns List of recipes."
        }
    }
)
@limiter.limit("120/minute", key_func=get_auth_key)
@limiter.limit("60/minute", key_func=get_public_key)
async def get_material_recipes(
    request: Request,
    ticker: Optional[str] = Query(None, description="If filled, find recipes for single ticker."),
    tickers: Optional[str] = Query(None, description="If filled, find recipes for multiple comma-separated tickers."),
    user_id: Optional[str] = Depends(OptionalAuth())
):
    db = request.app.state.db

    json_string = await generate_recipes_json(db, ticker=ticker, tickers=tickers)

    return Response(
        content=json_string,
        media_type="application/json",
        headers={"Cache-Control": "public, max-age=86400"}
    )
