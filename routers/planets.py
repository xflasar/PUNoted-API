from fastapi import APIRouter, Depends, HTTPException, Request

from app.core.security import require_internal_origin

# from .auth import get_current_user

planets_router = APIRouter(dependencies=[Depends(require_internal_origin)])


@planets_router.get("/planets_names")
async def get_planets_names(
    request: Request,
    # current_user = Depends(get_current_user)
):
    """
    Fetches a list of planet names and their IDs.
    """
    try:
        db = request.app.state.db
        query = "SELECT planetid, naturalid FROM planets ORDER BY naturalid;"
        async with db.pool.acquire() as con:
            rows = await con.fetch(query)

        planets = [{"planetid": row["planetid"], "naturalid": row["naturalid"]} for row in rows]

        return {"status": "success", "data": planets}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
