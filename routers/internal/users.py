import logging
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import JSONResponse

from app.core.security import require_internal_origin
from auth import get_current_user_id

users_router = APIRouter(dependencies=[Depends(require_internal_origin)])
logger = logging.getLogger("users_router")

@users_router.get("/list")
async def list_users(request: Request, user_id: str = Depends(get_current_user_id)):
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            records = await conn.fetch(
                """
                WITH InternalUsers AS (
    -- Step 1: Gather all registered internal users
    SELECT
        U.ACCOUNTID,
        UD.DISPLAYNAME AS USERNAME,
        CD.COMPANYNAME,
        CD.COMPANYCODE
    FROM
        USERS AS U
        INNER JOIN USERS_DATA AS UD ON U.USERDATAID = UD.USERID
        INNER JOIN COMPANY_DATA AS CD ON U.USERDATAID = CD.USERDATAID
)
-- Step 2: Merge the internal list with the public list
SELECT DISTINCT
    IU.ACCOUNTID,
    COALESCE(IU.USERNAME, PUD.USERNAME) AS USERNAME,
    COALESCE(IU.COMPANYNAME, PUD.COMPANY_NAME) AS COMPANYNAME,
    COALESCE(IU.COMPANYCODE, PUD.COMPANY_CODE) AS COMPANYCODE,
    PUD.CREATED_TIMESTAMP,
    PUD.ACTIVE_DAYS_PER_WEEK
FROM InternalUsers AS IU
FULL OUTER JOIN PUBLIC_USERS_DATA AS PUD 
    ON PUD.COMPANY_CODE = IU.COMPANYCODE 
    AND PUD.USERNAME = IU.USERNAME
-- Step 3: Filter out the junk data
WHERE 
    COALESCE(IU.COMPANYNAME, PUD.COMPANY_NAME) IS NOT NULL 
    AND COALESCE(IU.COMPANYCODE, PUD.COMPANY_CODE) IS NOT NULL;
                """
            )

            if not records:
                return JSONResponse(content={"success": True, "data": []}, status_code=200)

            users_list: List[Dict[str, Any]] = [
                {
                    "accountid": str(record["accountid"]),
                    "username": record["username"],
                    "companyname": record["companyname"],
                    "companycode": record["companycode"]
                }
                for record in records
            ]

            return JSONResponse(content={"success": True, "data": users_list}, status_code=200)

    except Exception as e:
        logger.error(f"Error fetching users list: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    
@users_router.get("/search")
async def search_users(q: str, request: Request):
    if not q or len(q) < 2:
        return JSONResponse(content={"success": True, "data": []})

    search_term = f"%{q}%"  # For ILIKE fuzzy matching

    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            records = await conn.fetch(
                """
                WITH MatchedUsers AS (
                    SELECT 
                        U.ACCOUNTID as id, 
                        UD.DISPLAYNAME as username, 
                        CD.COMPANYCODE as company_code,
                        TRUE as is_registered
                    FROM users U
                    JOIN users_data UD ON U.USERDATAID = UD.USERID
                    JOIN company_data CD ON U.USERDATAID = CD.USERDATAID
                    WHERE UD.DISPLAYNAME ILIKE $1 OR CD.COMPANYCODE ILIKE $1

                    UNION ALL

                    SELECT 
                        NULL as id, 
                        USERNAME as username, 
                        COMPANY_CODE as company_code,
                        FALSE as is_registered
                    FROM public_users_data
                    WHERE USERNAME ILIKE $1 OR COMPANY_CODE ILIKE $1
                )
                -- DISTINCT ON tells Postgres to group by these specific columns
                SELECT DISTINCT ON (company_code, username) 
                    id, 
                    username, 
                    company_code, 
                    is_registered 
                FROM MatchedUsers
                -- We MUST order by the DISTINCT columns first. 
                -- Then we order by is_registered DESC so TRUE comes before FALSE.
                ORDER BY company_code, username, is_registered DESC
                LIMIT 10;
                """,
                search_term
            )

            results = [dict(r) for r in records]
            for r in results:
                if r.get("id"):
                    r["id"] = str(r["id"])

            return JSONResponse(content={"success": True, "data": results})

    except Exception as e:
        logger.error(f"Error searching users: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")