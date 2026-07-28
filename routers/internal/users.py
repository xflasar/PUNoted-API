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

@users_router.post("/delete_data")
async def delete_user_data(request: Request, user_id: str = Depends(get_current_user_id)):
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            async with conn.transaction():
                # 1. Fetch userdataid
                userdataid = await conn.fetchval(
                    "SELECT userdataid FROM users WHERE accountid::text = $1;", 
                    user_id
                )

                if userdataid:
                    # Deleting Production & Site Data
                    await conn.execute(
                        """
                        DELETE FROM site_production_line_orders 
                        WHERE productionlineid IN (
                            SELECT productionlineid FROM site_production_lines 
                            WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1)
                        );
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM site_production_lines 
                        WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM workforce_needs 
                        WHERE workforceid IN (
                            SELECT workforceid FROM workforces 
                            WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1)
                        );
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM workforces 
                        WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM platform_materials 
                        WHERE platformid IN (
                            SELECT platformid FROM site_platforms 
                            WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1)
                        );
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM site_platforms 
                        WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM site_experts 
                        WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute("DELETE FROM sites WHERE userid = $1;", userdataid)

                    # Deleting Storage Data
                    await conn.execute(
                        """
                        DELETE FROM storage_items 
                        WHERE storageid IN (SELECT storageid FROM storages WHERE userid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute("DELETE FROM storages WHERE userid = $1;", userdataid)
                    await conn.execute("DELETE FROM warehouses WHERE userid = $1;", userdataid)

                    # Deleting Ships & Flights
                    await conn.execute(
                        """
                        DELETE FROM ship_repair_materials 
                        WHERE shipid IN (SELECT shipid FROM ships WHERE userid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute("DELETE FROM ships WHERE userid = $1;", userdataid)
                    await conn.execute(
                        """
                        DELETE FROM ship_flight_segments 
                        WHERE flightid IN (SELECT id FROM ship_flights WHERE userid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute("DELETE FROM ship_flights WHERE userid = $1;", userdataid)

                    # Deleting Contracts
                    await conn.execute("DELETE FROM contract_loan_installments WHERE contractparty = $1;", userdataid)
                    await conn.execute("DELETE FROM contract_materials WHERE contractparty = $1;", userdataid)
                    await conn.execute("DELETE FROM contract_conditions WHERE contractparty = $1;", userdataid)
                    await conn.execute("DELETE FROM contracts WHERE partnerid = $1 OR contractparty = $1;", userdataid)

                    # Deleting Company & HQ details
                    await conn.execute(
                        """
                        DELETE FROM efficiency_gains 
                        WHERE headquartersid IN (
                            SELECT xata_id FROM headquarters 
                            WHERE companyid IN (SELECT companyid FROM company_data WHERE userdataid = $1)
                        );
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM efficiency_gains_next_level 
                        WHERE headquartersid IN (
                            SELECT xata_id FROM headquarters 
                            WHERE companyid IN (SELECT companyid FROM company_data WHERE userdataid = $1)
                        );
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM headquarters_upgrade_items 
                        WHERE headquartersid IN (
                            SELECT xata_id FROM headquarters 
                            WHERE companyid IN (SELECT companyid FROM company_data WHERE userdataid = $1)
                        );
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM headquarters 
                        WHERE companyid IN (SELECT companyid FROM company_data WHERE userdataid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM representation_contributors 
                        WHERE companyid IN (SELECT companyid FROM company_data WHERE userdataid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM representation 
                        WHERE companyid IN (SELECT companyid FROM company_data WHERE userdataid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM rating_reports 
                        WHERE companyid IN (SELECT companyid FROM company_data WHERE userdataid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute("DELETE FROM company_data WHERE userdataid = $1;", userdataid)

                    # Deleting Cash balances & User Data
                    await conn.execute("DELETE FROM user_currency_accounts_history WHERE accountid = $1;", userdataid)
                    await conn.execute("DELETE FROM user_currency_accounts WHERE userid = $1;", userdataid)
                    await conn.execute("DELETE FROM users_data WHERE userid = $1;", userdataid)

                # Reset synchronized flag and userdataid link in users table
                await conn.execute(
                    "UPDATE users SET userdataid = NULL, is_synchronized = FALSE WHERE accountid::text = $1;",
                    user_id
                )

            return JSONResponse(content={"success": True, "message": "All synced user data deleted successfully."})

    except Exception as e:
        logger.error(f"Error deleting user data: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@users_router.post("/delete_account")
async def delete_user_account(request: Request, user_id: str = Depends(get_current_user_id)):
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            async with conn.transaction():
                # 1. Fetch userdataid
                userdataid = await conn.fetchval(
                    "SELECT userdataid FROM users WHERE accountid::text = $1;", 
                    user_id
                )

                if userdataid:
                    # Deleting Production & Site Data
                    await conn.execute(
                        """
                        DELETE FROM site_production_line_orders 
                        WHERE productionlineid IN (
                            SELECT productionlineid FROM site_production_lines 
                            WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1)
                        );
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM site_production_lines 
                        WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM workforce_needs 
                        WHERE workforceid IN (
                            SELECT workforceid FROM workforces 
                            WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1)
                        );
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM workforces 
                        WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM platform_materials 
                        WHERE platformid IN (
                            SELECT platformid FROM site_platforms 
                            WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1)
                        );
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM site_platforms 
                        WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM site_experts 
                        WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute("DELETE FROM sites WHERE userid = $1;", userdataid)

                    # Deleting Storage Data
                    await conn.execute(
                        """
                        DELETE FROM storage_items 
                        WHERE storageid IN (SELECT storageid FROM storages WHERE userid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute("DELETE FROM storages WHERE userid = $1;", userdataid)
                    await conn.execute("DELETE FROM warehouses WHERE userid = $1;", userdataid)

                    # Deleting Ships & Flights
                    await conn.execute(
                        """
                        DELETE FROM ship_repair_materials 
                        WHERE shipid IN (SELECT shipid FROM ships WHERE userid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute("DELETE FROM ships WHERE userid = $1;", userdataid)
                    await conn.execute(
                        """
                        DELETE FROM ship_flight_segments 
                        WHERE flightid IN (SELECT id FROM ship_flights WHERE userid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute("DELETE FROM ship_flights WHERE userid = $1;", userdataid)

                    # Deleting Contracts
                    await conn.execute("DELETE FROM contract_loan_installments WHERE contractparty = $1;", userdataid)
                    await conn.execute("DELETE FROM contract_materials WHERE contractparty = $1;", userdataid)
                    await conn.execute("DELETE FROM contract_conditions WHERE contractparty = $1;", userdataid)
                    await conn.execute("DELETE FROM contracts WHERE partnerid = $1 OR contractparty = $1;", userdataid)

                    # Deleting Company & HQ details
                    await conn.execute(
                        """
                        DELETE FROM efficiency_gains 
                        WHERE headquartersid IN (
                            SELECT xata_id FROM headquarters 
                            WHERE companyid IN (SELECT companyid FROM company_data WHERE userdataid = $1)
                        );
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM efficiency_gains_next_level 
                        WHERE headquartersid IN (
                            SELECT xata_id FROM headquarters 
                            WHERE companyid IN (SELECT companyid FROM company_data WHERE userdataid = $1)
                        );
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM headquarters_upgrade_items 
                        WHERE headquartersid IN (
                            SELECT xata_id FROM headquarters 
                            WHERE companyid IN (SELECT companyid FROM company_data WHERE userdataid = $1)
                        );
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM headquarters 
                        WHERE companyid IN (SELECT companyid FROM company_data WHERE userdataid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM representation_contributors 
                        WHERE companyid IN (SELECT companyid FROM company_data WHERE userdataid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM representation 
                        WHERE companyid IN (SELECT companyid FROM company_data WHERE userdataid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute(
                        """
                        DELETE FROM rating_reports 
                        WHERE companyid IN (SELECT companyid FROM company_data WHERE userdataid = $1);
                        """,
                        userdataid
                    )
                    await conn.execute("DELETE FROM company_data WHERE userdataid = $1;", userdataid)

                    # Deleting Cash balances & User Data
                    await conn.execute("DELETE FROM user_currency_accounts_history WHERE accountid = $1;", userdataid)
                    await conn.execute("DELETE FROM user_currency_accounts WHERE userid = $1;", userdataid)
                    await conn.execute("DELETE FROM users_data WHERE userid = $1;", userdataid)

                # 2. Delete Settings & Tokens linked to accountid (UUID or text)
                await conn.execute("DELETE FROM user_global_settings WHERE userid = $1;", user_id)
                await conn.execute("DELETE FROM user_web_settings WHERE userid = $1;", user_id)
                await conn.execute("DELETE FROM user_tokens WHERE userid = $1;", user_id)
                await conn.execute("DELETE FROM user_api_tokens WHERE userid = $1;", user_id)
                await conn.execute("DELETE FROM user_notifications WHERE accountid = $1;", user_id)
                await conn.execute("DELETE FROM user_verification_codes WHERE userid = $1;", user_id)

                # 3. Delete from sharing groups / memberships
                await conn.execute("DELETE FROM data_group_members WHERE user_id::text = $1;", user_id)

                # 4. Finally delete the user account
                await conn.execute("DELETE FROM users WHERE accountid::text = $1;", user_id)

            return JSONResponse(content={"success": True, "message": "User account and all data deleted successfully."})

    except Exception as e:
        logger.error(f"Error deleting user account: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")