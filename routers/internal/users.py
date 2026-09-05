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


@users_router.get("/data-summary")
async def get_user_data_summary(request: Request, user_id: str = Depends(get_current_user_id)):
    """
    Returns wide database breakdown of all user data locations separated into:
      1. In-Game Telemetry & Operations Data (linked to users_data.userid)
      2. Web Account & System Credentials Data (linked to users.accountid)
    """
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            user_row = await conn.fetchrow("SELECT userdataid FROM users WHERE accountid::text = $1;", user_id)
            userdata_id = user_row["userdataid"] if user_row and user_row["userdataid"] else user_id

            # --- 1. IN-GAME TELEMETRY & OPERATIONS DATA (users_data.userid) ---
            # Blueprints
            bp_count = await conn.fetchval("SELECT COUNT(*) FROM ship_blueprints WHERE user_id = $1 OR user_id = $2;", user_id, userdata_id) or 0
            bom_count = await conn.fetchval("SELECT COUNT(*) FROM ship_blueprint_bill_of_materials WHERE blueprintid IN (SELECT id FROM ship_blueprints WHERE user_id = $1 OR user_id = $2);", user_id, userdata_id) or 0
            comp_count = await conn.fetchval("SELECT COUNT(*) FROM ship_blueprint_components WHERE blueprintid IN (SELECT id FROM ship_blueprints WHERE user_id = $1 OR user_id = $2);", user_id, userdata_id) or 0

            # Ships
            ships_count = await conn.fetchval("SELECT COUNT(*) FROM ships WHERE userid = $1 OR userid = $2;", user_id, userdata_id) or 0
            flights_count = await conn.fetchval("SELECT COUNT(*) FROM ship_flights WHERE userid = $1 OR userid = $2 OR shipid IN (SELECT shipid FROM ships WHERE userid = $1 OR userid = $2);", user_id, userdata_id) or 0
            repairs_count = await conn.fetchval("SELECT COUNT(*) FROM ship_repair_materials WHERE shipid IN (SELECT shipid FROM ships WHERE userid = $1 OR userid = $2);", user_id, userdata_id) or 0

            # Storages
            storages_count = await conn.fetchval("SELECT COUNT(*) FROM storages WHERE userid = $1 OR userid = $2;", user_id, userdata_id) or 0
            storage_items_count = await conn.fetchval("SELECT COUNT(*) FROM storage_items WHERE storageid IN (SELECT storageid FROM storages WHERE userid = $1 OR userid = $2);", user_id, userdata_id) or 0

            # Sites
            sites_count = await conn.fetchval("SELECT COUNT(*) FROM sites WHERE userid = $1 OR userid = $2;", user_id, userdata_id) or 0
            platforms_count = await conn.fetchval("SELECT COUNT(*) FROM site_platforms WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1 OR userid = $2);", user_id, userdata_id) or 0
            workforce_count = await conn.fetchval("SELECT COUNT(*) FROM workforces WHERE userid = $1 OR userid = $2 OR siteid IN (SELECT siteid FROM sites WHERE userid = $1 OR userid = $2);", user_id, userdata_id) or 0

            # Production Lines
            prod_lines_count = await conn.fetchval("SELECT COUNT(*) FROM site_production_lines WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1 OR userid = $2);", user_id, userdata_id) or 0
            prod_orders_count = await conn.fetchval("SELECT COUNT(*) FROM site_production_line_orders WHERE productionlineid IN (SELECT productionlineid FROM site_production_lines WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1 OR userid = $2));", user_id, userdata_id) or 0

            # Contracts
            contracts_count = await conn.fetchval("SELECT COUNT(*) FROM contracts WHERE userid = $1 OR userid = $2 OR party = $1 OR party = $2 OR partnerid = $1 OR partnerid = $2;", user_id, userdata_id) or 0
            conditions_count = await conn.fetchval("SELECT COUNT(*) FROM contract_conditions WHERE contractid IN (SELECT id FROM contracts WHERE userid = $1 OR userid = $2 OR party = $1 OR party = $2 OR partnerid = $1 OR partnerid = $2);", user_id, userdata_id) or 0

            # Trade Orders & Company
            comex_count = await conn.fetchval("SELECT COUNT(*) FROM comex_trade_orders WHERE userid = $1 OR userid = $2;", user_id, userdata_id) or 0
            company_count = await conn.fetchval("SELECT COUNT(*) FROM company_data WHERE userdataid = $1;", userdata_id) or 0
            users_data_count = await conn.fetchval("SELECT COUNT(*) FROM users_data WHERE userid = $1;", userdata_id) or 0

            # Financials
            finances_count = await conn.fetchval("SELECT COUNT(*) FROM user_currency_accounts WHERE userid = $1 OR userid = $2;", user_id, userdata_id) or 0
            fin_history_count = await conn.fetchval("SELECT COUNT(*) FROM user_currency_accounts_history WHERE userid = $1 OR userid = $2;", user_id, userdata_id) or 0

            # --- 2. WEB ACCOUNT & CREDENTIALS DATA (users.accountid) ---
            user_base_count = await conn.fetchval("SELECT COUNT(*) FROM users WHERE accountid::text = $1;", user_id) or 0
            api_tokens_count = await conn.fetchval("SELECT COUNT(*) FROM user_api_tokens WHERE user_id::text = $1;", user_id) or 0
            auth_tokens_count = await conn.fetchval("SELECT COUNT(*) FROM user_tokens WHERE userid = $1;", user_id) or 0
            global_settings_count = await conn.fetchval("SELECT COUNT(*) FROM user_global_settings WHERE userid = $1;", user_id) or 0
            web_settings_count = await conn.fetchval("SELECT COUNT(*) FROM user_web_settings WHERE user_id::text = $1;", user_id) or 0
            data_groups_count = await conn.fetchval("SELECT COUNT(*) FROM data_group_members WHERE user_id::text = $1;", user_id) or 0
            notifications_count = await conn.fetchval("SELECT COUNT(*) FROM user_notifications WHERE accountid::text = $1;", user_id) or 0

            return {
                "success": True,
                "sections": [
                    {
                        "sectionId": "ingame",
                        "sectionTitle": "In-Game Telemetry & Operations Data",
                        "sectionBadge": "Linked to users_data.userid (FIO Game ID)",
                        "description": "Telemetry, assets, production queues, and market trade logs synced from Apex/FIO.",
                        "categories": [
                            {
                                "id": "company_profile",
                                "label": "Company & Corporation Profile",
                                "count": company_count + users_data_count,
                                "description": "FIO Company profile, code, and user data metadata",
                                "subTables": [
                                    { "label": "Company Data", "count": company_count },
                                    { "label": "User Data Profile", "count": users_data_count },
                                ]
                            },
                            {
                                "id": "blueprints",
                                "label": "Ship Blueprints",
                                "count": bp_count + bom_count + comp_count,
                                "description": "Custom ship designs & component specs",
                                "subTables": [
                                    { "label": "Blueprint Headers", "count": bp_count },
                                    { "label": "Bill of Materials", "count": bom_count },
                                    { "label": "Component Selections", "count": comp_count },
                                ]
                            },
                            {
                                "id": "ships",
                                "label": "Fleets & Ships",
                                "count": ships_count + flights_count + repairs_count,
                                "description": "Owned active ships, flight logs & repair materials",
                                "subTables": [
                                    { "label": "Active Ships", "count": ships_count },
                                    { "label": "Flight Logs", "count": flights_count },
                                    { "label": "Repair Materials", "count": repairs_count },
                                ]
                            },
                            {
                                "id": "storages",
                                "label": "Storages & Inventories",
                                "count": storages_count + storage_items_count,
                                "description": "Station & site warehouses & inventory items",
                                "subTables": [
                                    { "label": "Storage Warehouses", "count": storages_count },
                                    { "label": "Stored Inventory Items", "count": storage_items_count },
                                ]
                            },
                            {
                                "id": "sites",
                                "label": "Production Sites",
                                "count": sites_count + platforms_count + workforce_count,
                                "description": "Base buildings, platforms & site workforces",
                                "subTables": [
                                    { "label": "Production Sites", "count": sites_count },
                                    { "label": "Building Platforms", "count": platforms_count },
                                    { "label": "Workforce Populations", "count": workforce_count },
                                ]
                            },
                            {
                                "id": "production",
                                "label": "Production Lines",
                                "count": prod_lines_count + prod_orders_count,
                                "description": "Active production lines & recipe queue orders",
                                "subTables": [
                                    { "label": "Production Lines", "count": prod_lines_count },
                                    { "label": "Recipe Queue Orders", "count": prod_orders_count },
                                ]
                            },
                            {
                                "id": "contracts",
                                "label": "Contracts & Conditions",
                                "count": contracts_count + conditions_count,
                                "description": "Player contracts & contract condition terms",
                                "subTables": [
                                    { "label": "Active Contracts", "count": contracts_count },
                                    { "label": "Contract Conditions", "count": conditions_count },
                                ]
                            },
                            {
                                "id": "comex_orders",
                                "label": "Exchange Trade Orders",
                                "count": comex_count,
                                "description": "CX exchange market orders and trade logs",
                                "subTables": [
                                    { "label": "Comex Trade Orders", "count": comex_count },
                                ]
                            },
                            {
                                "id": "finances",
                                "label": "Financial Balances",
                                "count": finances_count + fin_history_count,
                                "description": "Accounting accounts & balance history snapshots",
                                "subTables": [
                                    { "label": "Currency Accounts", "count": finances_count },
                                    { "label": "Balance History Snapshots", "count": fin_history_count },
                                ]
                            },
                        ]
                    },
                    {
                        "sectionId": "account",
                        "sectionTitle": "Web Account & System Credentials",
                        "sectionBadge": "Linked to users.accountid (Auth User UUID)",
                        "description": "Authentication credentials, API tokens, web preferences, data group memberships, and system settings.",
                        "categories": [
                            {
                                "id": "account_identity",
                                "label": "Account Core Credentials",
                                "count": user_base_count,
                                "description": "Base user account login record and password hash",
                                "subTables": [
                                    { "label": "Users Base Account", "count": user_base_count },
                                ]
                            },
                            {
                                "id": "auth_tokens",
                                "label": "Active Web Sessions & API Tokens",
                                "count": auth_tokens_count + api_tokens_count,
                                "description": "Browser session refresh tokens and developer API keys",
                                "subTables": [
                                    { "label": "Auth Refresh Tokens", "count": auth_tokens_count },
                                    { "label": "Developer API Tokens", "count": api_tokens_count },
                                ]
                            },
                            {
                                "id": "user_settings",
                                "label": "Web Settings & Preferences",
                                "count": global_settings_count + web_settings_count,
                                "description": "Global game configuration and page layout preferences",
                                "subTables": [
                                    { "label": "Global Game Settings", "count": global_settings_count },
                                    { "label": "Page Layout Preferences", "count": web_settings_count },
                                ]
                            },
                            {
                                "id": "group_memberships",
                                "label": "Data Sharing Groups",
                                "count": data_groups_count,
                                "description": "Shared corp/group access memberships",
                                "subTables": [
                                    { "label": "Group Memberships", "count": data_groups_count },
                                ]
                            },
                            {
                                "id": "user_notifications",
                                "label": "System Notifications",
                                "count": notifications_count,
                                "description": "In-app notifications and system alerts",
                                "subTables": [
                                    { "label": "Notification Messages", "count": notifications_count },
                                ]
                            },
                        ]
                    }
                ]
            }
    except Exception as e:
        logger.error(f"Error fetching user data summary for {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to fetch data summary")


@users_router.delete("/delete-data")
async def delete_user_data_category(
    request: Request,
    category: str,
    user_id: str = Depends(get_current_user_id)
):
    """
    Deletes specific categories of in-game user data, account data, all in-game data, or whole account.
    """
    try:
        pool = request.app.state.db.pool
        async with pool.acquire() as conn:
            user_row = await conn.fetchrow("SELECT userdataid FROM users WHERE accountid::text = $1;", user_id)
            userdata_id = user_row["userdataid"] if user_row and user_row["userdataid"] else user_id

            if category == "company_profile":
                if userdata_id:
                    await conn.execute("DELETE FROM company_data WHERE userdataid = $1;", userdata_id)
                    await conn.execute("DELETE FROM users_data WHERE userid = $1;", userdata_id)
                msg = "Successfully deleted company profile and user data."

            elif category == "blueprints":
                await conn.execute("DELETE FROM ship_blueprint_bill_of_materials WHERE blueprintid IN (SELECT id FROM ship_blueprints WHERE user_id = $1 OR user_id = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM ship_blueprint_components WHERE blueprintid IN (SELECT id FROM ship_blueprints WHERE user_id = $1 OR user_id = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM ship_blueprints WHERE user_id = $1 OR user_id = $2;", user_id, userdata_id)
                msg = "Successfully deleted all ship blueprints data and component specs."

            elif category == "ships":
                await conn.execute("DELETE FROM ship_repair_materials WHERE shipid IN (SELECT shipid FROM ships WHERE userid = $1 OR userid = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM ship_flights WHERE userid = $1 OR userid = $2 OR shipid IN (SELECT shipid FROM ships WHERE userid = $1 OR userid = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM ships WHERE userid = $1 OR userid = $2;", user_id, userdata_id)
                msg = "Successfully deleted all fleet, ship flight logs, and repair data."

            elif category == "storages":
                await conn.execute("DELETE FROM storage_items WHERE storageid IN (SELECT storageid FROM storages WHERE userid = $1 OR userid = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM storages WHERE userid = $1 OR userid = $2;", user_id, userdata_id)
                msg = "Successfully deleted all storage inventory and stored items."

            elif category == "sites":
                await conn.execute("DELETE FROM site_production_line_orders WHERE productionlineid IN (SELECT productionlineid FROM site_production_lines WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1 OR userid = $2));", user_id, userdata_id)
                await conn.execute("DELETE FROM site_production_lines WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1 OR userid = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM site_platforms WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1 OR userid = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM workforces WHERE userid = $1 OR userid = $2 OR siteid IN (SELECT siteid FROM sites WHERE userid = $1 OR userid = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM sites WHERE userid = $1 OR userid = $2;", user_id, userdata_id)
                msg = "Successfully deleted all production sites, platforms, workforces, and line orders."

            elif category == "production":
                await conn.execute("DELETE FROM site_production_line_orders WHERE productionlineid IN (SELECT productionlineid FROM site_production_lines WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1 OR userid = $2));", user_id, userdata_id)
                await conn.execute("DELETE FROM site_production_lines WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1 OR userid = $2);", user_id, userdata_id)
                msg = "Successfully deleted all production lines and active queue orders."

            elif category == "contracts":
                await conn.execute("DELETE FROM contract_conditions WHERE contractid IN (SELECT id FROM contracts WHERE userid = $1 OR userid = $2 OR party = $1 OR party = $2 OR partnerid = $1 OR partnerid = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM contracts WHERE userid = $1 OR userid = $2 OR party = $1 OR party = $2 OR partnerid = $1 OR partnerid = $2;", user_id, userdata_id)
                msg = "Successfully deleted all contracts and contract condition terms."

            elif category == "comex_orders":
                await conn.execute("DELETE FROM comex_trade_orders WHERE userid = $1 OR userid = $2;", user_id, userdata_id)
                msg = "Successfully deleted all exchange trade orders."

            elif category == "finances":
                await conn.execute("DELETE FROM user_currency_accounts_history WHERE userid = $1 OR userid = $2;", user_id, userdata_id)
                await conn.execute("DELETE FROM user_currency_accounts WHERE userid = $1 OR userid = $2;", user_id, userdata_id)
                msg = "Successfully deleted all financial balance accounts and history snapshots."

            elif category == "auth_tokens":
                await conn.execute("DELETE FROM user_tokens WHERE userid = $1;", user_id)
                await conn.execute("DELETE FROM user_api_tokens WHERE user_id::text = $1;", user_id)
                msg = "Successfully deleted all web session tokens and API keys."

            elif category == "user_settings":
                await conn.execute("DELETE FROM user_global_settings WHERE userid = $1;", user_id)
                await conn.execute("DELETE FROM user_web_settings WHERE user_id::text = $1;", user_id)
                msg = "Successfully reset all global and web layout settings."

            elif category == "group_memberships":
                await conn.execute("DELETE FROM data_group_members WHERE user_id::text = $1;", user_id)
                msg = "Successfully removed data group memberships."

            elif category == "user_notifications":
                await conn.execute("DELETE FROM user_notifications WHERE accountid::text = $1;", user_id)
                msg = "Successfully cleared system notifications."

            elif category == "all_ingame":
                # Purge all synced telemetry data across all main & sub tables
                if userdata_id:
                    await conn.execute("DELETE FROM company_data WHERE userdataid = $1;", userdata_id)
                    await conn.execute("DELETE FROM users_data WHERE userid = $1;", userdata_id)

                await conn.execute("DELETE FROM comex_trade_orders WHERE userid = $1 OR userid = $2;", user_id, userdata_id)

                await conn.execute("DELETE FROM ship_blueprint_bill_of_materials WHERE blueprintid IN (SELECT id FROM ship_blueprints WHERE user_id = $1 OR user_id = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM ship_blueprint_components WHERE blueprintid IN (SELECT id FROM ship_blueprints WHERE user_id = $1 OR user_id = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM ship_blueprints WHERE user_id = $1 OR user_id = $2;", user_id, userdata_id)

                await conn.execute("DELETE FROM ship_repair_materials WHERE shipid IN (SELECT shipid FROM ships WHERE userid = $1 OR userid = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM ship_flights WHERE userid = $1 OR userid = $2 OR shipid IN (SELECT shipid FROM ships WHERE userid = $1 OR userid = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM ships WHERE userid = $1 OR userid = $2;", user_id, userdata_id)

                await conn.execute("DELETE FROM storage_items WHERE storageid IN (SELECT storageid FROM storages WHERE userid = $1 OR userid = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM storages WHERE userid = $1 OR userid = $2;", user_id, userdata_id)

                await conn.execute("DELETE FROM site_production_line_orders WHERE productionlineid IN (SELECT productionlineid FROM site_production_lines WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1 OR userid = $2));", user_id, userdata_id)
                await conn.execute("DELETE FROM site_production_lines WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1 OR userid = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM site_platforms WHERE siteid IN (SELECT siteid FROM sites WHERE userid = $1 OR userid = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM workforces WHERE userid = $1 OR userid = $2 OR siteid IN (SELECT siteid FROM sites WHERE userid = $1 OR userid = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM sites WHERE userid = $1 OR userid = $2;", user_id, userdata_id)

                await conn.execute("DELETE FROM contract_conditions WHERE contractid IN (SELECT id FROM contracts WHERE userid = $1 OR userid = $2 OR party = $1 OR party = $2 OR partnerid = $1 OR partnerid = $2);", user_id, userdata_id)
                await conn.execute("DELETE FROM contracts WHERE userid = $1 OR userid = $2 OR party = $1 OR party = $2 OR partnerid = $1 OR partnerid = $2;", user_id, userdata_id)

                await conn.execute("DELETE FROM user_currency_accounts_history WHERE userid = $1 OR userid = $2;", user_id, userdata_id)
                await conn.execute("DELETE FROM user_currency_accounts WHERE userid = $1 OR userid = $2;", user_id, userdata_id)
                msg = "Successfully purged all synced in-game telemetry data across all tables."

            elif category == "whole_account" or category == "account_identity":
                if userdata_id:
                    await conn.execute("DELETE FROM company_data WHERE userdataid = $1;", userdata_id)
                    await conn.execute("DELETE FROM users_data WHERE userid = $1;", userdata_id)
                await conn.execute("DELETE FROM user_tokens WHERE userid = $1;", user_id)
                await conn.execute("DELETE FROM user_api_tokens WHERE user_id::text = $1;", user_id)
                await conn.execute("DELETE FROM user_global_settings WHERE userid = $1;", user_id)
                await conn.execute("DELETE FROM user_web_settings WHERE user_id::text = $1;", user_id)
                await conn.execute("DELETE FROM data_group_members WHERE user_id::text = $1;", user_id)
                await conn.execute("DELETE FROM user_notifications WHERE accountid::text = $1;", user_id)
                await conn.execute("DELETE FROM users WHERE accountid::text = $1;", user_id)
                msg = "Entire user account and data purged successfully."

            else:
                raise HTTPException(status_code=400, detail="Invalid deletion category specified.")

            return JSONResponse(content={"success": True, "message": msg})
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete category '{category}' for user {user_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Data deletion failed")