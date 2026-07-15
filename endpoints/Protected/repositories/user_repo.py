from typing import List, Optional

SQL_FETCH_COMPANY_DATA = """
WITH TargetData AS (
    SELECT 
        u.username,
        c.companyid as companyid, c.companycode as companycode, c.companyname as companyname,
        cr.id as corpid, cr.name as corpname, cr.code as corpcode,
        c.xata_updatedat
    FROM company_data c
    JOIN users u ON u.userdataid = c.userdataid
    LEFT JOIN corporation_shareholders cs ON cs.companyid = c.companyid
    LEFT JOIN corporations cr ON cr.id = cs.corporationid
    WHERE 
        -- 1. Filter by Usernames (if provided)
        ($1::text[] IS NULL OR u.username = ANY($1::text[]))
        
        -- 2. Filter by Company Codes (if provided)
        AND ($2::text[] IS NULL OR c.companycode = ANY($2::text[]))
        
        -- 3. Filter by Company Names (if provided)
        AND ($3::text[] IS NULL OR c.companyname = ANY($3::text[]))
)
SELECT 
    COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'Username', td.username,
                'Company', jsonb_build_object(
                    'CompanyId', td.companyid,
                    'CompanyCode', td.companycode,
                    'CompanyName', td.companyname,
                    -- 'CountryId', td.countryid,
                    -- 'CountryCode', td.countrycode,
                    -- 'CountryName', td.countryname,
                    'CorporationId', td.corpid,
                    'CorporationCode', td.corpcode,
                    'CorporationName', td.corpname,
                    'Timestamp', CAST(EXTRACT(EPOCH FROM td.xata_updatedat) * 1000 AS BIGINT)
                )
            )
        ),
        '[]'::jsonb
    )
FROM TargetData td;
"""

async def fetch_company_data(
    db,
    usernames: Optional[List[str]] = None,
    codes: Optional[List[str]] = None,
    names: Optional[List[str]] = None
) -> list:
    """
    Fetches company data by Usernames, Company Codes, or Company Names.
    Returns: [{ "Username": "x", "Company": {...} }]
    """
    # Convert empty lists to None for SQL handling to ensure checks are ignored if empty
    p_usernames = usernames if usernames else None
    p_codes = codes if codes else None
    p_names = names if names else None

    row = await db.fetch_one(SQL_FETCH_COMPANY_DATA, p_usernames, p_codes, p_names)
    return row[0] if row else []
