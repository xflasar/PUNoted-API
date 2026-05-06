import logging

logger = logging.getLogger(__name__)

SQL_FETCH_PUBLIC_COMPANY = """
SELECT jsonb_build_object(
    'UserId', id,
    'Username', username,
    'CompanyId', company_id,
    'CompanyName', company_name,
    'CompanyCode', company_code,
    'SubscriptionLevel', subscription_level,
    'HighestTier', highest_tier,
    'Pioneer', pioneer,
    'Moderator', moderator,
    'Team', team,
    'Translator', translator,
    'ActiveDaysPerWeek', active_days_per_week,
    'CreatedTimestamp', created_timestamp,
    'Gifts', COALESCE(gifts, '{}'::jsonb)
)::text
FROM public_users_data
WHERE UPPER(company_code) = UPPER($1)
LIMIT 1;
"""

async def get_public_company_json(db, company_code: str) -> str:
    try:
        async with db.pool.acquire() as conn:
            json_str = await conn.fetchval(SQL_FETCH_PUBLIC_COMPANY, company_code)
            return json_str
    except Exception as e:
        logger.error(f"Error fetching public company data for {company_code}: {e}", exc_info=True)
        raise e