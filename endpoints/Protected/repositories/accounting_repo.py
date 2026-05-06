from asyncpg import Connection

SQL_FETCH_ACCOUNTS_LIST = """
WITH target_users AS (
    SELECT u.username, u.userdataid
    FROM users u
    WHERE u.username = ANY($1::text[])
),
user_account_data AS (
    SELECT 
        jsonb_build_object(
            'Username', tu.username,
            'Accounts', COALESCE(jsonb_agg(
                jsonb_build_object(
                    'Currency', uca.balancecurrencycode,
                    'Balance', uca.balanceamount,
                    'BookBalance', uca.bookbalanceamount,
                    'Category', uca.category,
                    'Type', uca.type,
                    'AccountNumber', uca.number,
                    'LastUpdatedEpochMs', EXTRACT(EPOCH FROM uca.xata_updatedat) * 1000
                ) ORDER BY uca.balancecurrencycode
            ) FILTER (
                WHERE uca.balancecurrencycode IS NOT NULL
                AND ($2::text IS NULL OR uca.balancecurrencycode = $2)
            ), '[]'::jsonb)
        ) as user_data_obj
    FROM target_users tu
    LEFT JOIN user_currency_accounts uca ON uca.userid = tu.userdataid
    GROUP BY tu.username
)
SELECT COALESCE(jsonb_agg(user_data_obj), '[]'::jsonb) FROM user_account_data;
"""

async def fetch_user_accounts(conn: Connection, target_usernames: list, currency: str = None):
    """
    Fetches accounts for the provided list of users.
    Optionally filters by currency code (e.g., 'ICA').
    """
    return await conn.fetchval(SQL_FETCH_ACCOUNTS_LIST, target_usernames, currency)