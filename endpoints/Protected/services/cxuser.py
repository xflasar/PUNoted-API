from datetime import datetime
from typing import List, Optional


# --- CSV Streamer (Multi-User Support) ---
async def stream_orders_csv(
    db,
    usernames_list: List[str],
    start_date=None,
    end_date=None,
    limit=None,
    status=None,
    order_type=None,
    ticker=None
):
    async with db.pool.acquire() as conn:
        query, params = _build_multi_user_query(
            usernames_list, start_date, end_date, limit,
            status, order_type, ticker, for_csv=True
        )

        async with conn.transaction():
            async for record in conn.cursor(query, *params, prefetch=2000):
                yield list(record.values())

# --- JSON Fetcher (Grouped by User) ---
async def fetch_orders_as_json(
    db,
    usernames_list: List[str],
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    limit: Optional[int] = None,
    status: Optional[str] = None,
    order_type: Optional[str] = None,
    ticker: Optional[str] = None,
) -> str:
    async with db.pool.acquire() as conn:
        base_query, params = _build_multi_user_query(
            usernames_list, start_date, end_date, limit,
            status, order_type, ticker, for_csv=False
        )

        final_query = f"""
            WITH raw_data AS ({base_query})
            SELECT 
                COALESCE(
                    json_agg(
                        json_build_object(
                            'Username', username,
                            'Orders', orders
                        )
                    ), 
                    '[]'
                )
            FROM (
                SELECT username, json_agg(to_jsonb(raw_data) - 'username') as orders
                FROM raw_data
                GROUP BY username
            ) sub;
        """

        json_str = await conn.fetchval(final_query, *params)
        return json_str

def _build_multi_user_query(
    usernames_list,
    start_date,
    end_date,
    limit,
    status,
    order_type,
    ticker,
    for_csv=False
):

    # 1. CTE params
    params = [usernames_list]

    # 2. Build Filter Conditions
    conditions = []

    if start_date:
        params.append(start_date)
        conditions.append(f"o.created >= ${len(params)}")

    if end_date:
        params.append(end_date)
        conditions.append(f"o.created <= ${len(params)}")

    if status:
        params.append(status)
        conditions.append(f"o.status = ${len(params)}")

    if order_type:
        params.append(order_type)
        conditions.append(f"o.type = ${len(params)}")

    if ticker:
        params.append(ticker)
        conditions.append(f"m.ticker = ${len(params)}")

    # Join all conditions with AND
    where_clause = ""
    if conditions:
        where_clause = " AND " + " AND ".join(conditions)

    # 3. Limit Clause
    limit_clause = ""
    if limit:
        params.append(limit)
        limit_clause = f"LIMIT ${len(params)}"

    # 4. Column Selection
    if for_csv:
        cols = """
            tu.username,
            o.orderid::text, 
            to_char(o.created, 'YYYY-MM-DD"T"HH24:MI:SS') as date, 
            m.ticker, o.type, o.status, 
            o.limitamount::text as price, o.limitcurrency as currency,
            SUM(t.amount)::text as filled_amount,
            (SUM(t.amount) * o.limitamount)::text as total_value
        """
    else:
        cols = """
            tu.username,
            o.orderid as "order_id", 
            extract(epoch from o.created) * 1000 as "date_epoch_ms",
            m.ticker, o.type, o.status, 
            o.limitamount as "price", o.limitcurrency as "currency",
            SUM(t.amount) as "filled_amount",
            (SUM(t.amount) * o.limitamount) as "total_value"
        """

    # 5. Full Query
    query = f"""
        WITH target_users AS (
            SELECT u.username, u.userdataid
            FROM users u
            WHERE u.username = ANY($1::text[])
        )
        SELECT {cols}
        FROM comex_trade_orders o
        INNER JOIN target_users tu ON o.userid = tu.userdataid
        INNER JOIN comex_trade_orders_trades t ON o.orderid = t.orderId
        JOIN materials m ON o.materialid = m.materialid
        WHERE 1=1 {where_clause}
        GROUP BY tu.username, o.orderid, o.created, m.ticker, o.type, o.status, o.limitamount, o.limitcurrency
        ORDER BY o.created DESC
        {limit_clause}
    """

    return query, params
