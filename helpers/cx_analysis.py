from datetime import datetime, timezone
from typing import Any, Dict, List


from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

async def get_cx_dashboard_data(db, user_id: str, start_date: Optional[datetime], end_date: Optional[datetime], exchange_filter: str = "IC1") -> Dict[str, Any]:
    async with db.pool.acquire() as conn:

        if not exchange_filter: 
            exchange_filter = await conn.fetchval("SELECT cxcode FROM user_global_settings WHERE userid = $1", user_id)
        # 1. Resolve Internal ID (Moved up so we can use it for Date Lookup)
        internal_id = await conn.fetchval("SELECT userdataid FROM users WHERE accountid = $1", user_id)
        if not internal_id:
            internal_id = user_id

        # 2. Dynamic Date Resolution (For "ALL" Range)
        if start_date is None:
            # Query the absolute beginning of the user's history
            first_activity = await conn.fetchval("SELECT MIN(cto.created) FROM comex_trade_orders cto LEFT JOIN commodity_exchanges ce ON ce.id = cto.exchangeid WHERE userid = $1 AND ce.code = $2", internal_id, exchange_filter)
            
            if first_activity:
                # Add a small buffer (e.g., 1 hour) before the first trade to make charts look nice
                start_date = first_activity - timedelta(hours=1)
            else:
                # Fallback: If user has NO data, default to 24 hours ago to prevent crashes
                start_date = datetime.utcnow() - timedelta(hours=24)
        
        if end_date is None:
            end_date = datetime.utcnow()

        # 3. Determine Granularity (Now that dates are guaranteed to be set)
        time_diff = end_date - start_date
        
        if time_diff.total_seconds() < 172800:  # < 48 hours -> Hourly
            trunc_type = "hour"
            interval_str = "1 hour"
            chart_start = start_date
            chart_end = end_date
        else:  # >= 48 hours -> Daily
            trunc_type = "day"
            interval_str = "1 day"
            # Force chart boundaries to midnight for clean daily grid
            chart_start = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            chart_end = end_date.replace(hour=0, minute=0, second=0, microsecond=0)

        # --- A. KPI STATS ---
        kpi_query = f"""
            WITH trade_data AS (
                SELECT o.type, m.ticker, t.amount, (t.amount * t.priceAmount) as value
                FROM comex_trade_orders_trades t
                JOIN comex_trade_orders o ON t.orderId = o.orderid
                JOIN materials m ON o.materialid = m.materialid
                JOIN commodity_exchanges ce ON o.exchangeid = ce.id
                WHERE o.userid = $1 
                  AND ce.code = $4
                  AND t.tradetime >= $2 
                  AND t.tradetime < ($3::timestamp + '{interval_str}'::interval)
            )
            SELECT 
                COALESCE(SUM(CASE WHEN type = 'SELLING' THEN value ELSE 0 END), 0) as revenue,
                COALESCE(SUM(CASE WHEN type = 'BUYING' THEN value ELSE 0 END), 0) as expenses,
                COALESCE(SUM(CASE WHEN type = 'SELLING' THEN amount ELSE 0 END), 0) as vol_sold,
                COALESCE(SUM(CASE WHEN type = 'BUYING' THEN amount ELSE 0 END), 0) as vol_bought,
                COUNT(*) as total_trades
            FROM trade_data;
        """
        kpi_stats = await conn.fetchrow(kpi_query, internal_id, start_date, end_date, exchange_filter)

        # --- B. RAW TRADES ---
        trades_query = f"""
            SELECT t.tradetime as time, m.ticker, o.type, t.amount, (t.amount * t.priceAmount) as value
            FROM comex_trade_orders_trades t
            JOIN comex_trade_orders o ON t.orderId = o.orderid
            JOIN materials m ON o.materialid = m.materialid
            JOIN commodity_exchanges ce ON o.exchangeid = ce.id
            WHERE o.userid = $1 
              AND ce.code = $4
              AND t.tradetime >= $2 
              AND t.tradetime < ($3::timestamp + '{interval_str}'::interval)
            ORDER BY t.tradetime ASC
        """
        raw_trades = await conn.fetch(trades_query, internal_id, start_date, end_date, exchange_filter)

        # --- C. CHART DATA ---
        chart_query = f"""
            WITH time_series AS (
                SELECT generate_series(
                    $4::timestamp, 
                    $5::timestamp, 
                    '{interval_str}'::interval
                ) as ts
            ),
            grouped_data AS (
                SELECT 
                    DATE_TRUNC('{trunc_type}', t.tradetime) as time_point,
                    SUM(CASE WHEN o.type = 'SELLING' THEN (t.amount * t.priceAmount) ELSE 0 END) as revenue,
                    SUM(CASE WHEN o.type = 'BUYING' THEN (t.amount * t.priceAmount) ELSE 0 END) as expenses,
                    SUM(t.amount) as volume
                FROM comex_trade_orders_trades t
                JOIN comex_trade_orders o ON t.orderId = o.orderid
                JOIN commodity_exchanges ce ON o.exchangeid = ce.id
                WHERE o.userid = $1 
                  AND ce.code = $6
                  AND t.tradetime >= $2 
                  AND t.tradetime < ($3::timestamp + '{interval_str}'::interval)
                GROUP BY 1
            )
            SELECT 
                ts as time_point,
                COALESCE(gd.revenue, 0) as revenue,
                COALESCE(gd.expenses, 0) as expenses,
                COALESCE(gd.volume, 0) as volume
            FROM time_series
            LEFT JOIN grouped_data gd ON time_series.ts = gd.time_point
            ORDER BY ts ASC
        """
        chart_rows = await conn.fetch(chart_query, internal_id, start_date, end_date, chart_start, chart_end, exchange_filter)

        # --- D. BEST PERFORMERS ---
        best_sell_query = f"""
            SELECT m.ticker FROM comex_trade_orders_trades t
            JOIN comex_trade_orders o ON t.orderId = o.orderid
            JOIN materials m ON o.materialid = m.materialid
            JOIN commodity_exchanges ce ON o.exchangeid = ce.id
            WHERE o.userid = $1 AND o.type = 'SELLING'
              AND t.tradetime >= $2 AND t.tradetime < ($3::timestamp + '{interval_str}'::interval) AND ce.code = $4
            GROUP BY m.ticker ORDER BY SUM(t.amount * t.priceAmount) DESC LIMIT 1
        """
        best_buy_query = f"""
            SELECT m.ticker FROM comex_trade_orders_trades t
            JOIN comex_trade_orders o ON t.orderId = o.orderid
            JOIN materials m ON o.materialid = m.materialid
            JOIN commodity_exchanges ce ON o.exchangeid = ce.id
            WHERE o.userid = $1 AND o.type = 'BUYING' 
              AND t.tradetime >= $2 AND t.tradetime < ($3::timestamp + '{interval_str}'::interval) AND ce.code = $4
            GROUP BY m.ticker ORDER BY SUM(t.amount) DESC LIMIT 1
        """
        best_seller = await conn.fetchval(best_sell_query, internal_id, start_date, end_date, exchange_filter)
        most_bought = await conn.fetchval(best_buy_query, internal_id, start_date, end_date, exchange_filter)

        # --- E. ACTIVE ORDERS ---
        active_orders_query = """
            SELECT o.orderid, m.ticker, o.type, o.initialamount as initial_amount, o.limitamount as price, 
            o.limitcurrency as currency, o.status, o.created, COALESCE(SUM(t.amount), 0) as filled_amount
            FROM comex_trade_orders o
            LEFT JOIN comex_trade_orders_trades t ON o.orderid = t.orderId
            LEFT JOIN materials m ON o.materialid = m.materialid
            LEFT JOIN commodity_exchanges ce ON o.exchangeid = ce.id
            WHERE o.userid = $1 AND (o.status = 'OPEN' OR o.status = 'PARTIALLY_FILLED' OR o.status = 'PLACED') AND ce.code = $2
            GROUP BY o.orderid, m.ticker, o.type, o.initialamount, o.limitamount, o.limitcurrency, o.status, o.created
            ORDER BY o.created DESC
        """
        active_orders = await conn.fetch(active_orders_query, internal_id, exchange_filter)

        # --- F. USER HISTORY ---
        user_history_query = f"""
            SELECT 
                o.orderid, m.ticker, o.type, o.initialamount as initial_amount, o.limitamount as price, 
                o.limitcurrency as currency, o.status, o.created as date, 
                COALESCE(SUM(t.amount), 0) as filled_amount,
                (COALESCE(SUM(t.amount), 0) * o.limitamount) as total_value
            FROM comex_trade_orders o
            LEFT JOIN comex_trade_orders_trades t ON o.orderid = t.orderId
            LEFT JOIN materials m ON o.materialid = m.materialid
            LEFT JOIN commodity_exchanges ce ON o.exchangeid = ce.id
            WHERE o.userid = $1 
              AND ce.code = $4
              AND (o.status = 'FILLED' OR o.status = 'CANCELLED')
              AND o.created >= $2 
              AND o.created < ($3::timestamp + '{interval_str}'::interval)
            GROUP BY o.orderid, m.ticker, o.type, o.initialamount, o.limitamount, o.limitcurrency, o.status, o.created
            HAVING COALESCE(SUM(t.amount), 0) > 0 
            ORDER BY o.created DESC
        """
        user_history = await conn.fetch(user_history_query, internal_id, start_date, end_date, exchange_filter)

    return {
        "kpi": {
            "revenue": kpi_stats["revenue"],
            "expenses": kpi_stats["expenses"],
            "profit": kpi_stats["revenue"] - kpi_stats["expenses"],
            "volumeSold": kpi_stats["vol_sold"],
            "volumeBought": kpi_stats["vol_bought"],
            "totalTrades": kpi_stats["total_trades"],
            "bestSellingItem": best_seller or "N/A",
            "mostBoughtItem": most_bought or "N/A",
        },
        "chartData": [
            {
                "time": r["time_point"].replace(tzinfo=timezone.utc).isoformat(),
                "revenue": r["revenue"],
                "expenses": r["expenses"],
                "volume": r["volume"],
            }
            for r in chart_rows
        ],
        "activeOrders": [
            {
                **dict(row),
                "created": row["created"].replace(tzinfo=timezone.utc).isoformat() if row["created"] else None,
                "fill_percent": (row["filled_amount"] / row["initial_amount"] * 100) if row["initial_amount"] > 0 else 0,
                "total_value": row["filled_amount"] * row["price"],
            }
            for row in active_orders
        ],
        "userHistory": [
            {
                "ticker": row["ticker"],
                "type": row["type"],
                "amount": row["filled_amount"],
                "value": row["total_value"],
                "price": row["price"],
                "status": row["status"],
                "date": row["date"].replace(tzinfo=timezone.utc).isoformat() if row["date"] else None,
            }
            for row in user_history
        ],
        "trades": [
            {
                "time": r["time"].replace(tzinfo=timezone.utc).isoformat(),
                "ticker": r["ticker"],
                "type": r["type"],
                "amount": r["amount"],
                "value": r["value"],
            }
            for r in raw_trades
        ],
        "lastUpdated": datetime.now(timezone.utc).isoformat(),
    }


async def get_storage_valuation(db, account_id: str, exchange_ticker: str = "IC1", storageid: str = None) -> Dict[str, Any]:
    """
    Retrieves storage items and their current market valuation on a specific CX.
    Returns a list of available storages and the valuation for the selected storage.
    """
    
    # Map Exchange Ticker to System Name for smart defaulting
    EXCHANGE_SYSTEM_MAP = {
        "IC1": "Hortus",
        "AI1": "Antares I",
        "CI1": "Benten",
        "CI2": "Arclight",
        "NC1": "Moria",
        "NC2": "Hubur",
    }
    target_system = EXCHANGE_SYSTEM_MAP.get(exchange_ticker, "Hortus")
    ticker_pattern = f"%.{exchange_ticker}"

    try:
        async with db.pool.acquire() as conn:
            # 1. Fetch ALL storages for the user to return in the response
            # We get ID, Station Name, and System Name to help with defaulting
            storages_query = """
                SELECT 
                    s.storageid, 
                    st.name as station_name,
                    sys.name as system_name
                FROM storages s
                INNER JOIN warehouses w ON s.addressableid = w.warehouseid
                INNER JOIN stations st ON w.warehouseid = st.warehouseid
                INNER JOIN systems sys ON w.addresssystem = sys.systemid
                INNER JOIN users_data ud ON s.userid = ud.userid
                INNER JOIN users u ON u.userdataid = ud.userid
                WHERE u.accountid = $1
            """
            storage_rows = await conn.fetch(storages_query, account_id)
            
            available_storages = [
                {"storageid": str(row["storageid"]), "storagelocation": row["station_name"], "system": row["system_name"]} 
                for row in storage_rows
            ]

            # 2. Determine which storage to use for Valuation
            target_storage_id = None
            target_storage_name = f"Storage ({exchange_ticker})" # Default name

            if storageid:
                # Case A: User specified a storage ID
                target_storage_id = storageid
                # Find name for display
                for s in available_storages:
                    if s["storageid"] == storageid:
                        target_storage_name = s["storagelocation"]
                        break
            else:
                # Case B: Default (No ID provided) -> "Use exchange_ticker" logic
                # We try to find a storage in the system matching the exchange (e.g. Hortus for IC1)
                for s in available_storages:
                    if s["system"] == target_system:
                        target_storage_id = s["storageid"]
                        target_storage_name = s["storagelocation"]
                        break
                
                # Fallback: If no storage in that system, just take the first one found
                if not target_storage_id and available_storages:
                    target_storage_id = available_storages[0]["storageid"]
                    target_storage_name = available_storages[0]["storagelocation"]

            # 3. Fetch Valuation Items for the Target Storage
            results = []
            if target_storage_id:
                items_query = """
                    SELECT
                        mt.ticker,
                        mt.materialid,
                        si.quantity,
                        cxb.askprice,
                        cxb.bidprice,
                        mp.price AS corpprice
                    FROM cx_brokers AS cxb
                    INNER JOIN materials AS mt ON mt.materialid = cxb.materialid
                    INNER JOIN material_prices AS mp ON mp.ticker = mt.ticker
                    -- Join directly to the specific storage ID we resolved
                    INNER JOIN storage_items AS si ON si.materialid = mt.materialid
                    WHERE 
                        cxb.ticker LIKE $1
                        AND si.storageid = $2
                        AND si.quantity > 0
                    ORDER BY cxb.ticker
                """
                rows = await conn.fetch(items_query, ticker_pattern, int(target_storage_id) if str(target_storage_id).isdigit() else target_storage_id)

                for row in rows:
                    q = float(row["quantity"])
                    ask = float(row["askprice"] or 0)
                    corp = float(row["corpprice"] or 0)
                    results.append({
                        "ticker": row["ticker"],
                        "materialId": row["materialid"],
                        "amount": q,
                        "marketAsk": ask,
                        "marketBid": float(row["bidprice"] or 0),
                        "corporatePrice": corp,
                        "totalValueCorp": corp * q,
                        "totalValue": (ask if ask > 0 else corp) * q,
                    })

        # 4. Construct Final Response
        # Filter the storages list to only include essential fields for frontend
        response_storages = [{"storageid": s["storageid"], "storagelocation": s["storagelocation"]} for s in available_storages]

        return {
            "status": "success",
            "data": {
                "storageName": target_storage_name,
                "storages": response_storages, # List of all available storages
                "items": results,
            },
        }

    except Exception as e:
        print(f"Error fetching storage valuation: {e}")
        return {"status": "error", "message": str(e)}


async def get_bulk_prices(db, tickers: List[str], exchange_code: str = "IC1") -> Dict[str, Any]:
    """
    Fetches Ask/Bid prices for a specific list of tickers on an exchange.
    """
    if not tickers:
        return {"status": "success", "data": []}

    # Format tickers for LIKE query if needed, or exact match if tickers are full "MAT.IC1"
    # Assuming frontend sends just "DW", "RAT", we append the exchange code.
    formatted_tickers = [f"{t}.{exchange_code}" for t in tickers]

    query = """
        SELECT 
            ticker, 
            askprice, 
            bidprice 
        FROM cx_brokers 
        WHERE ticker = ANY($1)
    """

    try:
        async with db.pool.acquire() as conn:
            rows = await conn.fetch(query, formatted_tickers)
        results = []
        for row in rows:
            # Extract pure ticker "DW" from "DW.IC1"
            clean_ticker = row["ticker"].split(".")[0]
            results.append(
                {
                    "ticker": clean_ticker,
                    "ask": float(row["askprice"] or 0),
                    "bid": float(row["bidprice"] or 0),
                }
            )

        return {"status": "success", "data": results}
    except Exception as e:
        return {"status": "error", "message": str(e)}
