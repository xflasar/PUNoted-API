from datetime import datetime, timedelta
from asyncpg import Connection
from typing import List, Dict, Any

class ContractsRepository:
    def __init__(self, db):
        self.db = db

    async def get_contracts(self, user_id: str, category: str, status: str, search: str, limit: int, offset: int) -> Dict[str, Any]:
        # 1. Prepare Parameters
        params = ['silentreaper']
        next_idx = 2
        
        status_clause = "TRUE"
        if status and status != "ALL":
            status_clause = f"c.status = ${next_idx}"
            params.append(status)
            next_idx += 1

        search_clause = "TRUE"
        if search:
            search_clause = f"(c.name ILIKE ${next_idx} OR c.localid ILIKE ${next_idx} OR c.partnername ILIKE ${next_idx} OR c.partnercode ILIKE ${next_idx})"
            params.append(f"%{search}%")
            next_idx += 1

        # Dynamic Category Logic
        category_clause = "TRUE"
        if category == "TRADE":
            category_clause = "EXISTS (SELECT 1 FROM contract_conditions x WHERE x.contractid = c.id AND x.type IN ('COMEX_PURCHASE_PICKUP', 'DELIVERY'))"
        elif category == "SHIPMENT":
            category_clause = "EXISTS (SELECT 1 FROM contract_conditions x WHERE x.contractid = c.id AND x.type = 'DELIVERY_SHIPMENT')"
        elif category == "LOAN":
            category_clause = "EXISTS (SELECT 1 FROM contract_conditions x WHERE x.contractid = c.id AND x.type IN ('LOAN_PAYOUT', 'LOAN_INSTALLMENT'))"

        async with self.db.pool.acquire() as conn:
            # 2. Get Total Count (Strictly matching the Data Query logic)
            # We must JOIN on contract_conditions with the exact same logic as the main query
            # to ensure the Count matches the visible rows.
            count_sql = f"""
                SELECT COUNT(DISTINCT c.id)
                FROM contracts c
                INNER JOIN users u ON u.userdataid = c.userid
                -- We replicate the JOIN/WHERE logic from your main SQL here
                -- This ensures we count only the rows that satisfy the 'party = contractparty' check
                LEFT JOIN contract_conditions cc_pay 
                    ON c.id = cc_pay.contractid 
                    AND c.party = cc_pay.contractparty 
                    AND cc_pay.type = 'PAYMENT'
                WHERE u.username = $1
                  AND {status_clause}
                  AND {search_clause}
                  AND {category_clause}
                  -- CRITICAL: This condition filters the list, so it must filter the count too
                  AND c.party = cc_pay.contractparty
            """
            total_count = await conn.fetchval(count_sql, *params)

            # 3. Get Data
            params.append(limit) 
            params.append(offset)
            
            sql = f"""
                SELECT 
                  c.id, c.localid, c.name, c.date, c.status, 
                  CASE
                    WHEN EXISTS (
                      SELECT 1
                      FROM contract_conditions cc
                      WHERE cc.contractid = c.id
                        AND cc.type = 'COMEX_PURCHASE_PICKUP'
                        AND cc.contractparty = c.party
                    ) THEN 'BUY'
                  
                    WHEN EXISTS (
                      SELECT 1
                      FROM contract_conditions cc
                      WHERE cc.contractid = c.id
                        AND cc.type = 'DELIVERY'
                        AND cc.party = c.party
                    ) THEN 'SELL'

                    WHEN EXISTS (
                        SELECT 1
                        FROM contract_conditions cc
                        WHERE cc.contractid = c.id
                          AND cc.type ='LOAN_PAYOUT'
                          AND cc.party = c.party
                    ) THEN 'LOAN_GIVEN'
                    
                    WHEN EXISTS (
                        SELECT 1
                        FROM contract_conditions cc
                        WHERE cc.contractid = c.id
                          AND cc.type ='LOAN_PAYOUT'
                          AND cc.party != c.party
                    ) THEN 'LOAN_TAKEN'

                    WHEN EXISTS (
                        SELECT 1
                        FROM contract_conditions cc
                        WHERE cc.contractid = c.id
                          AND cc.type = 'DELIVERY_SHIPMENT'
                          AND cc.party != c.party
                    ) THEN 'SHIPMENT_GIVEN'
                    
                    WHEN EXISTS (
                        SELECT 1
                        FROM contract_conditions cc
                        WHERE cc.contractid = c.id
                          AND cc.type = 'DELIVERY_SHIPMENT'
                          AND cc.party = c.party
                    ) THEN 'SHIPMENT_TAKEN'
                  
                    ELSE 'OTHER'
                  END AS contracttype,
                  c.partnername, c.partnercode, c.duedate, c.party,
                  COALESCE(SUM(cc_pay.amountmoney), 0) as total_amount,
                  MAX(cc_pay.currencymoney) as currency
                FROM contracts c
                JOIN users u ON u.userdataid = c.userid
                LEFT JOIN contract_conditions cc_pay 
                  ON c.id = cc_pay.contractid 
                  AND c.party = cc_pay.contractparty
                  AND (cc_pay.type = 'PAYMENT' or cc_pay.type = 'LOAN_INSTALLMENT' or cc_pay.type = 'LOAN_PAYOUT')

                WHERE u.username = $1
                  AND {category_clause}
                  AND c.party = cc_pay.contractparty 
                  AND {status_clause} 
                  AND {search_clause}
                GROUP BY c.id, c.localid, c.name, c.date, c.status, c.contracttype, c.partnername, c.partnercode, c.duedate, c.party
                ORDER BY c.date DESC
                LIMIT ${next_idx} OFFSET ${next_idx + 1}
            """

            rows = await conn.fetch(sql, *params)
        
        return {
            "items": [dict(row) for row in rows],
            "total": total_count
        }
    
    async def get_loans(self, user_id: str, status: str, search: str) -> Dict[str, Any]:

        # 1. Clean Parameter Setup
        status_val = None if status == "ALL" or not status else status
        search_val = f"%{search}%" if search else None

        user_id = 'silentreaper' # FIX: Replace with dynamic user_id (username) from auth context

        # Base WHERE clause used for BOTH count and pagination
        # $1 = user_id (username), $2 = status, $3 = search
        base_where = """
            WHERE u.username = $1
              AND ($2::text IS NULL OR c.status = $2)
              AND ($3::text IS NULL OR (c.name ILIKE $3 OR c.localid ILIKE $3 OR c.partnername ILIKE $3 OR c.partnercode ILIKE $3))
              AND EXISTS (
                  SELECT 1 FROM contract_conditions x 
                  WHERE x.contractid = c.id AND x.contractparty = c.party AND x.type IN ('LOAN_PAYOUT', 'LOAN_INSTALLMENT')
              )
              AND (c.status != 'BREACHED' OR c.canextend = true)
        """

        async with self.db.pool.acquire() as conn:
            # 2. Get Total Count
            count_sql = f"""
                SELECT COUNT(c.id)
                FROM contracts c
                JOIN users u ON u.userdataid = c.userid
                {base_where}
            """
            total_count = await conn.fetchval(count_sql, user_id, status_val, search_val)

            if total_count == 0:
                return {"items": [], "total": 0}

            # 3. Main Data Query
            sql = f"""
                WITH PagedContracts AS (
                    SELECT c.id, c.party
                    FROM contracts c
                    JOIN users u ON u.userdataid = c.userid
                    {base_where}
                    ORDER BY c.date DESC
                ),
                LoanStats AS (
    SELECT 
        cc.contractid,

        -- Because we evaluate ALL conditions, the math will be 100% accurate 
        -- regardless of who is paying the installments.
        COUNT(*) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') as total_installments,
        COUNT(*) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT' AND cc.status = 'FULFILLED') as fulfilled_installments,
        MAX(cc.deadlineduration_millis) / 86400000.0 as avg_interval_days,

        CASE 
            WHEN SUM(cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') > 0 
            THEN (MAX(cli.interestamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') / SUM(cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT')) * 100.0
            ELSE 0.0
        END as interest_rate,

        CASE 
            WHEN SUM(cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') > 0 
            THEN (SUM(cli.interestamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') / SUM(cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT')) * 100.0
            ELSE 0.0
        END as total_interest_rate,

        CASE 
            WHEN COALESCE(MIN(cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT'), 0) = 0 THEN 'INTEREST_LOAN'
            WHEN COUNT(DISTINCT cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') < COUNT(DISTINCT cli.totalamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') THEN 'STABLE_LOAN'
            WHEN COUNT(DISTINCT cli.totalamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') < COUNT(DISTINCT cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') THEN 'ANNUITY_LOAN'
            WHEN (MAX(cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') - MIN(cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT')) <= (MAX(cli.totalamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') - MIN(cli.totalamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT')) THEN 'STABLE_LOAN'
            ELSE 'ANNUITY_LOAN'
        END as loan_strategy,

        SUM(cli.totalamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') as sum_total,
        MAX(cc.currencymoney) as currency_code,

        BOOL_OR(cc.type = 'LOAN_PAYOUT' AND cc.party = pc.party) as is_payout_party

    FROM contract_conditions cc
    JOIN PagedContracts pc ON pc.id = cc.contractid AND cc.contractparty = pc.party
    LEFT JOIN contract_loan_installments cli ON cli.conditionid = cc.id AND cli.contractparty = cc.contractparty
    GROUP BY cc.contractid
)
SELECT 
    c.id, c.localid, c.name, c.date, 
    CASE 
        WHEN c.status = 'BREACHED' AND c.extensiondeadline IS NOT NULL THEN 'BREACHED (EXTENDED)'
        ELSE c.status 
    END AS status, 
    c.partnername, c.partnercode, c.duedate, c.party,

    -- Contract Type (Given vs Taken)
    CASE
        WHEN ls.is_payout_party THEN 'LOAN_GIVEN'
        ELSE 'LOAN_TAKEN'
    END AS contracttype,

    COALESCE(ls.sum_total, 0) as total_amount,
    COALESCE(ls.currency_code, 'ICA') as currency,

    ROUND(COALESCE(ls.interest_rate, 0)::numeric, 3) as implied_interest_rate,
    ROUND(COALESCE(ls.total_interest_rate, 0)::numeric, 3) as total_interest_rate,

    COALESCE(ls.loan_strategy, 'UNKNOWN') as loan_strategy,
    c.extensiondeadline,

    ROUND(COALESCE(ls.avg_interval_days, 0)::numeric) as installment_interval,
    COALESCE(ls.total_installments, 0) as installment_count,
    COALESCE(ls.fulfilled_installments, 0) as installment_done

FROM PagedContracts pc
JOIN contracts c ON c.id = pc.id AND c.party = pc.party
LEFT JOIN LoanStats ls ON ls.contractid = c.id
ORDER BY c.date DESC;
            """

            rows = await conn.fetch(sql, user_id, status_val, search_val)

            return {
                "items": [dict(row) for row in rows],
                "total": total_count
            }

    async def get_contract_detail(self, contract_id: str, user_id: str) -> Dict[str, Any]:
        async with self.db.pool.acquire() as conn:
            # 1. Fetch the Base Contract
            # We need 'party' to determine if we are the Lender or Borrower later
            contract = await conn.fetchrow(
                """
                SELECT c.* FROM contracts c 
                INNER JOIN users u ON u.userdataid = c.userid 
                WHERE c.id = $1 AND u.username = $2
                """, 
                contract_id, 'silentreaper' # Using dynamic user_id (username)
            )

            if not contract:
                return None

            # 2. Fetch Conditions (including Loan Installment data if available)
            # Note: We select cli.totalamount specifically to avoid column collision
            conditions = await conn.fetch(
                """
                SELECT cc.*, 
                       (SELECT string_agg(cm.amount || 'x ' || cm.materialid, ', ') 
                        FROM contract_materials cm WHERE cm.contractconditionid = cc.id) as material_summary,
                       cli.*
                FROM contract_conditions cc
                INNER JOIN contracts c ON c.id = cc.contractid
                INNER JOIN users u ON u.userdataid = c.userid
                LEFT JOIN contract_loan_installments cli ON cli.conditionid = cc.id AND cc.contractparty = cli.contractparty
                WHERE c.id = $1 AND u.username = $2 AND c.party = cc.contractparty
                ORDER BY cc.index ASC
                """, 
                contract_id, 'tonatsi'
            )

            # 3. Flatten and Process
            result = dict(contract)
            result["conditions"] = [dict(c) for c in conditions]

            # --- LOGIC: Determine Contract Type & Calculate Totals ---

            current_type = "OTHER"
            total_principal = 0.0
            total_interest = 0.0

            # We need to know who 'we' are in this contract
            my_party = result.get('party')

            for c in result["conditions"]:
                c_type = c.get('type')
                c_party = c.get('party')

                # A. Calculate Total for Loans
                if c_type == 'LOAN_INSTALLMENT':
                    # Sum Principal
                    p = c.get('repaymentamount')
                    if p: total_principal += float(p)

                    # Sum Interest
                    i = c.get('interestamount')
                    if i: total_interest += float(i)

                # B. Determine Contract Type (Priority Logic)
                # We check specific conditions to tag the whole contract
                if c_type == 'LOAN_PAYOUT':
                    current_type = 'LOAN_GIVEN' if c_party == my_party else 'LOAN_TAKEN'
                elif c_type == 'COMEX_PURCHASE_PICKUP' and current_type == 'OTHER':
                    current_type = 'BUY'
                elif c_type == 'DELIVERY' and current_type == 'OTHER':
                    current_type = 'SELL'
                elif c_type == 'DELIVERY_SHIPMENT':
                    current_type = 'SHIPMENT_TAKEN' if c_party == my_party else 'SHIPMENT_GIVEN'

            # 4. Apply Computed Values
            result['contracttype'] = current_type

            # Only override total_amount if it's missing (0) in the base table
            # AND we actually calculated a loan total
            if not result.get('total_amount') and (total_principal + total_interest) > 0:
                result['total_amount'] = total_principal + total_interest
            
            if total_principal > 0:
                result['implied_interest_rate'] = round((total_interest / total_principal) * 100, 3)
            else:
                result['implied_interest_rate'] = 0.0

            return result

    async def get_stats(self, user_id: str) -> Dict[str, Any]:
            async with self.db.pool.acquire() as conn:
                # 1. Status Counts
                status_rows = await conn.fetch(
                    "SELECT c.status, COUNT(*) as count FROM contracts c INNER JOIN users u ON u.userdataid = c.userid WHERE u.accountid = $1 GROUP BY c.status", 
                    user_id
                )
                status_counts = {r['status']: r['count'] for r in status_rows}

                # 2. Financials (Simplified logic based on contract type)
                # Assuming 'SELLING'/'SHIPPING' is revenue, 'BUYING' is expense
                financials = await conn.fetchrow("""
                    SELECT 
                        COALESCE(SUM(cc.amountmoney) FILTER (
                            WHERE cc.status = 'FULFILLED' AND cc.type = 'PAYMENT' AND cc.party != cc.contractparty
                        ), 0) as revenue,
                        COALESCE(SUM(cc.amountmoney) FILTER (
                            WHERE cc.status = 'FULFILLED' AND cc.type = 'PAYMENT' AND cc.party = cc.contractparty
                        ), 0) as expenses
                    FROM contracts c
                    JOIN contract_conditions cc ON c.id = cc.contractid
                    INNER JOIN users u ON u.userdataid = c.userid
                    WHERE u.accountid = $1 AND c.party = cc.contractparty
                """, user_id)

                # 3. History (Last 14 days activity)
                history_rows = await conn.fetch("""
                    SELECT date::date as day, COUNT(*) as count 
                    FROM contracts 
                    INNER JOIN users ON users.userdataid = contracts.userid
                    WHERE users.accountid = $1 AND date > NOW() - INTERVAL '14 days'
                    GROUP BY day ORDER BY day ASC
                """, user_id)

                return {
                    "total_count": sum(status_counts.values()),
                    "status_counts": status_counts,
                    "total_revenue": financials['revenue'],
                    "total_expenses": financials['expenses'],
                    "net_value": financials['revenue'] - financials['expenses'],
                    "history": [{"date": str(r['day']), "count": r['count']} for r in history_rows]
                }
    async def get_dashboard_stats(self, user_id: str) -> Dict[str, Any]:
        async with self.db.pool.acquire() as conn:
            # Financials query using date_trunc for weekly comparison (Monday start)
            # Logic: If I am the payer (party == contractparty) -> Expense. Else -> Revenue.
            financial_query = """
                SELECT 
                    COUNT(DISTINCT c.id) as count,
                    COALESCE(SUM(cc.amountmoney) FILTER (
                        WHERE cc.status = 'FULFILLED' AND cc.type = 'PAYMENT' AND cc.party != cc.contractparty
                    ), 0) as revenue,
                    COALESCE(SUM(cc.amountmoney) FILTER (
                        WHERE cc.status = 'FULFILLED' AND cc.type = 'PAYMENT' AND cc.party = cc.contractparty
                    ), 0) as expenses
                FROM contracts c
                INNER JOIN users u ON u.userdataid = c.userid
                LEFT JOIN contract_conditions cc ON c.id = cc.contractid AND c.party = cc.contractparty
                WHERE u.accountid = $1 AND c.date >= $2 AND c.date < $3
            """

            # Calculate dates
            today = datetime.utcnow()
            # Start of current week (Monday)
            start_current_week = (today - timedelta(days=today.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
            # Start of last week
            start_last_week = start_current_week - timedelta(days=7)
            # End of current week (Next Monday)
            end_current_week = start_current_week + timedelta(days=7)

            # Execute
            current = await conn.fetchrow(financial_query, user_id, start_current_week, end_current_week)
            last = await conn.fetchrow(financial_query, user_id, start_last_week, start_current_week)

            # Counts
            counts = await conn.fetchrow("""
                SELECT 
                    COUNT(*) FILTER (WHERE c.status IN ('OPEN', 'CLOSED', 'PARTIALLY_FULFILLED')) as active,
                    COUNT(*) FILTER (WHERE c.status = 'BREACHED' AND c.canextend = true) as active_breached
                FROM contracts c
                INNER JOIN users u ON u.userdataid = c.userid
                WHERE u.accountid = $1
            """, user_id)

            return {
                "current_week": {
                    "revenue": float(current['revenue']), 
                    "expenses": float(current['expenses']), 
                    "net": float(current['revenue']) - float(current['expenses']), 
                    "count": current['count']
                },
                "last_week": {
                    "revenue": float(last['revenue']), 
                    "expenses": float(last['expenses']), 
                    "net": float(last['revenue']) - float(last['expenses']), 
                    "count": last['count']
                },
                "total_active": counts['active'],
                "active_breached": counts['active_breached']
            }

    async def get_dashboard_widgets(self, user_id: str) -> Dict[str, List[Any]]:
        async with self.db.pool.acquire() as conn:
            # Base selection with total_amount calc for list display
            base_sql = """
                SELECT c.id, c.localid, c.name, c.date, c.status, c.contracttype, 
                       c.partnername, c.partnercode, c.duedate, c.party,
                       COALESCE(SUM(cc.amountmoney) FILTER (WHERE cc.type = 'PAYMENT'), 0) as total_amount,
                       MAX(cc.currencymoney) FILTER (WHERE cc.type = 'PAYMENT') as currency
                FROM contracts c
                INNER JOIN users u ON u.userdataid = c.userid
                LEFT JOIN contract_conditions cc ON c.id = cc.contractid AND c.party = cc.contractparty
                WHERE u.accountid = $1 
            """
            group_by = " GROUP BY c.id, c.localid, c.name, c.date, c.status, c.contracttype, c.partnername, c.partnercode, c.duedate, c.party "

            # 1. Immediate (Due < 24h)
            immediate = await conn.fetch(
                base_sql + " AND c.status = 'PARTIALLY_FILLED' AND c.duedate < NOW() + INTERVAL '1 day' " + group_by + " ORDER BY c.duedate ASC LIMIT 5", 
                user_id
            )
            
            # 2. Recent Active
            active = await conn.fetch(
                base_sql + " AND c.status IN ('OPEN', 'CLOSED', 'PARTIALLY_FILLED') " + group_by + " ORDER BY c.date DESC LIMIT 5", 
                user_id
            )
            
            # 3. Breached
            breached = await conn.fetch(
                base_sql + " AND c.status = 'BREACHED' " + group_by + " ORDER BY c.date DESC LIMIT 5", 
                user_id
            )

            def map_row(r):
                # Simple helper to map DB row to schema
                return dict(r)

            return {
                "immediate": [map_row(r) for r in immediate],
                "active": [map_row(r) for r in active],
                "breached": [map_row(r) for r in breached]
            }