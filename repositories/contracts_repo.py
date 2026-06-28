from typing import Optional, Any, Dict, List
from datetime import datetime, timedelta

class ContractsRepository:
    def __init__(self, db):
        self.db = db

    async def get_contracts(self, user_id: str, category: str, status: str, search: str, limit: int, offset: int) -> Dict[str, Any]:
        async with self.db.pool.acquire() as conn:
            username = await conn.fetchval("SELECT username FROM users WHERE accountid = $1", user_id)

        # 1. Prepare Parameters
        params = [username]
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

        category_clause = "TRUE"
        if category == "TRADE":
            category_clause = """
                EXISTS (SELECT 1 FROM contract_conditions x WHERE x.contractid = c.id AND x.type IN ('COMEX_PURCHASE_PICKUP', 'DELIVERY', 'PROVISION_SHIPMENT', 'PICKUP_SHIPMENT', 'PROVISION')) 
                AND NOT EXISTS (SELECT 1 FROM contract_conditions x WHERE x.contractid = c.id AND x.type IN ('DELIVERY_SHIPMENT', 'EXPLORATION', 'LOAN_PAYOUT', 'LOAN_INSTALLMENT'))
            """
        elif category == "SHIPMENT":
            category_clause = "EXISTS (SELECT 1 FROM contract_conditions x WHERE x.contractid = c.id AND x.type = 'DELIVERY_SHIPMENT')"
        elif category == "LOAN":
            category_clause = "EXISTS (SELECT 1 FROM contract_conditions x WHERE x.contractid = c.id AND x.type IN ('LOAN_PAYOUT', 'LOAN_INSTALLMENT'))"

        async with self.db.pool.acquire() as conn:
            # 2. Get Total Count
            count_sql = f"""
                SELECT COUNT(DISTINCT c.id)
                FROM contracts c
                INNER JOIN users u ON u.userdataid = c.userid
                WHERE u.username = $1
                  AND {status_clause}
                  AND {search_clause}
                  AND {category_clause}
            """
            total_count = await conn.fetchval(count_sql, *params)

            # 3. Get Data
            params.append(limit)
            params.append(offset)

            sql = f"""
                SELECT 
                  c.id, c.localid, c.name, c.date, c.status, 
                  c.partnername, c.partnercode, c.duedate, 
                  COALESCE(c.party, 'UNKNOWN')::text as party,
                  
                  CASE
                    WHEN EXISTS (SELECT 1 FROM contract_conditions cc WHERE cc.contractid = c.id AND cc.type ='LOAN_PAYOUT' AND cc.party = c.party) THEN 'LOAN_GIVEN'
                    WHEN EXISTS (SELECT 1 FROM contract_conditions cc WHERE cc.contractid = c.id AND cc.type ='LOAN_PAYOUT' AND cc.party != c.party) THEN 'LOAN_TAKEN'
                    WHEN EXISTS (SELECT 1 FROM contract_conditions cc WHERE cc.contractid = c.id AND cc.type = 'EXPLORATION') THEN 'EXPLORATION'
                    WHEN (c.name ILIKE '%motion%' OR c.name ILIKE '%mot%' OR c.preamble ILIKE '%motion%' OR c.preamble ILIKE '%mot%') THEN 'MOTION'
                    WHEN EXISTS (SELECT 1 FROM contract_conditions cc WHERE cc.contractid = c.id AND cc.type = 'DELIVERY_SHIPMENT' AND cc.party != c.party) THEN 'SHIPMENT_GIVEN'
                    WHEN EXISTS (SELECT 1 FROM contract_conditions cc WHERE cc.contractid = c.id AND cc.type = 'DELIVERY_SHIPMENT' AND cc.party = c.party) THEN 'SHIPMENT_TAKEN'
                    WHEN EXISTS (SELECT 1 FROM contract_conditions cc WHERE cc.contractid = c.id AND cc.type IN ('PROVISION_SHIPMENT', 'DELIVERY', 'PROVISION') AND cc.party = c.party) THEN 'SELL'
                    WHEN EXISTS (
                       SELECT 1 FROM contract_conditions cc
                       WHERE cc.contractid = c.id
                         AND (
                           (cc.type IN ('PROVISION_SHIPMENT', 'DELIVERY', 'PROVISION') AND cc.party != c.party)
                           OR (cc.type IN ('COMEX_PURCHASE_PICKUP', 'PICKUP_SHIPMENT') AND cc.contractparty = c.party)
                         )
                    ) THEN 'BUY'
                    ELSE 'OTHER'
                  END AS contracttype,
                  
                  (
                    EXISTS (
                        SELECT 1 FROM contract_conditions cc_inc 
                        WHERE cc_inc.contractid = c.id 
                          AND cc_inc.type IN ('PAYMENT', 'LOAN_INSTALLMENT', 'LOAN_PAYOUT') 
                          AND cc_inc.party != c.party
                    )
                  )::boolean as is_income,
                  
                  COALESCE((
                      SELECT SUM(cc_pay.amountmoney)
                      FROM contract_conditions cc_pay
                      WHERE cc_pay.contractid = c.id
                        AND cc_pay.type IN ('PAYMENT', 'LOAN_INSTALLMENT', 'LOAN_PAYOUT')
                  ), 0)::float as total_amount,
                  
                  COALESCE((
                      SELECT MAX(cc_pay.currencymoney)
                      FROM contract_conditions cc_pay
                      WHERE cc_pay.contractid = c.id
                        AND cc_pay.type IN ('PAYMENT', 'LOAN_INSTALLMENT', 'LOAN_PAYOUT')
                  ), 'ICA') as currency

                FROM contracts c
                JOIN users u ON u.userdataid = c.userid
                WHERE u.username = $1
                  AND {category_clause}
                  AND {status_clause} 
                  AND {search_clause}
                ORDER BY c.date DESC
                LIMIT ${next_idx} OFFSET ${next_idx + 1}
            """

            rows = await conn.fetch(sql, *params)

        return {
            "items": [dict(row) for row in rows],
            "total": total_count
        }

    async def get_loans(self, user_id: str, status: str, search: str) -> Dict[str, Any]:
        async with self.db.pool.acquire() as conn:
            username = await conn.fetchval("SELECT username FROM users WHERE accountid = $1", user_id)

        status_val = None if status == "ALL" or not status else status
        search_val = f"%{search}%" if search else None

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
            count_sql = f"""
                SELECT COUNT(c.id)
                FROM contracts c
                JOIN users u ON u.userdataid = c.userid
                {base_where}
            """
            total_count = await conn.fetchval(count_sql, username, status_val, search_val)

            if total_count == 0:
                return {"items": [], "total": 0}

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

            rows = await conn.fetch(sql, username, status_val, search_val)

            return {
                "items": [dict(row) for row in rows],
                "total": total_count
            }

    async def get_contract_detail(self, contract_id: str, user_id: str) -> Dict[str, Any]:
        async with self.db.pool.acquire() as conn:
            username = await conn.fetchval("SELECT username FROM users WHERE accountid = $1", user_id)

            contract = await conn.fetchrow(
                """
                SELECT c.*,
                   CASE 
                     WHEN EXISTS (SELECT 1 FROM contract_conditions cc WHERE cc.contractid = c.id AND cc.type ='LOAN_PAYOUT' AND cc.party = c.party) THEN 'LOAN_GIVEN'
                     WHEN EXISTS (SELECT 1 FROM contract_conditions cc WHERE cc.contractid = c.id AND cc.type ='LOAN_PAYOUT' AND cc.party != c.party) THEN 'LOAN_TAKEN'
                     WHEN EXISTS (SELECT 1 FROM contract_conditions cc WHERE cc.contractid = c.id AND cc.type = 'EXPLORATION') THEN 'EXPLORATION'
                     WHEN (c.name ILIKE '%motion%' OR c.name ILIKE '%mot%' OR c.preamble ILIKE '%motion%' OR c.preamble ILIKE '%mot%') THEN 'MOTION'
                     WHEN EXISTS (SELECT 1 FROM contract_conditions cc WHERE cc.contractid = c.id AND cc.type = 'DELIVERY_SHIPMENT' AND cc.party != c.party) THEN 'SHIPMENT_GIVEN'
                     WHEN EXISTS (SELECT 1 FROM contract_conditions cc WHERE cc.contractid = c.id AND cc.type = 'DELIVERY_SHIPMENT' AND cc.party = c.party) THEN 'SHIPMENT_TAKEN'
                     WHEN EXISTS (SELECT 1 FROM contract_conditions cc WHERE cc.contractid = c.id AND cc.type IN ('PROVISION_SHIPMENT', 'DELIVERY', 'PROVISION') AND cc.party = c.party) THEN 'SELL'
                     WHEN EXISTS (
                       SELECT 1 FROM contract_conditions cc
                       WHERE cc.contractid = c.id
                         AND (
                           (cc.type IN ('PROVISION_SHIPMENT', 'DELIVERY', 'PROVISION') AND cc.party != c.party)
                           OR (cc.type IN ('COMEX_PURCHASE_PICKUP', 'PICKUP_SHIPMENT') AND cc.contractparty = c.party)
                         )
                     ) THEN 'BUY'
                     ELSE 'OTHER'
                  END AS contracttype,
                  
                  COALESCE((
                      SELECT TRUE 
                      FROM contract_conditions cc_inc 
                      WHERE cc_inc.contractid = c.id 
                        AND cc_inc.type IN ('PAYMENT', 'LOAN_INSTALLMENT', 'LOAN_PAYOUT') 
                        AND c.party IS NOT NULL
                        AND cc_inc.party != c.party 
                      LIMIT 1
                  ), FALSE) AS is_income

                FROM contracts c 
                INNER JOIN users u ON u.userdataid = c.userid 
                WHERE c.id = $1 AND u.username = $2
                """,
                contract_id, username
            )

            if not contract:
                return None

            conditions = await conn.fetch(
                """
                SELECT cc.*, 
                       (SELECT string_agg(cm.amount || 'x ' || COALESCE(m.ticker, cm.materialid), ', ') 
                        FROM contract_materials cm 
                        LEFT JOIN materials m ON m.materialid = cm.materialid
                        WHERE cm.contractconditionid = cc.id) as material_summary,
                       cli.*,
                       s1.name as addresssystemname,
                       p1.name as addressplanetname,
                       st1.name as addressstationname,
                       s2.name as destinationsystemname,
                       p2.name as destinationplanetname,
                       st2.name as destinationstationname
                FROM contract_conditions cc
                INNER JOIN contracts c ON c.id = cc.contractid
                INNER JOIN users u ON u.userdataid = c.userid
                LEFT JOIN contract_loan_installments cli ON cli.conditionid = cc.id AND cc.contractparty = cli.contractparty
                LEFT JOIN systems s1 ON s1.systemid = cc.addresssystemid
                LEFT JOIN planets p1 ON p1.planetid = cc.addressplanetid
                LEFT JOIN stations st1 ON st1.stationid = cc.addressstationid
                LEFT JOIN systems s2 ON s2.systemid = cc.destinationsystemid
                LEFT JOIN planets p2 ON p2.planetid = cc.destinationplanetid
                LEFT JOIN stations st2 ON st2.stationid = cc.destinationstationid
                WHERE c.id = $1 AND u.username = $2
                ORDER BY cc.index ASC
                """,
                contract_id, username
            )

            result = dict(contract)
            result["conditions"] = [dict(c) for c in conditions]

            total_principal = 0.0
            total_interest = 0.0

            for c in result["conditions"]:
                c_type = c.get('type')
                if c_type == 'LOAN_INSTALLMENT':
                    p = c.get('repaymentamount')
                    if p: total_principal += float(p)
                    i = c.get('interestamount')
                    if i: total_interest += float(i)

            if not result.get('total_amount') and (total_principal + total_interest) > 0:
                result['total_amount'] = total_principal + total_interest

            if total_principal > 0:
                result['implied_interest_rate'] = round((total_interest / total_principal) * 100, 3)
            else:
                result['implied_interest_rate'] = 0.0

            return result

    async def get_stats(self, user_id: str) -> Dict[str, Any]:
        async with self.db.pool.acquire() as conn:
            status_rows = await conn.fetch(
                "SELECT c.status, COUNT(*) as count FROM contracts c INNER JOIN users u ON u.userdataid = c.userid WHERE u.accountid = $1 GROUP BY c.status",
                user_id
            )
            status_counts = {r['status']: r['count'] for r in status_rows}

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
            financial_query = """
                SELECT 
                    COUNT(DISTINCT c.id) as count,
                    COALESCE(SUM(cc.amountmoney) FILTER (
                        WHERE cc.status = 'FULFILLED' AND cc.type = 'PAYMENT' AND (cc.party != c.party OR c.party IS NULL)
                    ), 0) as revenue,
                    COALESCE(SUM(cc.amountmoney) FILTER (
                        WHERE cc.status = 'FULFILLED' AND cc.type = 'PAYMENT' AND cc.party = c.party
                    ), 0) as expenses
                FROM contracts c
                INNER JOIN users u ON u.userdataid = c.userid
                LEFT JOIN contract_conditions cc ON c.id = cc.contractid
                WHERE u.accountid = $1 AND c.date >= $2 AND c.date < $3
            """

            today = datetime.utcnow()
            start_current_week = (today - timedelta(days=today.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
            start_last_week = start_current_week - timedelta(days=7)
            end_current_week = start_current_week + timedelta(days=7)

            current = await conn.fetchrow(financial_query, user_id, start_current_week, end_current_week)
            last = await conn.fetchrow(financial_query, user_id, start_last_week, start_current_week)

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

            immediate = await conn.fetch(
                base_sql + " AND c.status = 'PARTIALLY_FILLED' AND c.duedate < NOW() + INTERVAL '1 day' " + group_by + " ORDER BY c.duedate ASC LIMIT 5",
                user_id
            )

            active = await conn.fetch(
                base_sql + " AND c.status IN ('OPEN', 'CLOSED', 'PARTIALLY_FILLED') " + group_by + " ORDER BY c.date DESC LIMIT 5",
                user_id
            )

            breached = await conn.fetch(
                base_sql + " AND c.status = 'BREACHED' " + group_by + " ORDER BY c.date DESC LIMIT 5",
                user_id
            )

            return {
                "immediate": [dict(r) for r in immediate],
                "active": [dict(r) for r in active],
                "breached": [dict(r) for r in breached]
            }

    async def sync_loan_repayments(self):
        async with self.db.pool.acquire() as conn:
            await conn.execute("""
                UPDATE bank_loan_requests blr
                SET status = 'REPAID', updated_at = NOW()
                FROM contracts c
                WHERE (blr.contract_id = c.localid OR blr.contract_id = c.id)
                  AND blr.status = 'APPROVED'
                  AND c.status IN ('FULFILLED', 'CLOSED')
            """)

    async def create_bank(self, owner_username: str, name: str, description: str, liquidity: float, rate: float) -> Dict[str, Any]:
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO player_banks (name, owner_username, liquidity, default_interest_rate, description)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING *
            """, name, owner_username, liquidity, rate, description)
            return dict(row)

    async def get_banks(self) -> List[Dict[str, Any]]:
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT b.*,
                       (SELECT COUNT(*) FROM bank_loan_requests WHERE bank_id = b.id AND status = 'APPROVED') as active_loans_count
                FROM player_banks b
                ORDER BY b.created_at DESC
            """)
            return [dict(row) for row in rows]

    async def get_bank_by_owner(self, owner_username: str) -> Optional[Dict[str, Any]]:
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM player_banks WHERE owner_username = $1", owner_username)
            return dict(row) if row else None

    async def create_loan_request(self, bank_id: int, requester_username: str, amount: float, interest_rate: float, term_days: int) -> Dict[str, Any]:
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO bank_loan_requests (bank_id, requester_username, amount, interest_rate, term_days)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING *
            """, bank_id, requester_username, amount, interest_rate, term_days)
            return dict(row)

    async def get_requested_loans(self, requester_username: str) -> List[Dict[str, Any]]:
        await self.sync_loan_repayments()
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT lr.*, b.name as bank_name
                FROM bank_loan_requests lr
                JOIN player_banks b ON lr.bank_id = b.id
                WHERE lr.requester_username = $1
                ORDER BY lr.created_at DESC
            """, requester_username)
            return [dict(row) for row in rows]

    async def get_bank_loan_requests(self, bank_id: int) -> List[Dict[str, Any]]:
        await self.sync_loan_repayments()
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT lr.*, b.name as bank_name
                FROM bank_loan_requests lr
                JOIN player_banks b ON lr.bank_id = b.id
                WHERE lr.bank_id = $1
                ORDER BY lr.created_at DESC
            """, bank_id)
            return [dict(row) for row in rows]

    async def action_loan_request(self, loan_id: int, status: str, contract_id: Optional[str] = None) -> bool:
        async with self.db.pool.acquire() as conn:
            if status == "APPROVED" and contract_id:
                result = await conn.execute("""
                    UPDATE bank_loan_requests
                    SET status = $1, contract_id = $2, updated_at = NOW()
                    WHERE id = $3
                """, status, contract_id, loan_id)
            else:
                result = await conn.execute("""
                    UPDATE bank_loan_requests
                    SET status = $1, updated_at = NOW()
                    WHERE id = $2
                """, status, loan_id)
            return "UPDATE 1" in result