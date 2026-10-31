from typing import Optional, Any, Dict, List
from datetime import datetime, timedelta, timezone
import csv
import io


class BankingRepository:
    def __init__(self, db):
        self.db = db

    async def sync_loan_repayments(self, bank_id: Optional[int] = None):
        """
        Synchronizes bank loan requests with contract condition fulfillment statuses.
        Automatically settles PENDING deposits/withdrawals/loans when in-game conditions are FULFILLED.
        If bank_id is provided, only inspects transactions for that specific bank.
        """
        async with self.db.pool.acquire() as conn:
            # 1. Sync pending contract transactions (DEPOSITS, WITHDRAWALS, LOANS)
            if bank_id is not None:
                pending_records = await conn.fetch("""
                    SELECT 
                        blr.id, blr.bank_id, blr.amount, blr.transaction_type, blr.contract_id,
                        c.id as contract_db_id,
                        c.status as contract_status
                    FROM bank_loan_requests blr
                    JOIN contracts c ON (c.localid = blr.contract_id OR c.id = blr.contract_id)
                    WHERE blr.status = 'PENDING' AND blr.bank_id = $1
                """, bank_id)
            else:
                pending_records = await conn.fetch("""
                    SELECT 
                        blr.id, blr.bank_id, blr.amount, blr.transaction_type, blr.contract_id,
                        c.id as contract_db_id,
                        c.status as contract_status
                    FROM bank_loan_requests blr
                    JOIN contracts c ON (c.localid = blr.contract_id OR c.id = blr.contract_id)
                    WHERE blr.status = 'PENDING'
                """)
            
            for rec in pending_records:
                c_status = rec["contract_status"]
                c_id = rec["contract_db_id"]
                blr_id = rec["id"]
                b_id = rec["bank_id"]
                amt = float(rec["amount"] or 0)
                t_type = rec["transaction_type"] or "LOAN"
                
                if c_status in ("BREACHED", "CANCELLED", "TERMINATED", "REJECTED"):
                    await conn.execute("UPDATE bank_loan_requests SET status = 'REJECTED', updated_at = NOW() WHERE id = $1", blr_id)
                    continue
                
                # Check payment conditions for this contract
                pay_conds = await conn.fetch("""
                    SELECT status FROM contract_conditions
                    WHERE contractid = $1 AND type IN ('PAYMENT', 'LOAN_PAYOUT')
                """, c_id)
                
                if c_status in ("FULFILLED", "CLOSED"):
                    is_fulfilled = True
                elif pay_conds:
                    has_pending = any(pc["status"] == "PENDING" for pc in pay_conds)
                    has_fulfilled = any(pc["status"] == "FULFILLED" for pc in pay_conds)
                    is_fulfilled = (not has_pending) and has_fulfilled
                else:
                    is_fulfilled = False
                    
                if is_fulfilled:
                    if t_type == "DEPOSIT":
                        if amt > 0:
                            await conn.execute("UPDATE player_banks SET liquidity = COALESCE(liquidity, 0) + $1 WHERE id = $2", amt, b_id)
                        await conn.execute("UPDATE bank_loan_requests SET status = 'APPROVED', updated_at = NOW() WHERE id = $1", blr_id)
                    elif t_type == "WITHDRAWAL":
                        if amt > 0:
                            await conn.execute("UPDATE player_banks SET liquidity = GREATEST(0, COALESCE(liquidity, 0) - $1) WHERE id = $2", amt, b_id)
                        await conn.execute("UPDATE bank_loan_requests SET status = 'APPROVED', updated_at = NOW() WHERE id = $1", blr_id)
                    else: # LOAN
                        new_st = "REPAID" if c_status in ("FULFILLED", "CLOSED") else "APPROVED"
                        await conn.execute("UPDATE bank_loan_requests SET status = $1, updated_at = NOW() WHERE id = $2", new_st, blr_id)

            # 2. Check active loans (status = 'APPROVED') that reached FULFILLED or CLOSED
            if bank_id is not None:
                await conn.execute("""
                    UPDATE bank_loan_requests blr
                    SET status = 'REPAID', updated_at = NOW()
                    FROM contracts c
                    WHERE (blr.contract_id = c.localid OR blr.contract_id = c.id)
                      AND blr.status = 'APPROVED'
                      AND COALESCE(blr.transaction_type, 'LOAN') = 'LOAN'
                      AND c.status IN ('FULFILLED', 'CLOSED')
                      AND blr.bank_id = $1
                """, bank_id)
            else:
                await conn.execute("""
                    UPDATE bank_loan_requests blr
                    SET status = 'REPAID', updated_at = NOW()
                    FROM contracts c
                    WHERE (blr.contract_id = c.localid OR blr.contract_id = c.id)
                      AND blr.status = 'APPROVED'
                      AND COALESCE(blr.transaction_type, 'LOAN') = 'LOAN'
                      AND c.status IN ('FULFILLED', 'CLOSED')
                """)

    async def create_bank(
        self,
        owner_username: str,
        name: str,
        description: str,
        liquidity: float = 0.0,
        rate: float = 5.0,
        currency: str = "ICA",
        default_repay_days: int = 14,
        default_interval: str = "At Maturity",
        min_loan_amount: float = 0.0,
        max_loan_amount: float = 0.0,
        headline: Optional[str] = None,
        is_public: bool = True,
    ) -> Dict[str, Any]:
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO player_banks (
                    name, owner_username, liquidity, default_interest_rate, description,
                    currency, default_repay_days, default_interval, min_loan_amount, max_loan_amount, headline, is_public
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                RETURNING *
            """, name, owner_username, 0.0, rate, description, currency or "ICA", default_repay_days or 14, default_interval or "At Maturity", min_loan_amount or 0.0, max_loan_amount or 0.0, headline, is_public)
            return dict(row)

    async def update_bank(
        self,
        bank_id: int,
        headline: Optional[str] = None,
        description: Optional[str] = None,
        default_interest_rate: Optional[float] = None,
        public_show_loans: Optional[bool] = None,
        public_show_deposits: Optional[bool] = None,
        public_show_withdrawals: Optional[bool] = None,
        public_show_names: Optional[bool] = None,
        is_public: Optional[bool] = None,
        max_interest_rate: Optional[float] = None,
        weekly_yield_rate: Optional[float] = None,
        max_weekly_yield_rate: Optional[float] = None,
        deposit_status: Optional[str] = None,
        deposit_strategy: Optional[str] = None,
    ) -> Dict[str, Any]:
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow("""
                UPDATE player_banks
                SET headline = COALESCE($2, headline),
                    description = COALESCE($3, description),
                    default_interest_rate = COALESCE($4, default_interest_rate),
                    public_show_loans = COALESCE($5, public_show_loans),
                    public_show_deposits = COALESCE($6, public_show_deposits),
                    public_show_withdrawals = COALESCE($7, public_show_withdrawals),
                    public_show_names = COALESCE($8, public_show_names),
                    is_public = COALESCE($9, is_public),
                    max_interest_rate = COALESCE($10, max_interest_rate),
                    weekly_yield_rate = COALESCE($11, weekly_yield_rate),
                    max_weekly_yield_rate = COALESCE($12, max_weekly_yield_rate),
                    deposit_status = COALESCE($13, deposit_status),
                    deposit_strategy = COALESCE($14, deposit_strategy)
                WHERE id = $1
                RETURNING *
            """, bank_id, headline, description, default_interest_rate, public_show_loans, public_show_deposits, public_show_withdrawals, public_show_names, is_public, max_interest_rate, weekly_yield_rate, max_weekly_yield_rate, deposit_status, deposit_strategy)
            return dict(row) if row else {}

    async def get_banks(self, public_only: bool = False) -> List[Dict[str, Any]]:
        await self.sync_loan_repayments()
        async with self.db.pool.acquire() as conn:
            where_clause = "WHERE COALESCE(b.is_public, true) = true" if public_only else ""
            query = f"""
                SELECT b.*,
                       COALESCE(u.is_synchronized, false) as owner_is_synchronized,
                       u.xata_updatedat as owner_last_active,
                       (SELECT COUNT(*) FROM bank_loan_requests WHERE bank_id = b.id AND status = 'APPROVED' AND COALESCE(transaction_type, 'LOAN') = 'LOAN') as active_loans_count,
                       (SELECT COUNT(*) FROM bank_loan_requests WHERE bank_id = b.id AND COALESCE(transaction_type, 'LOAN') = 'LOAN') as total_loans_count,
                       COALESCE((SELECT SUM(amount) FROM bank_loan_requests WHERE bank_id = b.id AND COALESCE(transaction_type, 'LOAN') = 'LOAN'), 0.0) as total_lent_amount,
                       COALESCE((SELECT SUM(amount) FROM bank_loan_requests WHERE bank_id = b.id AND status = 'APPROVED' AND COALESCE(transaction_type, 'LOAN') = 'LOAN'), 0.0) as active_lent_amount,
                       COALESCE((SELECT SUM(amount) FROM bank_loan_requests WHERE bank_id = b.id AND COALESCE(transaction_type, 'LOAN') = 'DEPOSIT' AND status IN ('APPROVED', 'REPAID', 'FULFILLED')), 0.0) as total_deposit_amount,
                       COALESCE((SELECT SUM(amount) FROM bank_loan_requests WHERE bank_id = b.id AND COALESCE(transaction_type, 'LOAN') = 'WITHDRAWAL' AND status IN ('APPROVED', 'REPAID', 'FULFILLED')), 0.0) as total_withdrawal_amount,
                       COALESCE((SELECT COUNT(DISTINCT requester_username) FROM bank_loan_requests WHERE bank_id = b.id AND COALESCE(transaction_type, 'LOAN') = 'DEPOSIT'), 0) as depositors_count
                FROM player_banks b
                LEFT JOIN users u ON LOWER(u.username) = LOWER(b.owner_username)
                LEFT JOIN users_data ud ON ud.userid = u.userdataid
                {where_clause}
                ORDER BY b.created_at DESC
            """
            rows = await conn.fetch(query)
            return [dict(row) for row in rows]

    async def get_bank_by_owner(self, owner_username: str) -> Optional[Dict[str, Any]]:
        async with self.db.pool.acquire() as conn:
            bank_id = await conn.fetchval("SELECT id FROM player_banks WHERE owner_username = $1", owner_username)
        if bank_id:
            await self.sync_loan_repayments(bank_id=bank_id)
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT b.*,
                       COALESCE(u.is_synchronized, false) as owner_is_synchronized,
                       u.xata_updatedat as owner_last_active,
                       (SELECT COUNT(*) FROM bank_loan_requests WHERE bank_id = b.id AND status = 'APPROVED' AND COALESCE(transaction_type, 'LOAN') = 'LOAN') as active_loans_count,
                       (SELECT COUNT(*) FROM bank_loan_requests WHERE bank_id = b.id AND COALESCE(transaction_type, 'LOAN') = 'LOAN') as total_loans_count,
                       COALESCE((SELECT SUM(amount) FROM bank_loan_requests WHERE bank_id = b.id AND COALESCE(transaction_type, 'LOAN') = 'LOAN'), 0.0) as total_lent_amount,
                       COALESCE((SELECT SUM(amount) FROM bank_loan_requests WHERE bank_id = b.id AND status = 'APPROVED' AND COALESCE(transaction_type, 'LOAN') = 'LOAN'), 0.0) as active_lent_amount,
                       COALESCE((SELECT SUM(amount) FROM bank_loan_requests WHERE bank_id = b.id AND COALESCE(transaction_type, 'LOAN') = 'DEPOSIT' AND status IN ('APPROVED', 'REPAID', 'FULFILLED')), 0.0) as total_deposit_amount,
                       COALESCE((SELECT SUM(amount) FROM bank_loan_requests WHERE bank_id = b.id AND COALESCE(transaction_type, 'LOAN') = 'WITHDRAWAL' AND status IN ('APPROVED', 'REPAID', 'FULFILLED')), 0.0) as total_withdrawal_amount,
                       COALESCE((SELECT COUNT(DISTINCT requester_username) FROM bank_loan_requests WHERE bank_id = b.id AND COALESCE(transaction_type, 'LOAN') = 'DEPOSIT'), 0) as depositors_count
                FROM player_banks b
                LEFT JOIN users u ON LOWER(u.username) = LOWER(b.owner_username)
                LEFT JOIN users_data ud ON ud.userid = u.userdataid
                WHERE b.owner_username = $1
            """, owner_username)
            return dict(row) if row else None

    async def get_bank_by_id(self, bank_id: int) -> Optional[Dict[str, Any]]:
        await self.sync_loan_repayments(bank_id=bank_id)
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT b.*,
                       COALESCE(u.is_synchronized, false) as owner_is_synchronized,
                       u.xata_updatedat as owner_last_active,
                       (SELECT COUNT(*) FROM bank_loan_requests WHERE bank_id = b.id AND status = 'APPROVED' AND COALESCE(transaction_type, 'LOAN') = 'LOAN') as active_loans_count,
                       (SELECT COUNT(*) FROM bank_loan_requests WHERE bank_id = b.id AND COALESCE(transaction_type, 'LOAN') = 'LOAN') as total_loans_count,
                       COALESCE((SELECT SUM(amount) FROM bank_loan_requests WHERE bank_id = b.id AND COALESCE(transaction_type, 'LOAN') = 'LOAN'), 0.0) as total_lent_amount,
                       COALESCE((SELECT SUM(amount) FROM bank_loan_requests WHERE bank_id = b.id AND status = 'APPROVED' AND COALESCE(transaction_type, 'LOAN') = 'LOAN'), 0.0) as active_lent_amount,
                       COALESCE((SELECT SUM(amount) FROM bank_loan_requests WHERE bank_id = b.id AND COALESCE(transaction_type, 'LOAN') = 'DEPOSIT' AND status IN ('APPROVED', 'REPAID', 'FULFILLED')), 0.0) as total_deposit_amount,
                       COALESCE((SELECT SUM(amount) FROM bank_loan_requests WHERE bank_id = b.id AND COALESCE(transaction_type, 'LOAN') = 'WITHDRAWAL' AND status IN ('APPROVED', 'REPAID', 'FULFILLED')), 0.0) as total_withdrawal_amount,
                       COALESCE((SELECT COUNT(DISTINCT requester_username) FROM bank_loan_requests WHERE bank_id = b.id AND COALESCE(transaction_type, 'LOAN') = 'DEPOSIT'), 0) as depositors_count
                FROM player_banks b
                LEFT JOIN users u ON LOWER(u.username) = LOWER(b.owner_username)
                LEFT JOIN users_data ud ON ud.userid = u.userdataid
                WHERE b.id = $1
            """, bank_id)
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
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    lr.*, 
                    b.name as bank_name,
                    c.partner_code,
                    c.partner_name,
                    c.contract_status,
                    c.contract_duedate,
                    COALESCE(b.currency, 'ICA') as currency
                FROM bank_loan_requests lr
                JOIN player_banks b ON lr.bank_id = b.id
                LEFT JOIN LATERAL (
                    SELECT 
                        c.partnercode as partner_code,
                        c.partnername as partner_name,
                        c.status as contract_status,
                        c.duedate as contract_duedate
                    FROM contracts c
                    LEFT JOIN users u ON (u.userdataid::text = c.userid::text OR u.accountid::text = c.userid::text)
                    WHERE (c.localid = lr.contract_id OR c.id = lr.contract_id)
                    ORDER BY 
                        CASE WHEN LOWER(u.username) = LOWER(lr.requester_username) THEN 1 ELSE 2 END,
                        CASE WHEN c.status IN ('FULFILLED', 'CLOSED') THEN 1 ELSE 2 END,
                        c.date DESC
                    LIMIT 1
                ) c ON true
                WHERE lr.requester_username = $1
                ORDER BY lr.created_at DESC
            """, requester_username)
            return [dict(row) for row in rows]

    async def get_bank_loan_requests(self, bank_id: int) -> List[Dict[str, Any]]:
        await self.sync_loan_repayments(bank_id=bank_id)
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    lr.*, 
                    b.name as bank_name,
                    COALESCE(
                        CASE 
                            WHEN c.partner_code IS NOT NULL 
                                 AND LOWER(c.partner_code) != LOWER(b.name) 
                                 AND LOWER(c.partner_code) != LOWER(COALESCE(pud_bank.company_code, ''))
                                 AND LOWER(COALESCE(c.partner_name, '')) != LOWER(b.name)
                                 AND LOWER(COALESCE(c.partner_name, '')) != LOWER(COALESCE(pud_bank.company_name, ''))
                                 AND LOWER(COALESCE(c.partner_name, '')) != LOWER(b.owner_username)
                            THEN c.partner_code
                            WHEN c.user_company_code IS NOT NULL 
                                 AND LOWER(c.user_company_code) != LOWER(COALESCE(pud_bank.company_code, ''))
                            THEN c.user_company_code
                            ELSE NULL 
                        END,
                        lr.requester_username
                    ) as partner_code,
                    COALESCE(
                        CASE 
                            WHEN c.partner_name IS NOT NULL 
                                 AND LOWER(c.partner_name) != LOWER(b.name) 
                                 AND LOWER(c.partner_name) != LOWER(COALESCE(pud_bank.company_name, ''))
                                 AND LOWER(c.partner_name) != LOWER(b.owner_username)
                                 AND LOWER(COALESCE(c.partner_code, '')) != LOWER(COALESCE(pud_bank.company_code, ''))
                            THEN c.partner_name
                            WHEN c.user_company_name IS NOT NULL 
                                 AND LOWER(c.user_company_name) != LOWER(COALESCE(pud_bank.company_name, ''))
                            THEN c.user_company_name
                            WHEN c.contract_owner_username IS NOT NULL 
                                 AND LOWER(c.contract_owner_username) != LOWER(b.owner_username)
                            THEN c.contract_owner_username
                            ELSE NULL 
                        END,
                        lr.requester_username
                    ) as partner_name,
                    c.contract_status,
                    c.contract_duedate,
                    COALESCE(b.currency, 'ICA') as currency
                FROM bank_loan_requests lr
                JOIN player_banks b ON lr.bank_id = b.id
                LEFT JOIN public_users_data pud_bank ON LOWER(pud_bank.username) = LOWER(b.owner_username)
                LEFT JOIN LATERAL (
                    SELECT 
                        c.partnercode as partner_code,
                        c.partnername as partner_name,
                        c.status as contract_status,
                        c.duedate as contract_duedate,
                        u.username as contract_owner_username,
                        pud_user.company_code as user_company_code,
                        pud_user.company_name as user_company_name
                    FROM contracts c
                    LEFT JOIN users u ON (u.userdataid::text = c.userid::text OR u.accountid::text = c.userid::text)
                    LEFT JOIN public_users_data pud_user ON LOWER(pud_user.username) = LOWER(u.username)
                    WHERE (c.localid = lr.contract_id OR c.id = lr.contract_id)
                    ORDER BY 
                        CASE WHEN LOWER(u.username) = LOWER(b.owner_username) THEN 1 ELSE 2 END,
                        CASE WHEN c.partnercode = lr.requester_username OR c.partnername = lr.requester_username THEN 1 ELSE 2 END,
                        CASE WHEN c.status IN ('FULFILLED', 'CLOSED') THEN 1 ELSE 2 END,
                        c.date DESC
                    LIMIT 1
                ) c ON true
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

    async def link_contract_to_bank(
        self,
        bank_id: int,
        contract_localid: str,
        interest_rate: Optional[float] = None,
        term_days: Optional[int] = None,
        transaction_type: str = "LOAN"
    ) -> Dict[str, Any]:
        async with self.db.pool.acquire() as conn:
            bank_row = await conn.fetchrow("""
                SELECT b.default_interest_rate, b.default_repay_days, b.owner_username, b.name,
                       pud_bank.company_code as bank_owner_company_code, pud_bank.company_name as bank_owner_company_name
                FROM player_banks b
                LEFT JOIN public_users_data pud_bank ON LOWER(pud_bank.username) = LOWER(b.owner_username)
                WHERE b.id = $1
            """, bank_id)
            bank_rate = bank_row["default_interest_rate"] if bank_row else 5.0
            bank_term = bank_row["default_repay_days"] if bank_row else 14
            bank_owner = (bank_row["owner_username"] or "") if bank_row else ""

            contract_row = await conn.fetchrow("""
                SELECT 
                    c.localid, c.id, c.partnername, c.partnercode, c.status, c.date, c.duedate,
                    u.username as contract_owner_username,
                    pud_user.company_code as user_company_code,
                    pud_user.company_name as user_company_name,
                    COALESCE(
                        (
                            SELECT cc_payout.amountmoney
                            FROM contract_conditions cc_payout
                            WHERE cc_payout.contractid = c.id AND cc_payout.type IN ('LOAN_PAYOUT', 'PAYMENT')
                            LIMIT 1
                        ),
                        (
                            SELECT SUM(cli_sub.repaymentamount)
                            FROM (
                                SELECT DISTINCT ON (cli.conditionid) cli.repaymentamount
                                FROM contract_loan_installments cli
                                JOIN contract_conditions cc_sub ON cc_sub.id = cli.conditionid
                                WHERE cc_sub.contractid = c.id
                                ORDER BY cli.conditionid
                            ) cli_sub
                        ),
                        (
                            SELECT SUM(cc.amountmoney)
                            FROM contract_conditions cc
                            WHERE cc.contractid = c.id
                        ), 0.0
                    ) as total_amount,
                    COALESCE((
                        SELECT MAX(cc.currencymoney)
                        FROM contract_conditions cc
                        WHERE cc.contractid = c.id
                    ), 'ICA') as currency,
                    (
                        SELECT ROUND(SUM(cc_inst.deadlineduration_millis) / 86400000.0)::int
                        FROM (
                            SELECT DISTINCT cc_d.id, cc_d.deadlineduration_millis
                            FROM contract_conditions cc_d
                            WHERE cc_d.contractid = c.id AND cc_d.type = 'LOAN_INSTALLMENT'
                        ) cc_inst
                    ) as contract_term_days,
                    (
                        SELECT ROUND(
                            (
                                COALESCE((
                                    SELECT SUM(cli_d.interestamount)
                                    FROM (
                                        SELECT DISTINCT ON (cli.conditionid) cli.interestamount
                                        FROM contract_loan_installments cli
                                        JOIN contract_conditions cc_sub ON cc_sub.id = cli.conditionid
                                        WHERE cc_sub.contractid = c.id AND cc_sub.type = 'LOAN_INSTALLMENT'
                                        ORDER BY cli.conditionid
                                    ) cli_d
                                ), 0.0) / NULLIF(
                                    COALESCE(
                                        (SELECT amountmoney FROM contract_conditions WHERE contractid = c.id AND type = 'LOAN_PAYOUT' LIMIT 1),
                                        (
                                            SELECT SUM(cli_all.repaymentamount)
                                            FROM (
                                                SELECT DISTINCT ON (cli.conditionid) cli.repaymentamount 
                                                FROM contract_loan_installments cli
                                                JOIN contract_conditions cc_d ON cc_d.id = cli.conditionid
                                                WHERE cc_d.contractid = c.id
                                                ORDER BY cli.conditionid
                                            ) cli_all
                                        )
                                    ), 0.0
                                ) * 100.0
                            )::numeric, 3
                        )::float
                    ) as contract_interest_rate
                FROM contracts c
                LEFT JOIN users u ON (u.userdataid::text = c.userid::text OR u.accountid::text = c.userid::text)
                LEFT JOIN public_users_data pud_user ON LOWER(pud_user.username) = LOWER(u.username)
                WHERE LOWER(TRIM(c.localid)) = LOWER(TRIM($1)) OR LOWER(TRIM(c.id)) = LOWER(TRIM($1))
                ORDER BY 
                    CASE WHEN LOWER(u.username) = LOWER($2) THEN 1 ELSE 2 END,
                    c.date DESC
                LIMIT 1
            """, contract_localid.strip(), bank_owner)
            if not contract_row:
                raise ValueError(f"Contract with localid '{contract_localid}' not found")

            trans_type = transaction_type.upper().strip() if transaction_type else "LOAN"
            if trans_type not in ("LOAN", "DEPOSIT", "WITHDRAWAL"):
                trans_type = "LOAN"

            # Inspect contract payment / payout conditions
            pay_conds = await conn.fetch("""
                SELECT id, type, party, contractparty, status, amountmoney
                FROM contract_conditions
                WHERE contractid = $1 AND type IN ('PAYMENT', 'LOAN_PAYOUT')
                ORDER BY index ASC
            """, contract_row["id"])

            if contract_row["status"] in ("FULFILLED", "CLOSED"):
                is_payment_fulfilled = True
            elif pay_conds:
                has_pending = any(pc["status"] == "PENDING" for pc in pay_conds)
                has_fulfilled = any(pc["status"] == "FULFILLED" for pc in pay_conds)
                is_payment_fulfilled = (not has_pending) and has_fulfilled
            else:
                is_payment_fulfilled = False

            bank_owner_lower = bank_owner.lower()
            bank_code_lower = (bank_row["bank_owner_company_code"] or "").lower() if bank_row else ""
            bank_comp_lower = (bank_row["bank_owner_company_name"] or "").lower() if bank_row else ""
            bank_name_lower = (bank_row["name"] or "").lower() if bank_row else ""

            pcode = (contract_row["partnercode"] or "").strip()
            pname = (contract_row["partnername"] or "").strip()

            is_partner_bank = (
                (pcode and pcode.lower() == bank_code_lower) or
                (pname and (pname.lower() == bank_comp_lower or pname.lower() == bank_name_lower or pname.lower() == bank_owner_lower))
            )

            if is_partner_bank:
                c_code = (contract_row["user_company_code"] or "").strip()
                c_name = (contract_row["user_company_name"] or contract_row["contract_owner_username"] or "").strip()
            else:
                c_code = pcode
                c_name = pname or (contract_row["user_company_name"] or "").strip() or (contract_row["contract_owner_username"] or "").strip()

            if c_code and c_name and c_code.lower() != c_name.lower():
                partner = f"[{c_code}] {c_name}"
            elif c_code:
                partner = f"[{c_code}]"
            elif c_name:
                partner = c_name
            else:
                partner = "Unknown"
            amount = float(contract_row["total_amount"] or 0)
            if amount <= 0 and pay_conds:
                amount = float(sum(float(pc["amountmoney"] or 0) for pc in pay_conds))

            c_localid = contract_row["localid"] or contract_row["id"]

            if trans_type == "DEPOSIT":
                rate = 0.0
                term = 0
                if is_payment_fulfilled:
                    c_status = "APPROVED"
                    if amount > 0:
                        await conn.execute("UPDATE player_banks SET liquidity = COALESCE(liquidity, 0) + $1 WHERE id = $2", amount, bank_id)
                else:
                    c_status = "PENDING"
            elif trans_type == "WITHDRAWAL":
                rate = 0.0
                term = 0
                if is_payment_fulfilled:
                    c_status = "APPROVED"
                    if amount > 0:
                        await conn.execute("UPDATE player_banks SET liquidity = GREATEST(0, COALESCE(liquidity, 0) - $1) WHERE id = $2", amount, bank_id)
                else:
                    c_status = "PENDING"
            else:
                # LOAN
                if interest_rate is not None:
                    rate = float(interest_rate)
                elif contract_row["contract_interest_rate"] is not None:
                    rate = float(contract_row["contract_interest_rate"])
                else:
                    rate = float(bank_rate or 5.0)

                if term_days is not None:
                    term = int(term_days)
                elif contract_row["contract_term_days"] is not None and contract_row["contract_term_days"] > 0:
                    term = int(contract_row["contract_term_days"])
                elif contract_row["duedate"] and contract_row["date"]:
                    diff = (contract_row["duedate"] - contract_row["date"]).days
                    term = max(1, diff)
                else:
                    term = int(bank_term or 14)

                if contract_row["status"] in ("FULFILLED", "CLOSED"):
                    c_status = "REPAID"
                elif is_payment_fulfilled:
                    c_status = "APPROVED"
                else:
                    c_status = "PENDING"

            existing = await conn.fetchrow("""
                SELECT id FROM bank_loan_requests
                WHERE bank_id = $1 AND (contract_id = $2 OR contract_id = $3)
            """, bank_id, contract_row["localid"], contract_row["id"])
            if existing:
                raise ValueError(f"Contract '{c_localid}' is already linked to this bank")

            row = await conn.fetchrow("""
                INSERT INTO bank_loan_requests (
                    bank_id, requester_username, amount, interest_rate, term_days, status, contract_id, transaction_type, is_verified
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, true)
                RETURNING *
            """, bank_id, partner, amount, rate, term, c_status, c_localid, trans_type)
            res = dict(row)
            res["bank_name"] = await conn.fetchval("SELECT name FROM player_banks WHERE id = $1", bank_id)
            res["partner_code"] = contract_row["partnercode"]
            res["partner_name"] = contract_row["partnername"]
            res["contract_status"] = contract_row["status"]
            res["contract_duedate"] = contract_row["duedate"]
            res["currency"] = contract_row["currency"]
            res["transaction_type"] = trans_type
            res["is_verified"] = True
            return res

    async def record_manual_deposit(
        self,
        bank_id: int,
        amount: float,
        username: Optional[str] = None,
        transaction_type: str = "DEPOSIT",
        note: Optional[str] = None,
        update_liquidity: bool = True
    ) -> Dict[str, Any]:
        async with self.db.pool.acquire() as conn:
            bank_row = await conn.fetchrow("SELECT name, owner_username, currency FROM player_banks WHERE id = $1", bank_id)
            if not bank_row:
                raise ValueError("Bank not found")

            clean_user = (username or bank_row["owner_username"]).strip()
            t_type = "WITHDRAWAL" if transaction_type.upper() == "WITHDRAWAL" else "DEPOSIT"
            
            if amount <= 0:
                raise ValueError("Amount must be greater than zero")

            if update_liquidity:
                if t_type == "DEPOSIT":
                    await conn.execute("UPDATE player_banks SET liquidity = COALESCE(liquidity, 0) + $1 WHERE id = $2", amount, bank_id)
                elif t_type == "WITHDRAWAL":
                    await conn.execute("UPDATE player_banks SET liquidity = GREATEST(0, COALESCE(liquidity, 0) - $1) WHERE id = $2", amount, bank_id)

            ref_note = note.strip() if note and note.strip() else None

            row = await conn.fetchrow("""
                INSERT INTO bank_loan_requests (
                    bank_id, requester_username, amount, interest_rate, term_days, status, contract_id, transaction_type, is_verified
                )
                VALUES ($1, $2, $3, 0.0, 0, 'APPROVED', $4, $5, false)
                RETURNING *
            """, bank_id, clean_user, amount, ref_note, t_type)

            res = dict(row)
            res["bank_name"] = bank_row["name"]
            res["partner_code"] = clean_user
            res["partner_name"] = clean_user
            res["contract_status"] = "APPROVED"
            res["contract_duedate"] = None
            res["currency"] = bank_row["currency"] or "ICA"
            res["transaction_type"] = t_type
            res["is_verified"] = False
            return res

    async def import_bank_ledger_csv(self, bank_id: int, csv_content: str) -> Dict[str, Any]:
        if not csv_content or not csv_content.strip():
            return {"imported": 0, "verified": 0, "unverified": 0, "errors": ["Empty CSV content"]}

        first_line = csv_content.strip().split("\n")[0]
        delimiter = ";" if ";" in first_line and "," not in first_line else ","
        reader = csv.DictReader(io.StringIO(csv_content.strip()), delimiter=delimiter)

        imported = 0
        verified = 0
        unverified = 0
        errors = []

        async with self.db.pool.acquire() as conn:
            bank_row = await conn.fetchrow("SELECT default_interest_rate, default_repay_days FROM player_banks WHERE id = $1", bank_id)
            default_rate = float(bank_row["default_interest_rate"] or 5.0) if bank_row else 5.0
            default_term = int(bank_row["default_repay_days"] or 14) if bank_row else 14

            for row_idx, row in enumerate(reader, start=1):
                clean_row = {str(k).strip().lower(): str(v).strip() for k, v in row.items() if k is not None}
                
                user = (
                    clean_row.get("user") or 
                    clean_row.get("borrower") or 
                    clean_row.get("depositor") or 
                    clean_row.get("player") or 
                    clean_row.get("requester_username") or 
                    clean_row.get("requester")
                )
                if not user:
                    errors.append(f"Row {row_idx}: Missing username/borrower/depositor")
                    continue

                user = user.strip()
                if not (user.startswith("[") and "]" in user):
                    if 2 <= len(user) <= 6 and user.isupper():
                        p_match = await conn.fetchval(
                            "SELECT partnername FROM contracts WHERE LOWER(partnercode) = LOWER($1) AND partnername IS NOT NULL AND partnername != '' LIMIT 1",
                            user
                        )
                        if p_match:
                            user = f"[{user}] {p_match}"
                        else:
                            u_match = await conn.fetchval(
                                "SELECT company_name FROM public_users_data WHERE LOWER(company_code) = LOWER($1) AND company_name IS NOT NULL LIMIT 1",
                                user
                            )
                            if u_match:
                                user = f"[{user}] {u_match}"
                    else:
                        u_info = await conn.fetchrow(
                            "SELECT company_code, company_name FROM public_users_data WHERE LOWER(username) = LOWER($1)",
                            user
                        )
                        if u_info and u_info["company_code"] and u_info["company_name"]:
                            user = f"[{u_info['company_code']}] {u_info['company_name']}"

                amount_str = clean_row.get("amount") or clean_row.get("sum") or clean_row.get("value") or "0"
                try:
                    amount = float(amount_str.replace(",", "").replace(" ", "").replace("$", ""))
                except ValueError:
                    errors.append(f"Row {row_idx}: Invalid amount '{amount_str}'")
                    continue

                if amount <= 0:
                    errors.append(f"Row {row_idx}: Amount must be positive")
                    continue

                raw_type = clean_row.get("type") or clean_row.get("transaction_type") or "LOAN"
                raw_type_lower = raw_type.lower()
                if raw_type_lower.startswith("dep"):
                    trans_type = "DEPOSIT"
                elif raw_type_lower.startswith("with") or raw_type_lower.startswith("ret"):
                    trans_type = "WITHDRAWAL"
                else:
                    trans_type = "LOAN"

                rate_str = clean_row.get("interest_rate") or clean_row.get("rate") or clean_row.get("interest") or ""
                try:
                    rate = float(rate_str.replace("%", "").strip()) if rate_str else (default_rate if trans_type == "LOAN" else 0.0)
                except ValueError:
                    rate = default_rate if trans_type == "LOAN" else 0.0

                term_str = clean_row.get("term_days") or clean_row.get("term") or clean_row.get("days") or ""
                try:
                    term = int(term_str) if term_str else (default_term if trans_type == "LOAN" else 0)
                except ValueError:
                    term = default_term if trans_type == "LOAN" else 0

                raw_status = (clean_row.get("status") or "APPROVED").upper()
                if raw_status in ("REPAID", "COMPLETED", "FULFILLED", "CLOSED"):
                    status = "REPAID" if trans_type == "LOAN" else "APPROVED"
                elif raw_status in ("PENDING",):
                    status = "PENDING"
                elif raw_status in ("REJECTED", "CANCELLED"):
                    status = "REJECTED"
                else:
                    status = "APPROVED"

                date_str = clean_row.get("date") or clean_row.get("created_at") or clean_row.get("timestamp")
                row_date = None
                if date_str:
                    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d.%m.%Y", "%m/%d/%Y"):
                        try:
                            row_date = datetime.strptime(date_str, fmt)
                            break
                        except ValueError:
                            pass

                contract_localid = clean_row.get("contract_localid") or clean_row.get("contract_id") or clean_row.get("localid")
                is_row_verified = False
                matched_contract_id = None

                if contract_localid:
                    contract_row = await conn.fetchrow("""
                        SELECT id, localid, status FROM contracts 
                        WHERE localid = $1 OR id = $1
                        LIMIT 1
                    """, contract_localid)
                    if contract_row:
                        is_row_verified = True
                        matched_contract_id = contract_row["localid"] or contract_row["id"]
                        if trans_type == "LOAN" and contract_row["status"] in ("FULFILLED", "CLOSED"):
                            status = "REPAID"
                    else:
                        matched_contract_id = contract_localid

                if row_date:
                    await conn.execute("""
                        INSERT INTO bank_loan_requests (
                            bank_id, requester_username, amount, interest_rate, term_days,
                            status, contract_id, transaction_type, is_verified, created_at, updated_at
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)
                    """, bank_id, user, amount, rate, term, status, matched_contract_id, trans_type, is_row_verified, row_date)
                else:
                    await conn.execute("""
                        INSERT INTO bank_loan_requests (
                            bank_id, requester_username, amount, interest_rate, term_days,
                            status, contract_id, transaction_type, is_verified
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """, bank_id, user, amount, rate, term, status, matched_contract_id, trans_type, is_row_verified)

                if status in ("APPROVED", "REPAID", "FULFILLED"):
                    if trans_type == "DEPOSIT":
                        await conn.execute("UPDATE player_banks SET liquidity = COALESCE(liquidity, 0) + $1 WHERE id = $2", amount, bank_id)
                    elif trans_type == "WITHDRAWAL":
                        await conn.execute("UPDATE player_banks SET liquidity = GREATEST(0, COALESCE(liquidity, 0) - $1) WHERE id = $2", amount, bank_id)
                    elif trans_type == "LOAN" and status == "APPROVED":
                        await conn.execute("UPDATE player_banks SET liquidity = GREATEST(0, COALESCE(liquidity, 0) - $1) WHERE id = $2", amount, bank_id)

                imported += 1
                if is_row_verified:
                    verified += 1
                else:
                    unverified += 1

        return {
            "imported": imported,
            "verified": verified,
            "unverified": unverified,
            "errors": errors
        }

    async def get_bank_depositors(self, bank_id: int) -> Dict[str, Any]:
        await self.sync_loan_repayments(bank_id=bank_id)
        async with self.db.pool.acquire() as conn:
            bank_row = await conn.fetchrow("""
                SELECT id, name, owner_username, liquidity, default_interest_rate, weekly_yield_rate, last_yield_distribution, created_at
                FROM player_banks
                WHERE id = $1
            """, bank_id)
            if not bank_row:
                raise ValueError("Bank not found")

            owner_username = bank_row["owner_username"]
            weekly_rate = float(bank_row["weekly_yield_rate"] or 1.5)
            created_at = bank_row["created_at"]
            if created_at and created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            
            now = datetime.now(timezone.utc)
            # Unified global 7-day weekly cycle (anchored to Monday 00:00 UTC across all banks)
            cycle_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
            cycle_duration = timedelta(days=7)
            next_distribution = cycle_start + cycle_duration

            time_elapsed = now - cycle_start
            days_elapsed = max(0.0, min(7.0, time_elapsed.total_seconds() / 86400.0))
            cycle_progress_percent = round(min(100.0, (days_elapsed / 7.0) * 100.0), 1)

            rows = await conn.fetch("""
                SELECT 
                    requester_username,
                    amount,
                    transaction_type,
                    status,
                    created_at,
                    contract_id
                FROM bank_loan_requests
                WHERE bank_id = $1 
                  AND transaction_type IN ('DEPOSIT', 'WITHDRAWAL', 'YIELD')
                  AND status != 'REJECTED'
                ORDER BY created_at ASC
            """, bank_id)

            depositors: Dict[str, Dict[str, Any]] = {}
            for row in rows:
                user = row["requester_username"]
                amt = float(row["amount"] or 0)
                t_type = row["transaction_type"]
                r_status = row["status"]
                created_dt = row["created_at"]
                if created_dt and created_dt.tzinfo is None:
                    created_dt = created_dt.replace(tzinfo=timezone.utc)
                created_iso = created_dt.isoformat() if created_dt else now.isoformat()

                if user not in depositors:
                    depositors[user] = {
                        "username": user,
                        "total_deposited": 0.0,
                        "total_withdrawn": 0.0,
                        "current_balance": 0.0,
                        "starting_balance_at_cycle_start": 0.0,
                        "net_deposits_in_cycle": 0.0,
                        "in_cycle_dollar_days": 0.0,
                        "first_deposit_date": created_iso,
                        "first_deposit_dt": created_dt or now,
                        "last_activity_date": created_iso,
                        "transactions_count": 0,
                        "history": [],
                        "weekly_snapshots": [],
                        "is_owner": (user.lower() == owner_username.lower())
                    }

                dep = depositors[user]
                dep["transactions_count"] += 1
                dep["last_activity_date"] = created_iso

                is_settled = r_status in ("APPROVED", "REPAID", "FULFILLED")
                if not is_settled:
                    dep["history"].append({
                        "date": created_iso,
                        "type": t_type,
                        "amount": amt,
                        "balance": dep["current_balance"],
                        "status": r_status
                    })
                    continue

                if t_type in ("DEPOSIT", "YIELD"):
                    dep["total_deposited"] += amt
                    dep["current_balance"] += amt
                elif t_type == "WITHDRAWAL":
                    dep["total_withdrawn"] += amt
                    dep["current_balance"] = max(0.0, dep["current_balance"] - amt)

                # Time-weighted tracking for current cycle
                if created_dt and created_dt < cycle_start:
                    if t_type in ("DEPOSIT", "YIELD"):
                        dep["starting_balance_at_cycle_start"] += amt
                    elif t_type == "WITHDRAWAL":
                        dep["starting_balance_at_cycle_start"] = max(0.0, dep["starting_balance_at_cycle_start"] - amt)
                else:
                    c_dt = created_dt or now
                    rem_secs = max(0.0, (next_distribution - c_dt).total_seconds())
                    days_remaining = max(0.0, min(7.0, rem_secs / 86400.0))
                    if t_type in ("DEPOSIT", "YIELD"):
                        dep["net_deposits_in_cycle"] += amt
                        dep["in_cycle_dollar_days"] += amt * days_remaining
                    elif t_type == "WITHDRAWAL":
                        dep["net_deposits_in_cycle"] -= amt
                        dep["in_cycle_dollar_days"] -= amt * days_remaining

                dep["history"].append({
                    "date": created_iso,
                    "type": t_type,
                    "amount": amt,
                    "balance": dep["current_balance"],
                    "status": "SETTLED"
                })

            total_pool = sum(d["current_balance"] for d in depositors.values())
            owner_equity = sum(d["current_balance"] for d in depositors.values() if d["is_owner"])
            client_deposits = max(0.0, total_pool - owner_equity)

            # Query historical weekly snapshots for each depositor
            dep_snap_rows = await conn.fetch("""
                SELECT username, week_date, starting_balance, net_deposits, effective_capital, yield_earned, closing_balance
                FROM bank_depositor_weekly_snapshots
                WHERE bank_id = $1
                ORDER BY week_date ASC
            """, bank_id)
            user_past_snaps: Dict[str, list] = {}
            for sr in dep_snap_rows:
                u = sr["username"]
                if u not in user_past_snaps:
                    user_past_snaps[u] = []
                user_past_snaps[u].append({
                    "week_date": sr["week_date"].strftime("%Y-%m-%d"),
                    "starting_balance": float(sr["starting_balance"] or 0),
                    "net_deposits": float(sr["net_deposits"] or 0),
                    "effective_capital": float(sr["effective_capital"] or 0),
                    "yield_earned": float(sr["yield_earned"] or 0),
                    "closing_balance": float(sr["closing_balance"] or 0),
                    "is_current": False
                })

            now_iso = now.isoformat()
            total_projected_payout = 0.0

            for dep in depositors.values():
                bal = dep["current_balance"]
                u = dep["username"]
                dep["equity_share_percent"] = round((bal / total_pool * 100.0), 2) if total_pool > 0 else 0.0

                start_bal = dep["starting_balance_at_cycle_start"]
                total_dollar_days = (start_bal * 7.0) + dep["in_cycle_dollar_days"]
                effective_capital = max(0.0, total_dollar_days / 7.0)

                if effective_capital <= 0.01 and bal > 0.01:
                    first_dt = dep.get("first_deposit_dt", now)
                    if first_dt <= cycle_start:
                        days_active = 7.0
                    else:
                        active_secs = (next_distribution - first_dt).total_seconds()
                        days_active = max(0.0, min(7.0, active_secs / 86400.0))
                    proration_factor = round(days_active / 7.0, 3)
                    effective_capital = bal * proration_factor
                else:
                    days_active = round(min(7.0, (effective_capital / bal) * 7.0), 1) if bal > 0 else 7.0
                    proration_factor = round(min(1.0, effective_capital / bal), 3) if bal > 0 else 1.0

                is_owner = dep.get("is_owner", False)
                if is_owner:
                    payout = 0.0
                    dep["days_active_in_cycle"] = days_active
                    dep["cycle_proration_factor"] = proration_factor
                    dep["weekly_yield_rate"] = 0.0
                    dep["effective_capital"] = round(effective_capital, 2)
                    dep["projected_weekly_payout"] = 0.0
                else:
                    payout = round(effective_capital * (weekly_rate / 100.0), 2)
                    dep["days_active_in_cycle"] = days_active
                    dep["cycle_proration_factor"] = proration_factor
                    dep["weekly_yield_rate"] = weekly_rate
                    dep["effective_capital"] = round(effective_capital, 2)
                    dep["projected_weekly_payout"] = payout
                    total_projected_payout += payout

                dep["weekly_snapshots"] = list(user_past_snaps.get(u, []))
                dep["weekly_snapshots"].append({
                    "week_date": cycle_start.strftime("%Y-%m-%d"),
                    "starting_balance": round(start_bal, 2),
                    "net_deposits": round(dep["net_deposits_in_cycle"], 2),
                    "effective_capital": round(effective_capital, 2),
                    "yield_earned": payout,
                    "closing_balance": round(bal, 2),
                    "projected_closing_balance": round(bal + payout, 2),
                    "is_current": True
                })

                if dep["history"]:
                    dep["history"].append({
                        "date": now_iso,
                        "type": "CURRENT",
                        "amount": 0.0,
                        "balance": bal
                    })

                dep.pop("first_deposit_dt", None)
                dep.pop("starting_balance_at_cycle_start", None)
                dep.pop("net_deposits_in_cycle", None)
                dep.pop("in_cycle_dollar_days", None)

            # Query historical bank weekly snapshots
            bank_snap_rows = await conn.fetch("""
                SELECT week_date, total_pool, owner_equity, client_deposits, total_yield_distributed, depositors_count
                FROM bank_weekly_snapshots
                WHERE bank_id = $1
                ORDER BY week_date ASC
            """, bank_id)
            weekly_history = []
            for bsr in bank_snap_rows:
                weekly_history.append({
                    "week_date": bsr["week_date"].strftime("%Y-%m-%d"),
                    "total_pool": float(bsr["total_pool"] or 0),
                    "owner_equity": float(bsr["owner_equity"] or 0),
                    "client_deposits": float(bsr["client_deposits"] or 0),
                    "total_yield_distributed": float(bsr["total_yield_distributed"] or 0),
                    "depositors_count": int(bsr["depositors_count"] or 0),
                    "is_current": False
                })
            weekly_history.append({
                "week_date": cycle_start.strftime("%Y-%m-%d"),
                "total_pool": round(total_pool, 2),
                "owner_equity": round(owner_equity, 2),
                "client_deposits": round(client_deposits, 2),
                "total_yield_distributed": round(total_projected_payout, 2),
                "depositors_count": len(depositors),
                "is_current": True
            })

            result_depositors = list(depositors.values())
            result_depositors.sort(key=lambda x: (not x["is_owner"], -x["current_balance"]))

            metrics = {
                "total_pool": round(total_pool, 2),
                "owner_equity": round(owner_equity, 2),
                "owner_equity_percent": round((owner_equity / total_pool * 100.0), 1) if total_pool > 0 else 0.0,
                "client_deposits": round(client_deposits, 2),
                "client_deposits_percent": round((client_deposits / total_pool * 100.0), 1) if total_pool > 0 else 0.0,
                "weekly_yield_rate": weekly_rate,
                "cycle_start_date": cycle_start.isoformat(),
                "next_distribution_date": next_distribution.isoformat(),
                "days_elapsed_in_cycle": round(days_elapsed, 1),
                "cycle_progress_percent": cycle_progress_percent,
                "est_weekly_payout_pool": round(total_projected_payout, 2),
                "weekly_history": weekly_history
            }

            return {
                "metrics": metrics,
                "depositors": result_depositors
            }

    async def process_automatic_weekly_yields(self) -> Dict[str, Any]:
        """
        Background worker task that checks every bank.
        If a bank has reached its 7-day mark (>= 7 days since last_yield_distribution),
        it automatically credits all depositors (owner and clients) with their prorated yield,
        and resets the 7-day cycle to 0.
        """
        await self.sync_loan_repayments()
        now = datetime.now(timezone.utc)
        current_week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        results = []

        async with self.db.pool.acquire() as conn:
            banks = await conn.fetch("""
                SELECT id, name, last_yield_distribution, created_at, weekly_yield_rate
                FROM player_banks
            """)

            for b in banks:
                bank_id = b["id"]
                last_dist = b["last_yield_distribution"]
                if last_dist and last_dist.tzinfo is None:
                    last_dist = last_dist.replace(tzinfo=timezone.utc)

                if last_dist is not None and last_dist >= current_week_start:
                    continue

                if last_dist is None:
                    created_at = b["created_at"]
                    if created_at and created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=timezone.utc)
                    if created_at and created_at >= current_week_start:
                        continue

                try:
                    cycle_start = last_dist or (current_week_start - timedelta(days=7))
                    dist_result = await self._execute_bank_weekly_yield(conn, bank_id, cycle_start, current_week_start)
                    await conn.execute("UPDATE player_banks SET last_yield_distribution = $1 WHERE id = $2", current_week_start, bank_id)
                    results.append(dist_result)
                except Exception as e:
                    results.append({"bank_id": bank_id, "error": str(e)})

        return {"processed": len(results), "details": results}

    async def _execute_bank_weekly_yield(self, conn, bank_id: int, cycle_start: datetime, now: datetime) -> Dict[str, Any]:
        bank_row = await conn.fetchrow("""
            SELECT name, owner_username, weekly_yield_rate
            FROM player_banks
            WHERE id = $1
        """, bank_id)
        if not bank_row:
            return {"bank_id": bank_id, "status": "Bank not found"}

        weekly_rate = float(bank_row["weekly_yield_rate"] or 1.5)
        week_date = cycle_start.date()
        week_label = cycle_start.strftime("%Y-%m-%d")

        rows = await conn.fetch("""
            SELECT requester_username, amount, transaction_type, created_at
            FROM bank_loan_requests
            WHERE bank_id = $1
              AND transaction_type IN ('DEPOSIT', 'WITHDRAWAL', 'YIELD')
              AND status IN ('APPROVED', 'REPAID', 'FULFILLED')
              AND created_at <= $2
            ORDER BY created_at ASC
        """, bank_id, now)

        user_data: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            u = r["requester_username"]
            amt = float(r["amount"] or 0)
            t_type = r["transaction_type"]
            c_dt = r["created_at"]
            if c_dt and c_dt.tzinfo is None:
                c_dt = c_dt.replace(tzinfo=timezone.utc)

            if u not in user_data:
                user_data[u] = {
                    "starting_balance": 0.0,
                    "net_deposits": 0.0,
                    "in_cycle_dollar_days": 0.0,
                }

            ud = user_data[u]
            if c_dt < cycle_start:
                if t_type in ("DEPOSIT", "YIELD"):
                    ud["starting_balance"] += amt
                elif t_type == "WITHDRAWAL":
                    ud["starting_balance"] = max(0.0, ud["starting_balance"] - amt)
            else:
                rem_secs = max(0.0, (now - c_dt).total_seconds())
                days_held = min(7.0, rem_secs / 86400.0)
                if t_type in ("DEPOSIT", "YIELD"):
                    ud["net_deposits"] += amt
                    ud["in_cycle_dollar_days"] += amt * days_held
                elif t_type == "WITHDRAWAL":
                    ud["net_deposits"] -= amt
                    ud["in_cycle_dollar_days"] -= amt * days_held

        total_distributed = 0.0
        payouts_count = 0
        total_pool_closing = 0.0
        owner_closing = 0.0
        active_depositors_count = 0

        async with conn.transaction():
            for u, data in user_data.items():
                start_bal = data["starting_balance"]
                net_dep = data["net_deposits"]

                total_dollar_days = (start_bal * 7.0) + data["in_cycle_dollar_days"]
                effective_capital = max(0.0, total_dollar_days / 7.0)

                is_owner = (u.lower() == bank_row["owner_username"].lower())
                payout = 0.0 if is_owner else round(effective_capital * (weekly_rate / 100.0), 2)
                closing_bal = round(max(0.0, start_bal + net_dep + payout), 2)

                if start_bal <= 0.01 and net_dep <= 0.01 and payout <= 0.01:
                    continue

                active_depositors_count += 1
                total_pool_closing += closing_bal
                if u.lower() == bank_row["owner_username"].lower():
                    owner_closing += closing_bal

                if payout > 0.01:
                    note = f"Weekly Yield ({round(effective_capital, 2)} cap @ {weekly_rate}%) - Wk {week_label}"
                    await conn.execute("""
                        INSERT INTO bank_loan_requests (
                            bank_id, requester_username, amount, interest_rate, term_days,
                            status, contract_id, transaction_type, is_verified, created_at, updated_at
                        )
                        VALUES ($1, $2, $3, 0.0, 0, 'APPROVED', $4, 'YIELD', false, $5, $5)
                    """, bank_id, u, payout, note, now)
                    total_distributed += payout
                    payouts_count += 1

                # Save depositor weekly snapshot
                await conn.execute("""
                    INSERT INTO bank_depositor_weekly_snapshots (
                        bank_id, username, week_date, starting_balance, net_deposits,
                        effective_capital, yield_earned, closing_balance, created_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (bank_id, username, week_date) DO UPDATE SET
                        starting_balance = EXCLUDED.starting_balance,
                        net_deposits = EXCLUDED.net_deposits,
                        effective_capital = EXCLUDED.effective_capital,
                        yield_earned = EXCLUDED.yield_earned,
                        closing_balance = EXCLUDED.closing_balance
                """, bank_id, u, week_date, round(start_bal, 2), round(net_dep, 2), round(effective_capital, 2), payout, closing_bal, now)

            client_closing = max(0.0, total_pool_closing - owner_closing)
            # Save bank-level weekly snapshot
            await conn.execute("""
                INSERT INTO bank_weekly_snapshots (
                    bank_id, week_date, total_pool, owner_equity, client_deposits,
                    total_yield_distributed, depositors_count, created_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (bank_id, week_date) DO UPDATE SET
                    total_pool = EXCLUDED.total_pool,
                    owner_equity = EXCLUDED.owner_equity,
                    client_deposits = EXCLUDED.client_deposits,
                    total_yield_distributed = EXCLUDED.total_yield_distributed,
                    depositors_count = EXCLUDED.depositors_count
            """, bank_id, week_date, round(total_pool_closing, 2), round(owner_closing, 2), round(client_closing, 2), round(total_distributed, 2), active_depositors_count, now)

            await conn.execute("""
                UPDATE player_banks
                SET last_yield_distribution = $1
                WHERE id = $2
            """, now, bank_id)

        return {
            "bank_id": bank_id,
            "bank_name": bank_row["name"],
            "total_distributed": total_distributed,
            "payouts_count": payouts_count,
            "reset_cycle_time": now.isoformat()
        }
