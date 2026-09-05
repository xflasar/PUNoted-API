import datetime
import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class ContractNotificationEvaluator:
    """
    Evaluates contract state transitions, payment deadlines, and loan condition triggers.

    Leverages PostgreSQL predicate pushdown and indexed interval arithmetic
    (CURRENT_TIMESTAMP +/- INTERVAL window filtering) to evaluate contract status 
    candidates directly at the database engine layer, avoiding memory-intensive row 
    hydration and client-side iterative filtering.
    """

    def __init__(self, conn, create_notif_fn):
        self.conn = conn
        self.create_notif_fn = create_notif_fn

    async def evaluate_user_contracts(self, user_accountid: str, userdata_id: Optional[str] = None):
        if not userdata_id:
            userdata_id = user_accountid

        today_str = datetime.date.today().isoformat()
        now = datetime.datetime.now(datetime.timezone.utc)

        # 1. High-Performance SQL Query: Overall Due Contracts (Filtered to 3d window directly in DB)
        due_contracts = await self.conn.fetch(
            """
            SELECT id, localid, name, status, duedate, party, partnerid, partnername, partnercode, contracttype, preamble
            FROM contracts
            WHERE (userid = $1 OR userid = $2 OR party = $1 OR party = $2)
              AND status IN ('OPEN', 'IN_PROGRESS', 'ACTIVE', 'PENDING', 'ACCEPTED')
              AND duedate IS NOT NULL
              AND duedate <= CURRENT_TIMESTAMP + INTERVAL '3 days'
              AND duedate >= CURRENT_TIMESTAMP - INTERVAL '1 day';
            """,
            user_accountid, userdata_id
        )

        for c in due_contracts:
            cid = c["id"]
            c_type = (c.get("contracttype") or "").upper()
            preamble = (c.get("preamble") or "").lower()
            is_loan = "LOAN" in c_type or "CREDIT" in c_type or "FINANCING" in c_type or "loan" in preamble or "repayment" in preamble
            is_lender = (c.get("party") == user_accountid or c.get("party") == userdata_id)
            partner_name = c.get("partnername") or c.get("partnercode") or "Counterparty"

            due_dt = c["duedate"]
            if isinstance(due_dt, str):
                try:
                    due_dt = datetime.datetime.fromisoformat(due_dt.replace("Z", "+00:00"))
                except Exception:
                    due_dt = None

            if due_dt:
                if due_dt.tzinfo is None:
                    due_dt = due_dt.replace(tzinfo=datetime.timezone.utc)
                
                days_remaining = (due_dt - now).days

                if -1 <= days_remaining <= 3:
                    is_overdue = days_remaining < 0
                    days_label = "OVERDUE!" if is_overdue else ("today" if days_remaining == 0 else f"in {days_remaining} day(s)")
                    dedup = f"contract_due_{cid}_{days_remaining}d_{today_str}"
                    
                    if is_loan:
                        title = f"Loan Due: {partner_name}"
                        if is_lender:
                            msg = f"Hey! Loan payment from {partner_name} is due {days_label} (#{c.get('localid') or cid[:8]})."
                        else:
                            msg = f"Reminder: Loan installment payment to {partner_name} is due {days_label} (#{c.get('localid') or cid[:8]})."
                    else:
                        title = f"Contract Due Soon: #{c.get('localid') or 'Contract'}"
                        msg = f"Contract #{c.get('localid') or ''} ({c.get('name') or 'Active Contract'}) with {partner_name} is due {days_label}."

                    await self.create_notif_fn(
                        self.conn, user_accountid, "contract", "contract_due",
                        title, msg, dedup_key=dedup,
                        data={"contractid": cid, "days_remaining": days_remaining, "is_loan": is_loan}
                    )

        # 2. High-Performance SQL Query: Specific Conditions & Loan Installments Due (Using contract_loan_installments table)
        due_conditions = await self.conn.fetch(
            """
            SELECT cc.id as cond_id, cc.contractid, cc.type as cond_type, cc.status as cond_status,
                   cc.party as cond_party, cc.deadline, cc.amountmoney, cc.currencymoney,
                   cli.interestamount, cli.repaymentamount, cli.totalamount as installment_totalamount, cli.currency as installment_currency,
                   c.localid as contract_localid, c.name as contract_name, c.contracttype,
                   c.preamble, c.party as contract_party, c.partnername, c.partnercode
            FROM contract_conditions cc
            JOIN contracts c ON cc.contractid = c.id
            LEFT JOIN contract_loan_installments cli ON cc.id = cli.conditionid
            WHERE (c.userid = $1 OR c.userid = $2 OR c.party = $1 OR c.party = $2)
              AND c.status IN ('OPEN', 'IN_PROGRESS', 'ACTIVE', 'PENDING', 'ACCEPTED')
              AND cc.status NOT IN ('FULFILLED', 'CANCELLED', 'REJECTED')
              AND cc.deadline IS NOT NULL
              AND cc.deadline <= CURRENT_TIMESTAMP + INTERVAL '3 days'
              AND cc.deadline >= CURRENT_TIMESTAMP - INTERVAL '1 day';
            """,
            user_accountid, userdata_id
        )

        for cond in due_conditions:
            cid = cond["contractid"]
            cond_id = cond["cond_id"]
            cond_type = cond.get("cond_type") or "CONDITION"
            c_type = (cond.get("contracttype") or "").upper()
            preamble = (cond.get("preamble") or "").lower()
            is_loan = "LOAN" in c_type or "CREDIT" in c_type or "FINANCING" in c_type or "loan" in preamble or "repayment" in preamble
            is_lender = (cond.get("contract_party") == user_accountid or cond.get("contract_party") == userdata_id)
            partner_name = cond.get("partnername") or cond.get("partnercode") or "Counterparty"

            cond_deadline = cond["deadline"]
            if isinstance(cond_deadline, str):
                try:
                    cond_dt = datetime.datetime.fromisoformat(cond_deadline.replace("Z", "+00:00"))
                except Exception:
                    cond_dt = None
            else:
                cond_dt = cond_deadline

            if cond_dt:
                if cond_dt.tzinfo is None:
                    cond_dt = cond_dt.replace(tzinfo=datetime.timezone.utc)

                c_days = (cond_dt - now).days

                if -1 <= c_days <= 3:
                    cond_amount = cond.get("installment_totalamount") or cond.get("repaymentamount") or cond.get("amountmoney") or 0
                    cond_currency = cond.get("installment_currency") or cond.get("currencymoney") or "ICA"
                    is_overdue = c_days < 0
                    time_label = "OVERDUE!" if is_overdue else ("today" if c_days == 0 else f"in {c_days} day(s)")

                    dedup = f"condition_due_{cond_id}_{c_days}d_{today_str}"

                    if is_loan or "PAYMENT" in cond_type or "REPAYMENT" in cond_type:
                        if is_lender:
                            title = f"Loan Payment Due ({partner_name})"
                            msg = f"Hey! Loan payment of {cond_amount:,.2f} {cond_currency} from {partner_name} is due {time_label}!"
                        else:
                            title = f"Loan Payment Reminder ({partner_name})"
                            msg = f"Reminder: Loan installment of {cond_amount:,.2f} {cond_currency} to {partner_name} is due {time_label}!"
                    else:
                        title = f"Condition Due: {cond_type.replace('_', ' ')}"
                        msg = f"Condition '{cond_type.replace('_', ' ')}' for contract #{cond.get('contract_localid') or cid[:8]} ({cond.get('contract_name') or 'Contract'}) is {time_label}."

                    await self.create_notif_fn(
                        self.conn, user_accountid, "contract", "condition_due",
                        title, msg, dedup_key=dedup,
                        data={
                            "contractid": cid,
                            "conditionid": cond_id,
                            "condition_type": cond_type,
                            "amount": cond_amount,
                            "currency": cond_currency,
                            "days_remaining": c_days
                        }
                    )
