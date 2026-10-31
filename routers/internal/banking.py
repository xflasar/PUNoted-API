from typing import List, Optional
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request

from app.core.security import require_internal_origin
from auth import get_current_user_id
from models.banking_schema import (
    BankCreate,
    BankUpdate,
    BankResponse,
    LoanRequestCreate,
    LoanRequestResponse,
    LoanRequestAction,
    LoanLinkContract,
    ManualDepositPayload,
    BankCsvImportPayload,
    BankCsvImportResponse,
    BankDepositorsResponse,
)
from repositories.banking_repo import BankingRepository
from services.internal.banking_service import BankingService
from managers.global_ws_manager import global_ws_manager

banking_router = APIRouter(tags=["Banking"], dependencies=[Depends(require_internal_origin)])


def get_banking_service(request: Request) -> BankingService:
    return BankingService(BankingRepository(request.app.state.db))


async def get_username_from_user_id(conn, user_id: str) -> Optional[str]:
    return await conn.fetchval(
        "SELECT username FROM users WHERE accountid::text = $1 OR userdataid::text = $1", str(user_id)
    )


@banking_router.post("", response_model=BankResponse)
async def create_bank(
    data: BankCreate,
    user_id: str = Depends(get_current_user_id),
    service: BankingService = Depends(get_banking_service)
):
    async with service.repo.db.pool.acquire() as conn:
        owner_username = await get_username_from_user_id(conn, user_id)
        if not owner_username:
            raise HTTPException(status_code=404, detail="User username not found")

        existing = await conn.fetchrow(
            "SELECT id FROM player_banks WHERE LOWER(owner_username) = LOWER($1)", owner_username
        )
        if existing:
            raise HTTPException(status_code=400, detail="You already have an established bank")

        name_check = await conn.fetchrow(
            "SELECT id FROM player_banks WHERE LOWER(name) = LOWER($1)", data.name
        )
        if name_check:
            raise HTTPException(status_code=400, detail="A bank with this name already exists")

    created_bank = await service.create_bank(
        owner_username=owner_username,
        name=data.name,
        description=data.description or "",
        liquidity=0.0,
        rate=data.default_interest_rate,
        currency=data.currency or "ICA",
        default_repay_days=data.default_repay_days or 14,
        default_interval=data.default_interval or "At Maturity",
        min_loan_amount=data.min_loan_amount or 0.0,
        max_loan_amount=data.max_loan_amount or 0.0,
        headline=data.headline,
        is_public=data.is_public if data.is_public is not None else True,
    )
    await global_ws_manager.broadcast("banking_updates", {"type": "BANKING_DATA_UPDATE", "event": "BANK_CREATED"})
    return created_bank


@banking_router.put("/{bank_id:int}", response_model=BankResponse)
async def update_bank(
    bank_id: int,
    data: BankUpdate,
    user_id: str = Depends(get_current_user_id),
    service: BankingService = Depends(get_banking_service)
):
    async with service.repo.db.pool.acquire() as conn:
        username = await get_username_from_user_id(conn, user_id)
        owner_username = await conn.fetchval("SELECT owner_username FROM player_banks WHERE id = $1", bank_id)
        if not owner_username:
            raise HTTPException(status_code=404, detail="Bank not found")
        if not username or username.lower() != owner_username.lower():
            raise HTTPException(status_code=403, detail="You do not own this bank")

    updated_bank = await service.update_bank(
        bank_id=bank_id,
        headline=data.headline,
        description=data.description,
        default_interest_rate=data.default_interest_rate,
        public_show_loans=data.public_show_loans,
        public_show_deposits=data.public_show_deposits,
        public_show_withdrawals=data.public_show_withdrawals,
        public_show_names=data.public_show_names,
        is_public=data.is_public,
        max_interest_rate=data.max_interest_rate,
        weekly_yield_rate=data.weekly_yield_rate,
        max_weekly_yield_rate=data.max_weekly_yield_rate,
        deposit_status=data.deposit_status,
        deposit_strategy=data.deposit_strategy,
    )
    await global_ws_manager.broadcast("banking_updates", {"type": "BANKING_DATA_UPDATE", "event": "BANK_UPDATED", "bank_id": bank_id})
    return updated_bank


@banking_router.get("", response_model=List[BankResponse])
async def list_banks(
    service: BankingService = Depends(get_banking_service)
):
    return await service.get_banks()


@banking_router.get("/my-bank", response_model=Optional[BankResponse])
async def get_my_bank(
    user_id: str = Depends(get_current_user_id),
    service: BankingService = Depends(get_banking_service)
):
    async with service.repo.db.pool.acquire() as conn:
        owner_username = await get_username_from_user_id(conn, user_id)
        if not owner_username:
            return None
    return await service.get_bank_by_owner(owner_username)


@banking_router.get("/{bank_id:int}", response_model=Optional[BankResponse])
async def get_bank_by_id(
    bank_id: int,
    service: BankingService = Depends(get_banking_service)
):
    bank = await service.get_bank_by_id(bank_id)
    if not bank:
        raise HTTPException(status_code=404, detail="Bank not found")
    return bank


@banking_router.get("/{bank_id:int}/ledger")
async def get_public_bank_ledger(
    bank_id: int,
    service: BankingService = Depends(get_banking_service)
):
    bank = await service.get_bank_by_id(bank_id)
    if not bank:
        raise HTTPException(status_code=404, detail="Bank not found")

    if bank.get("is_public") is False:
        raise HTTPException(status_code=404, detail="Bank is private and not available publicly")

    show_loans = bank.get("public_show_loans") if bank.get("public_show_loans") is not None else True
    show_deposits = bank.get("public_show_deposits") if bank.get("public_show_deposits") is not None else True
    show_withdrawals = bank.get("public_show_withdrawals") if bank.get("public_show_withdrawals") is not None else True
    show_names = bool(bank.get("public_show_names"))

    records = await service.get_bank_loan_requests(bank_id)

    sanitized = []
    total_lent = 0.0
    total_repaid = 0.0
    active_lent = 0.0
    total_deposits = 0.0
    total_withdrawals = 0.0
    loans_count = 0
    deposits_count = 0

    for r in records:
        tx_type = r.get("transaction_type") or "LOAN"
        status = r.get("status") or "PENDING"
        amount = float(r.get("amount") or 0.0)

        # Banker visibility filters
        if tx_type == "LOAN" and not show_loans:
            continue
        if tx_type == "DEPOSIT" and not show_deposits:
            continue
        if tx_type == "WITHDRAWAL" and not show_withdrawals:
            continue

        if tx_type == "LOAN":
            loans_count += 1
            if status in ("APPROVED", "REPAID"):
                total_lent += amount
            if status == "REPAID":
                total_repaid += amount
            elif status == "APPROVED":
                active_lent += amount
        elif tx_type == "DEPOSIT" and status != "REJECTED":
            deposits_count += 1
            total_deposits += amount
        elif tx_type == "WITHDRAWAL" and status != "REJECTED":
            total_withdrawals += amount

        # Handle counterparty name visibility (anonymize if hidden)
        raw_user = (r.get("requester_username") or "").strip()
        p_code = (r.get("partner_code") or "").strip()
        p_name = (r.get("partner_name") or "").strip()

        if raw_user.startswith("[") and "]" in raw_user:
            resolved_name = raw_user
        elif p_code and p_name and p_code.lower() != p_name.lower():
            resolved_name = f"[{p_code}] {p_name}"
        elif p_name:
            resolved_name = p_name
        elif p_code:
            resolved_name = f"[{p_code}]"
        elif raw_user:
            resolved_name = raw_user
        else:
            resolved_name = "Anonymous"

        display_user = resolved_name if (show_names and resolved_name) else "Anonymous"

        sanitized.append({
            "id": r["id"],
            "requester_username": display_user,
            "amount": amount,
            "interest_rate": float(r.get("interest_rate") or 0.0),
            "term_days": int(r.get("term_days") or 0),
            "status": status,
            "transaction_type": tx_type,
            "is_verified": bool(r.get("is_verified")),
            # contract_id is strictly hidden from public viewers!
            "created_at": r["created_at"].isoformat() if hasattr(r["created_at"], "isoformat") else str(r["created_at"]),
            "contract_status": r.get("contract_status"),
            "contract_duedate": str(r.get("contract_duedate")) if r.get("contract_duedate") else None,
            "currency": bank.get("currency") or "ICA"
        })

    return {
        "bank_id": bank_id,
        "bank_name": bank.get("name"),
        "public_show_loans": show_loans,
        "public_show_deposits": show_deposits,
        "public_show_withdrawals": show_withdrawals,
        "public_show_names": show_names,
        "total_loans": loans_count,
        "total_lent": total_lent,
        "total_repaid": total_repaid,
        "active_lent": active_lent,
        "total_deposits": total_deposits,
        "total_withdrawals": total_withdrawals,
        "history": sanitized
    }


@banking_router.post("/sync")
async def trigger_bank_sync(
    request: Request,
    bank_id: Optional[int] = Query(None),
    user_id: str = Depends(get_current_user_id),
):
    repo = BankingRepository(request.app.state.db)
    if bank_id is None:
        async with repo.db.pool.acquire() as conn:
            owner_username = await get_username_from_user_id(conn, user_id)
            if owner_username:
                user_bank = await repo.get_bank_by_owner(owner_username)
                if user_bank:
                    bank_id = user_bank["id"]
    await repo.sync_loan_repayments(bank_id=bank_id)
    await global_ws_manager.broadcast("banking_updates", {
        "type": "BANKING_DATA_UPDATE",
        "event": "SYNC_TRIGGERED",
        "bank_id": bank_id
    })
    return {"status": "ok", "bank_id": bank_id}


@banking_router.post("/request", response_model=LoanRequestResponse)
async def request_bank_loan(
    data: LoanRequestCreate,
    user_id: str = Depends(get_current_user_id),
    service: BankingService = Depends(get_banking_service)
):
    async with service.repo.db.pool.acquire() as conn:
        requester_username = await get_username_from_user_id(conn, user_id)
        if not requester_username:
            raise HTTPException(status_code=404, detail="User username not found")

    new_loan = await service.create_loan_request(
        requester_username, data.bank_id, data.amount, data.interest_rate, data.term_days
    )
    await global_ws_manager.broadcast("banking_updates", {"type": "BANKING_DATA_UPDATE", "event": "LOAN_REQUESTED", "bank_id": data.bank_id})
    return new_loan


@banking_router.get("/loans/requested", response_model=List[LoanRequestResponse])
async def get_my_requested_loans(
    user_id: str = Depends(get_current_user_id),
    service: BankingService = Depends(get_banking_service)
):
    async with service.repo.db.pool.acquire() as conn:
        username = await get_username_from_user_id(conn, user_id)
        if not username:
            return []
    return await service.get_requested_loans(username)


@banking_router.get("/loans/received", response_model=List[LoanRequestResponse])
async def get_bank_received_loans(
    bank_id: int = Query(..., description="The ID of the bank"),
    user_id: str = Depends(get_current_user_id),
    service: BankingService = Depends(get_banking_service)
):
    return await service.get_bank_loan_requests(bank_id)


@banking_router.post("/loans/action")
async def action_bank_loan_request(
    data: LoanRequestAction,
    user_id: str = Depends(get_current_user_id),
    service: BankingService = Depends(get_banking_service)
):
    async with service.repo.db.pool.acquire() as conn:
        username = await get_username_from_user_id(conn, user_id)
        bank_id = await conn.fetchval("SELECT bank_id FROM bank_loan_requests WHERE id = $1", data.loan_id)
        if not bank_id:
            raise HTTPException(status_code=404, detail="Loan request not found")
        owner_username = await conn.fetchval("SELECT owner_username FROM player_banks WHERE id = $1", bank_id)
        if not username or username != owner_username:
            raise HTTPException(status_code=403, detail="You do not own this bank")

    success = await service.action_loan_request(data.loan_id, data.status, data.contract_id)
    await global_ws_manager.broadcast("banking_updates", {"type": "BANKING_DATA_UPDATE", "event": "LOAN_ACTION", "bank_id": bank_id})
    return {"success": success}


@banking_router.post("/loans/link", response_model=LoanRequestResponse)
async def link_contract_to_bank(
    data: LoanLinkContract,
    user_id: str = Depends(get_current_user_id),
    service: BankingService = Depends(get_banking_service)
):
    async with service.repo.db.pool.acquire() as conn:
        username = await get_username_from_user_id(conn, user_id)
        owner_username = await conn.fetchval("SELECT owner_username FROM player_banks WHERE id = $1", data.bank_id)
        if not username or username != owner_username:
            raise HTTPException(status_code=403, detail="You do not own this bank")

    try:
        linked_res = await service.link_contract_to_bank(
            data.bank_id,
            data.contract_localid,
            data.interest_rate,
            data.term_days,
            data.transaction_type or "LOAN"
        )
        await global_ws_manager.broadcast("banking_updates", {"type": "BANKING_DATA_UPDATE", "event": "CONTRACT_LINKED", "bank_id": data.bank_id})
        return linked_res
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@banking_router.post("/loans/import-csv", response_model=BankCsvImportResponse)
async def import_bank_ledger_csv(
    data: BankCsvImportPayload,
    user_id: str = Depends(get_current_user_id),
    service: BankingService = Depends(get_banking_service)
):
    async with service.repo.db.pool.acquire() as conn:
        username = await get_username_from_user_id(conn, user_id)
        owner_username = await conn.fetchval("SELECT owner_username FROM player_banks WHERE id = $1", data.bank_id)
        if not username or username != owner_username:
            raise HTTPException(status_code=403, detail="You do not own this bank")

    res = await service.import_bank_ledger_csv(data.bank_id, data.csv_content)
    await global_ws_manager.broadcast("banking_updates", {"type": "BANKING_DATA_UPDATE", "event": "LEDGER_IMPORTED", "bank_id": data.bank_id})
    return res


@banking_router.post("/deposits/manual", response_model=LoanRequestResponse)
async def record_manual_deposit(
    data: ManualDepositPayload,
    user_id: str = Depends(get_current_user_id),
    service: BankingService = Depends(get_banking_service)
):
    async with service.repo.db.pool.acquire() as conn:
        username = await get_username_from_user_id(conn, user_id)
        owner_username = await conn.fetchval("SELECT owner_username FROM player_banks WHERE id = $1", data.bank_id)
        if not username or username != owner_username:
            raise HTTPException(status_code=403, detail="You do not own this bank")

    try:
        deposit_res = await service.record_manual_deposit(
            data.bank_id,
            data.amount,
            data.username,
            data.transaction_type,
            data.note,
            data.update_liquidity
        )
        await global_ws_manager.broadcast("banking_updates", {"type": "BANKING_DATA_UPDATE", "event": "MANUAL_DEPOSIT", "bank_id": data.bank_id})
        return deposit_res
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@banking_router.get("/depositors", response_model=BankDepositorsResponse)
async def get_bank_depositors(
    bank_id: int = Query(..., description="The ID of the bank"),
    service: BankingService = Depends(get_banking_service)
):
    return await service.get_bank_depositors(bank_id)
