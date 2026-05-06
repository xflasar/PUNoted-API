import json
import logging
import time
from typing import Any, Dict, List

from helpers.db import _upsert_records
from helpers.shipments import fetch_active_shipments_structured
from managers.global_ws_manager import global_ws_manager

logger = logging.getLogger(__name__)


def _clean_array_field(records: List[Dict[str, Any]], field_name: str) -> None:
    """
    Parses string representations of arrays (e.g., '["id1", "id2"]') into
    Python list objects for fields that require array types (TEXT[]).
    """
    for record in records:
        value = record.get(field_name)
        if isinstance(value, str):
            try:
                # Attempt to parse the string as a JSON array
                parsed_value = json.loads(value)

                # If parsing succeeded and the result is a list, use it
                if isinstance(parsed_value, list):
                    record[field_name] = parsed_value

            except json.JSONDecodeError:
                # If it's a string but not a valid JSON array, log and skip (or handle error)
                # Since the data comes from a conversion process, this shouldn't happen
                # for these specific fields if the original source was an array.
                pass

        # Ensure 'None' values are converted to an empty list, as asyncpg prefers a list for TEXT[]
        if record.get(field_name) is None:
            record[field_name] = []


# --- CONSTANTS for UPSERT ---

# Contracts Table
CONTRACTS_UNIQUE_KEYS = ["id", "party"]
CONTRACTS_UPDATABLE_KEYS = [
    "localid",
    "timestamp",
    "party",
    "partnerid",
    "partnername",
    "partnercode",
    "status",
    "duedate",
    "name",
    "preamble",
    "extensiondeadline",
    "relatedcontracts",
    "contracttype",
]

# Conditions Table
CONDITIONS_UNIQUE_KEYS = ["id", "contractparty"]
CONDITIONS_UPDATABLE_KEYS = [
    "contractid",
    "deadline",
    "deadlineduration_millis",
    "amountmoney",
    "currencymoney",
    "dependencies",
    "addresssystemid",
    "addressplanetid",
    "addressstationid",
    "destinationsystemid",
    "destinationplanetid",
    "destinationstationid",
    "index",
    "type",
    "party",
    "status",
    "autoprovisionstoreid",
    "reputationchange",
    "contractparty",
]

# Materials Table (Compound Unique Key)
MATERIALS_UNIQUE_KEYS = ["contractconditionid", "materialid", "contractparty"]
MATERIALS_UPDATABLE_KEYS = ["amount", "pickedupamount"]

# Installments Table
INSTALLMENTS_UNIQUE_KEYS = ["conditionid", "contractparty"]
INSTALLMENTS_UPDATABLE_KEYS = [
    "interestamount",
    "repaymentamount",
    "totalamount",
    "currency",
]

# ----------------------------


async def handle_contracts_data_message(db, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handles a batch of converted contract data, performs bulk UPSERTs,
    and notifies relevant users via WebSocket.
    """
    start_time = time.perf_counter()
    logger.debug("Starting processing contract data.")

    converted_data = data["data"]

    # 1. Resolve User ID
    user_response = await db.fetch_one("SELECT accountid, userdataid FROM users WHERE accountid = $1;", data["userId"])
    if user_response and user_response.get("userdataid") is not None:
        userid = str(user_response.get("userdataid"))
    elif user_response:
        userid = str(user_response.get("accountid"))
    else:
        return {"success": False, "message": "User not found."}

    # Link records to user
    contracts = converted_data.get("contracts", [])
    for contract in contracts:
        contract["userid"] = userid

    conditions = converted_data.get("conditions", [])
    materials = converted_data.get("materials", [])
    installments = converted_data.get("installments", [])

    # Sanitation
    _clean_array_field(contracts, "relatedcontracts")
    _clean_array_field(conditions, "dependencies")

    total_records = len(contracts) + len(conditions) + len(materials) + len(installments)

    if total_records == 0:
        return {"success": True, "message": "No contract data processed."}

    try:
        async with db.pool.acquire() as con:
            async with con.transaction():
                await _upsert_records(con, "contracts", contracts, CONTRACTS_UNIQUE_KEYS)
                await _upsert_records(con, "contract_conditions", conditions, CONDITIONS_UNIQUE_KEYS)
                await _upsert_records(con, "contract_materials", materials, MATERIALS_UNIQUE_KEYS)
                await _upsert_records(
                    con,
                    "contract_loan_installments",
                    installments,
                    INSTALLMENTS_UNIQUE_KEYS,
                )

        # --- NOTIFICATION LOGIC ---
        # We pass the original 'data["userId"]' (accountid) for WS targeting
        current_account_id = data["userId"]
        await _process_notifications(db, global_ws_manager, current_account_id, contracts, conditions)

        end_time = time.perf_counter()
        logger.debug(f"Total processing took {end_time - start_time:.4f}s.")
        return {"success": True, "message": "Processed and Notified."}

    except Exception as e:
        logger.error(f"Error processing contract data message: {e}", exc_info=True)
        raise


async def _process_notifications(
    db,
    ws_manager,
    current_account_id: str,
    contracts: List[Dict],
    conditions: List[Dict],
):
    """
    Unified notification handler:
    1. Resolves all recipients (User + Partners).
    2. ALWAYS sends a 'CONTRACTS_DATA_UPDATE_SIGNAL' (for the general list).
    3. CONDITIONALLY sends 'SHIPMENT_DATA_UPDATE' (heavy payload) if shipment items exist.
    """
    try:
        # 1. Resolve Recipients (Current User + Any Partners found in contracts)
        recipients = {current_account_id}

        partner_company_ids = {c.get("partnerid") for c in contracts if c.get("partnerid")}
        if partner_company_ids:
            q = """
            SELECT u.accountid 
            FROM company_data cd
            JOIN users u ON cd.userdataid = u.userdataid
            WHERE cd.companyid = ANY($1::text[])
            """
            async with db.pool.acquire() as conn:
                rows = await conn.fetch(q, list(partner_company_ids))
                for r in rows:
                    if r["accountid"]:
                        recipients.add(str(r["accountid"]))

        # 2. Check if this batch contains Shipment data
        has_shipment_data = False
        for cond in conditions:
            if cond.get("type") == "DELIVERY_SHIPMENT":
                has_shipment_data = True
                break

        # 3. Broadcast to all recipients
        for account_id in recipients:
            # A. ALWAYS Signal the General Contracts List to refresh
            # This ensures "normal contracts have all contracts"
            try:
                await ws_manager.send_personal_message(account_id, {"type": "CONTRACTS_DATA_UPDATE_SIGNAL", "data": {}})
            except Exception as e:
                logger.warning(f"Failed to signal contract update to {account_id}: {e}")

            # B. IF Shipment, Push the Heavy Data for the Dashboard
            if has_shipment_data:
                try:
                    # Fetch fresh structure specifically for this user context
                    new_shipment_state = await fetch_active_shipments_structured(db, account_id)

                    await ws_manager.send_personal_message(
                        account_id,
                        {"type": "SHIPMENT_DATA_UPDATE", "data": new_shipment_state},
                    )
                    logger.debug(f"Pushed fresh shipment data to {account_id}")
                except Exception as e:
                    logger.error(f"Failed to push shipment data to {account_id}: {e}")

    except Exception as e:
        logger.error(f"Notification processing failed: {e}")
