import logging
from typing import Any, Dict, List, Optional

import orjson
from asyncpg import PostgresError
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse as DefaultJSONResponse

from app.core.security import require_internal_origin
from auth import get_current_user_id

logger = logging.getLogger(__name__)

contracts_router = APIRouter(prefix="/contracts", tags=["Contracts Data"], dependencies=[Depends(require_internal_origin)])


class ORJSONResponse(DefaultJSONResponse):
    """
    FastAPI Response class that uses the C-implemented orjson library
    for serialization, which is significantly faster than standard Python json.
    """

    media_type = "application/json"

    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)


async def _fetch_contracts_data_native(con, user_id: str) -> Dict[str, Any]:
    """
    Fetches all data, using DB-side jsonb aggregation and returning a
    native Python dict/list structure by dropping the final ::TEXT cast.
    """

    aggregation_query = """
        SELECT
            (jsonb_build_object('contracts', jsonb_agg(contract_obj)))::jsonb
            
        FROM (
            -- Base Contracts Query (All internal functions use jsonb_*)
            SELECT
                jsonb_build_object(
                    'id', c.id,
                    'localId', c.localid,
                    'date', jsonb_build_object('timestamp', EXTRACT(EPOCH FROM c.timestamp) * 1000),
                    'party', c.party,
                    'partner', jsonb_build_object(
                        'id', c.partnerid,
                        'name', c.partnername,
                        'code', c.partnercode
                    ),
                    'status', c.status,
                    'dueDate', c.duedate,
                    'name', c.name,
                    'preamble', c.preamble,
                    'extensionDeadline', c.extensiondeadline,
                    'relatedContracts', c.relatedcontracts,
                    'contractType', c.contracttype,
                    
                    'conditions', COALESCE(
                        (
                            SELECT
                                jsonb_agg(base_condition.condition_obj ORDER BY base_condition.index) 
                            FROM (
                                SELECT
                                    cc.index, 
                                    jsonb_build_object(
                                        'id', cc.id,
                                        'type', cc.type,
                                        'party', cc.party,
                                        'index', cc.index, 
                                        'status', cc.status,
                                        'dependencies', cc.dependencies,
                                        'deadlineDuration', jsonb_build_object('millis', cc.deadlineduration_millis),
                                        'deadline', cc.deadline,
                                        'amount', CASE WHEN cc.amountmoney IS NOT NULL THEN jsonb_build_object('amount', cc.amountmoney, 'currency', cc.currencymoney) ELSE NULL END,
                                        'addressSystemId', cc.addresssystemid,
                                        'addressPlanetId', cc.addressplanetid,
                                        'addressStationId', cc.addressstationid,
                                        'destinationSystemId', cc.destinationsystemid,
                                        'destinationPlanetId', cc.destinationplanetid,
                                        'destinationStationId', cc.destinationstationid,
                                        'autoProvisionStoreId', cc.autoprovisionstoreid,
                                        'reputationChange', cc.reputationchange,
                                        
                                        'materials_data', COALESCE(
                                            (
                                                SELECT 
                                                    jsonb_agg(
                                                        jsonb_build_object('materialid', cm.materialid, 'amount', cm.amount, 'pickedupamount', cm.pickedupamount)
                                                    )
                                                FROM contract_materials cm
                                                WHERE cm.contractconditionid = cc.id
                                            ),
                                            '[]'::jsonb
                                        )
                                    ) 
                                    || COALESCE(
                                        (
                                            SELECT jsonb_build_object(
                                                'interest', jsonb_build_object('amount', cli.interestamount, 'currency', cli.currency),
                                                'repayment', jsonb_build_object('amount', cli.repaymentamount, 'currency', cli.currency),
                                                'total', jsonb_build_object('amount', cli.totalamount, 'currency', cli.currency)
                                            )
                                            FROM contract_loan_installments cli
                                            WHERE cli.conditionid = cc.id
                                        ),
                                        '{}'::jsonb
                                    ) AS condition_obj
                                    
                                FROM contract_conditions cc
                                WHERE cc.contractid = c.id
                                
                            ) AS base_condition
                        ),
                        '[]'::jsonb
                    )
                ) AS contract_obj
            FROM contracts c
            INNER JOIN users u ON u.userdataid = c.userid
            WHERE u.accountid = $1
        ) AS contracts_alias;
    """

    raw = await con.fetchval(aggregation_query, user_id)

    # If asyncpg returned a str (i.e. JSON text), parse it with orjson.
    if isinstance(raw, str):
        try:
            parsed = orjson.loads(raw)
        except Exception:
            # If parsing fails, log and re-raise or return empty structure
            logger.exception("Failed to parse json string returned from DB")
            raise
        return parsed

    # If it's already a dict/list, return it straight away.
    if isinstance(raw, (dict, list)):
        return raw

    # If it's None or unexpected type, return empty structure
    return {"contracts": []}


@contracts_router.get("/")
async def get_user_contracts(request: Request, user_id: str = Depends(get_current_user_id)):
    """
    Fetches all contracts, returns a native Python dict, and uses ORJSON
    to serialize it.
    """
    db = request.app.state.db

    try:
        async with db.pool.acquire() as con:
            # This function returns a native Python dict
            contracts_data_dict = await _fetch_contracts_data_native(con, user_id)

            return ORJSONResponse(content=contracts_data_dict)

    except PostgresError as e:
        logger.error(f"Database error fetching contracts for user {user_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail="A database error occurred while retrieving contracts.",
        )
    except Exception as e:
        logger.error(f"Unexpected error fetching contracts for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")


# SHIPPING types
SHIPPING_CONDITION_TYPES = [
    "PROVISION_SHIPMENT",
    "DELIVERY_SHIPMENT",
    "PICKUP_SHIPMENT",
]


# ============================================================
# Fetch FLTERED contracts (status + shipping-type condition)
# ============================================================
async def _fetch_contracts_data_filtered(
    con,
    user_id: str,
    status_filter: Optional[str] = None,
    condition_types_filter: Optional[List[str]] = None,
) -> Dict[str, Any]:
    where_clauses = ["u.accountid = $1"]
    params = [user_id]
    param_count = 2

    # Filter by contract status
    if status_filter:
        where_clauses.append(f"c.status = ${param_count}")
        params.append(status_filter)
        param_count += 1

    # Filter contracts that have AT LEAST ONE shipping-type condition
    if condition_types_filter:
        where_clauses.append(f"""
            EXISTS (
                SELECT 1
                FROM contract_conditions cc
                WHERE cc.contractid = c.id
                  AND cc.type = ANY(${param_count}::text[])
            )
        """)
        params.append(condition_types_filter)
        param_count += 1

    where_clause_str = " AND ".join(where_clauses)

    # ===============================================================
    # MAIN AGGREGATION QUERY (jsonb → asyncpg dict → ORJSONResponse)
    # ===============================================================
    aggregation_query = f"""
        SELECT
            (jsonb_build_object('contracts', jsonb_agg(contract_obj)))::jsonb
        FROM (
            SELECT
                jsonb_build_object(
                    'id', c.id,
                    'localId', c.localid,
                    'date', jsonb_build_object('timestamp', EXTRACT(EPOCH FROM c.timestamp) * 1000),
                    'party', c.party,
                    'partner', jsonb_build_object(
                        'id', c.partnerid,
                        'name', c.partnername,
                        'code', c.partnercode
                    ),
                    'status', c.status,
                    'dueDate', c.duedate,
                    'name', c.name,
                    'preamble', c.preamble,
                    'extensionDeadline', c.extensiondeadline,
                    'relatedContracts', c.relatedcontracts,
                    'contractType', c.contracttype,

                    'conditions', COALESCE(
                        (
                            SELECT jsonb_agg(base_condition.condition_obj ORDER BY base_condition.index)
                            FROM (
                                SELECT
                                    cc.index,
                                    jsonb_build_object(
                                        'id', cc.id,
                                        'type', cc.type,
                                        'party', cc.party,
                                        'index', cc.index,
                                        'status', cc.status,
                                        'dependencies', cc.dependencies,
                                        'deadlineDuration', jsonb_build_object('millis', cc.deadlineduration_millis),
                                        'deadline', cc.deadline,
                                        'amount', CASE WHEN cc.amountmoney IS NOT NULL
                                                        THEN jsonb_build_object('amount', cc.amountmoney, 'currency', cc.currencymoney)
                                                        ELSE NULL END,
                                        'addressSystemId', cc.addresssystemid,
                                        'addressPlanetId', cc.addressplanetid,
                                        'addressStationId', cc.addressstationid,
                                        'destinationSystemId', cc.destinationsystemid,
                                        'destinationPlanetId', cc.destinationplanetid,
                                        'destinationStationId', cc.destinationstationid,
                                        'autoProvisionStoreId', cc.autoprovisionstoreid,
                                        'reputationChange', cc.reputationchange,

                                        'materials_data', COALESCE(
                                            (
                                                SELECT jsonb_agg(
                                                    jsonb_build_object(
                                                        'materialid', cm.materialid,
                                                        'amount', cm.amount,
                                                        'pickedupamount', cm.pickedupamount
                                                    )
                                                )
                                                FROM contract_materials cm
                                                WHERE cm.contractconditionid = cc.id
                                            ),
                                            '[]'::jsonb
                                        )
                                    )
                                    ||
                                    COALESCE(
                                        (
                                            SELECT jsonb_build_object(
                                                'interest', jsonb_build_object('amount', cli.interestamount, 'currency', cli.currency),
                                                'repayment', jsonb_build_object('amount', cli.repaymentamount, 'currency', cli.currency),
                                                'total', jsonb_build_object('amount', cli.totalamount, 'currency', cli.currency)
                                            )
                                            FROM contract_loan_installments cli
                                            WHERE cli.conditionid = cc.id
                                        ),
                                        '{{}}'::jsonb
                                    ) AS condition_obj
                                FROM contract_conditions cc
                                WHERE cc.contractid = c.id
                            ) AS base_condition
                        ),
                        '[]'::jsonb
                    )
                ) AS contract_obj

            FROM contracts c
            INNER JOIN users u ON u.userdataid = c.userid
            WHERE {where_clause_str}
        ) AS contracts_alias;
    """

    # Get native jsonb → dict OR json string → parse
    raw = await con.fetchval(aggregation_query, *params)

    if isinstance(raw, str):
        return orjson.loads(raw)

    if isinstance(raw, dict):
        return raw

    return {"contracts": []}


# ============================================================
# PARTIALLY_FULFILLED + SHIPPING CONDITIONS
# ============================================================
@contracts_router.get("/shipping-partial", response_class=ORJSONResponse)
async def get_contracts_shipping_partial(request: Request, user_id: str = Depends(get_current_user_id)):
    db = request.app.state.db

    try:
        async with db.pool.acquire() as con:
            contracts_data_dict = await _fetch_contracts_data_filtered(
                con,
                user_id,
                status_filter="PARTIALLY_FULFILLED",
                condition_types_filter=SHIPPING_CONDITION_TYPES,
            )
            return ORJSONResponse(content=contracts_data_dict)

    except PostgresError as e:
        logger.error(f"Database error fetching shipping/partial contracts for user {user_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail="A database error occurred while retrieving contracts.",
        )
    except Exception as e:
        logger.error(f"Unexpected error fetching shipping/partial contracts for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")
