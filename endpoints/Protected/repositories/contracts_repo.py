import csv
import io
import json

# -----------------------------------------------------------------------------
# 0. RESOLVE USERS (Fast PK Lookup)
# -----------------------------------------------------------------------------
SQL_RESOLVE_USER_IDS = """
    SELECT userdataid, username FROM users WHERE username = ANY($1::text[])
"""

# -----------------------------------------------------------------------------
# 1. DRIVER: Get IDs Fast (Using UserID Index)
#    ORDER BY c.date DESC ensures 18/2 comes before 17/2
# -----------------------------------------------------------------------------
SQL_GET_CONTRACT_IDS = """
SELECT c.id
FROM contracts c
WHERE 
    c.userid = ANY($1::text[]) 
    AND ($3::text IS NULL OR c.status = $3)
    AND ($4::text IS NULL OR c.partnercode = $4)
    AND ($5::text IS NULL OR c.party = $5)
    AND ($6::text IS NULL OR c.localid = $6)
    AND (
        $2::text[] IS NULL 
        OR EXISTS (SELECT 1 FROM contract_conditions cc WHERE cc.contractid = c.id AND cc.type = ANY($2::text[]) AND cc.contractparty = c.party)
    )
ORDER BY c.date DESC
LIMIT $7 OFFSET $8;
"""

# -----------------------------------------------------------------------------
# 2. BASE DATA (Always Fetched)
# -----------------------------------------------------------------------------
SQL_GET_BASE_DATA = """
SELECT 
    c.id, c.localid, c.date, c.extensiondeadline, c.duedate,
    c.canextend, c.canrequesttermination, c.terminationsent, c.terminationreceived,
    c.name, c.preamble, c.party, c.status,
    c.partnerid, c.partnername, c.partnercode,
    c.userid
FROM contracts c
WHERE c.id = ANY($1::text[]) AND c.userid = ANY($2::text[])
"""

# -----------------------------------------------------------------------------
# 3. GENERIC BUILDER (Shipments, Materials, Standard)
# -----------------------------------------------------------------------------
SQL_GET_CONDITIONS_GENERIC = """
WITH target_contracts AS (
    SELECT id, party FROM contracts 
    WHERE id = ANY($1::text[]) AND userid = ANY($2::text[])
    -- ONLY grab contracts that are NOT loans
    AND NOT EXISTS (SELECT 1 FROM contract_conditions WHERE contractid = contracts.id AND type IN ('LOAN_PAYOUT', 'LOAN_INSTALLMENT'))
)
SELECT 
    cc.contractid,
    jsonb_agg(
        jsonb_build_object(
            'ConditionId', cc.id,
            'Type', cc.type,
            'Party', cc.party,
            'ConditionIndex', cc.index,
            'Status', cc.status,
            'DeadlineEpochMs', EXTRACT(EPOCH FROM cc.deadline) * 1000,
            'DeadlineDurationMs', cc.deadlineduration_millis,
            
            'Amount', cc.amountmoney,
            'Currency', cc.currencymoney,
            
            'Address', TRIM(COALESCE(p_origin.naturalid, s_origin.name) || ' (' || COALESCE(sys_origin.name, sys_origin.naturalid) || ')'),
            'Destination', TRIM(COALESCE(p_dest.naturalid, s_dest.name) || ' (' || COALESCE(sys_dest.name, sys_dest.naturalid) || ')'),

            'MaterialId', mat.materialid,
            'MaterialTicker', mat.ticker,
            'MaterialAmount', mat.amount,
            'PickedUpMaterialId', CASE WHEN cc.type = 'COMEX_PURCHASE_PICKUP' THEN mat.materialid ELSE NULL END,
            'PickedUpMaterialTicker', CASE WHEN cc.type = 'COMEX_PURCHASE_PICKUP' THEN mat.ticker ELSE NULL END,
            'PickedUpAmount', mat.pickedupamount,

            'ShipmentItemId', cc.shipmentitemid,
            
            'Principal', NULL,
            'Interest', NULL,
            'Dependencies', COALESCE((SELECT jsonb_agg(jsonb_build_object('Dependency', dep)) FROM unnest(cc.dependencies) dep), '[]'::jsonb)
        ) ORDER BY cc.index
    ) as conditions_json
FROM target_contracts tc
JOIN contract_conditions cc ON cc.contractid = tc.id AND cc.contractparty = tc.party
LEFT JOIN planets p_origin ON p_origin.planetid = cc.addressplanetid
LEFT JOIN stations s_origin ON s_origin.stationid = cc.addressstationid
LEFT JOIN systems sys_origin ON sys_origin.systemid = cc.addresssystemid
LEFT JOIN planets p_dest ON p_dest.planetid = cc.destinationplanetid
LEFT JOIN stations s_dest ON s_dest.stationid = cc.destinationstationid
LEFT JOIN systems sys_dest ON sys_dest.systemid = cc.destinationsystemid
LEFT JOIN LATERAL (
    SELECT cm.materialid, m.ticker, cm.amount, cm.pickedupamount
    FROM contract_materials cm 
    JOIN materials m ON m.materialid = cm.materialid 
    WHERE cm.contractconditionid = cc.id LIMIT 1
) mat ON TRUE
GROUP BY cc.contractid
"""

# -----------------------------------------------------------------------------
# 4. LOAN BUILDER (Interest, Repayments)
# -----------------------------------------------------------------------------
SQL_GET_CONDITIONS_LOAN = """
WITH target_contracts AS (
    SELECT id, party FROM contracts 
    WHERE id = ANY($1::text[]) AND userid = ANY($2::text[])
    -- ONLY grab contracts that ARE loans
    AND EXISTS (SELECT 1 FROM contract_conditions WHERE contractid = contracts.id AND type IN ('LOAN_PAYOUT', 'LOAN_INSTALLMENT'))
),
loan_stats AS (
    SELECT 
        cc.contractid,
        COUNT(*) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') as total_installments,
        COUNT(*) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT' AND cc.status = 'FULFILLED') as fulfilled_installments,
        MAX(cc.deadlineduration_millis) / 86400000.0 as avg_interval_days,
        
        -- 1. NOMINAL RATE (e.g., 2.5% per installment)
        CASE 
            WHEN SUM(cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') > 0 
            THEN (MAX(cli.interestamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') / SUM(cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT')) * 100.0
            ELSE 0.0
        END as interest_rate,

        -- 2. TOTAL LIFETIME RATE (e.g., 15% over the whole loan)
        CASE 
            WHEN SUM(cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') > 0 
            THEN (SUM(cli.interestamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') / SUM(cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT')) * 100.0
            ELSE 0.0
        END as total_interest_rate,

        CASE 
            WHEN COALESCE(MIN(cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT'), 0) = 0 THEN 'INTEREST_LOAN'
            WHEN COUNT(DISTINCT cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') 
                 < 
                 COUNT(DISTINCT cli.totalamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') 
                 THEN 'STABLE_LOAN'
            WHEN COUNT(DISTINCT cli.totalamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') 
                 < 
                 COUNT(DISTINCT cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') 
                 THEN 'ANNUITY_LOAN'
            WHEN (MAX(cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') - MIN(cli.repaymentamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT'))
                 <=
                 (MAX(cli.totalamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT') - MIN(cli.totalamount) FILTER (WHERE cc.type = 'LOAN_INSTALLMENT'))
                 THEN 'STABLE_LOAN'
            ELSE 'ANNUITY_LOAN'
        END as loan_strategy

    FROM target_contracts tc
    JOIN contract_conditions cc ON cc.contractid = tc.id AND cc.contractparty = tc.party
    LEFT JOIN contract_loan_installments cli ON cli.conditionid = cc.id AND cli.contractparty = tc.party
    GROUP BY cc.contractid
)
SELECT 
    cc.contractid,
    ls.total_installments,
    ls.fulfilled_installments,
    ls.loan_strategy,
    ls.avg_interval_days,
    ls.interest_rate,
    ls.total_interest_rate,
    
    jsonb_agg(
        jsonb_build_object(
            'ConditionId', cc.id,
            'Type', cc.type,
            'Party', cc.party,
            'ConditionIndex', cc.index,
            'Status', cc.status,
            'DeadlineEpochMs', EXTRACT(EPOCH FROM cc.deadline) * 1000,
            'DeadlineDurationMs', cc.deadlineduration_millis,
            
            'Amount', COALESCE(cc.amountmoney, cli.totalamount),
            'Currency', COALESCE(cc.currencymoney, cli.currency),
            
            'Address', NULL,
            'Destination', NULL,

            'MaterialId', NULL,
            'MaterialTicker', NULL,
            'MaterialAmount', NULL,
            'PickedUpMaterialId', NULL,
            'PickedUpMaterialTicker', NULL,
            'PickedUpAmount', NULL,

            'ShipmentItemId', NULL,
            
            'Principal', cli.repaymentamount,
            'Interest', cli.interestamount,
            'Dependencies', COALESCE((SELECT jsonb_agg(jsonb_build_object('Dependency', dep)) FROM unnest(cc.dependencies) dep), '[]'::jsonb)
        ) ORDER BY cc.index
    ) as conditions_json
FROM target_contracts tc
JOIN contract_conditions cc ON cc.contractid = tc.id AND cc.contractparty = tc.party
JOIN loan_stats ls ON ls.contractid = cc.contractid
LEFT JOIN contract_loan_installments cli ON cli.conditionid = cc.id AND cli.contractparty = tc.party
GROUP BY cc.contractid, ls.total_installments, ls.fulfilled_installments, ls.loan_strategy, ls.avg_interval_days, ls.interest_rate, ls.total_interest_rate
"""

# ==============================================================================
# FUNCTION 1: JSON FETCH (For UI)
# ==============================================================================
async def get_filtered_contracts(
    conn,
    usernames_list,
    c_type=None,
    status=None,
    partner_code=None,
    party=None,
    local_id=None,
    limit=50,
    page=1
):
    offset = (page - 1) * limit

    # 0. Pre-resolve Users
    user_rows = await conn.fetch(SQL_RESOLVE_USER_IDS, usernames_list)
    if not user_rows: return '[]'
    user_ids = [r['userdataid'] for r in user_rows]
    user_map_name = {r['userdataid']: r['username'] for r in user_rows}

    # 1. Filter IDs (Map Category to DB Types)
    type_filter_array = None
    if c_type in ("TRADE", "BUY", "SELL"):
        type_filter_array = ['COMEX_PURCHASE_PICKUP', 'DELIVERY']
    elif c_type == "SHIPMENT":
        type_filter_array = ['DELIVERY_SHIPMENT']
    elif c_type == "LOAN":
        type_filter_array = ['LOAN_PAYOUT', 'LOAN_INSTALLMENT']
    elif c_type:
        type_filter_array = [c_type] # Fallback exact match

    contract_ids_rows = await conn.fetch(
        SQL_GET_CONTRACT_IDS,
        user_ids, type_filter_array, status, partner_code, party, local_id, limit, offset
    )

    if not contract_ids_rows:
        return '[]'

    ids_list = [r['id'] for r in contract_ids_rows]

    # 2. Base Data
    base_rows = await conn.fetch(SQL_GET_BASE_DATA, ids_list, user_ids)

    # 3. Polymorphic Details (Safe Merge)
    detail_map = {}

    # Run Loan logic if specifically asked for, or if viewing ALL contracts
    if c_type == 'LOAN' or c_type is None:
        loan_rows = await conn.fetch(SQL_GET_CONDITIONS_LOAN, ids_list, user_ids)
        for r in loan_rows:
            detail_map[r['contractid']] = {
                'conditions': json.loads(r['conditions_json']),
                'stats': {
                    'LoanStrategy': r['loan_strategy'],
                    'InstallmentInterval': round(float(r['avg_interval_days'] or 0)),
                    'InstallmentCount': r['total_installments'],
                    'InstallmentDone': r['fulfilled_installments'],
                    'InterestRate': round(float(r['interest_rate']), 4) if r['interest_rate'] is not None else 0.0,
                    'TotalInterestRate': round(float(r['total_interest_rate']), 4) if r['total_interest_rate'] is not None else 0.0
                }
            }

    # Run Generic logic if specifically asked for, or if viewing ALL contracts
    if c_type != 'LOAN':
        generic_rows = await conn.fetch(SQL_GET_CONDITIONS_GENERIC, ids_list, user_ids)
        for r in generic_rows:
            detail_map[r['contractid']] = {
                'conditions': json.loads(r['conditions_json']),
                'stats': {
                    'LoanStrategy': None,
                    'InstallmentInterval': 0,
                    'InstallmentCount': 0,
                    'InstallmentDone': 0,
                    'InterestRate': 0.0,
                    'TotalInterestRate': 0.0
                }
            }

    # 4. Assembly
    user_result_map = {}

    for r in base_rows:
        cid = r['id']
        username = user_map_name.get(r['userid'], "Unknown")
        details = detail_map.get(cid, {'conditions': [], 'stats': {}})
        stats = details.get('stats', {})

        contract_obj = {
            "ContractId": cid,
            "ContractLocalId": r['localid'],
            "UserNameSubmitted": username,
            "Timestamp": r['date'].isoformat() if r['date'] else None,
            "DateEpochMs": r['date'].timestamp() * 1000 if r['date'] else 0,

            "Status": r['status'],
            "Party": r['party'],
            "PartnerCode": r['partnercode'],
            "PartnerName": r['partnername'],

            "ExtensionDeadlineEpochMs": r['extensiondeadline'].timestamp() * 1000 if r['extensiondeadline'] else None,
            "DueDateEpochMs": r['duedate'].timestamp() * 1000 if r['duedate'] else None,

            "CanExtend": r['canextend'],
            "CanRequestTermination": r['canrequesttermination'],
            "TerminationSent": r['terminationsent'],
            "TerminationReceived": r['terminationreceived'],
            "Name": r['name'],
            "Preamble": r['preamble'],

            "Conditions": details['conditions'],

            # Merged Stats (Always present)
            "LoanStrategy": stats.get('LoanStrategy'),
            "InstallmentInterval": stats.get('InstallmentInterval', 0),
            "InstallmentCount": stats.get('InstallmentCount', 0),
            "InstallmentDone": stats.get('InstallmentDone', 0),
            "InterestRate": stats.get('InterestRate', 0.0),
            'TotalInterestRate': stats.get('TotalInterestRate', 0.0)
        }

        if username not in user_result_map:
            user_result_map[username] = []
        user_result_map[username].append(contract_obj)

    final_result = [
        {"Username": u, "Contracts": c_list}
        for u, c_list in user_result_map.items()
    ]

    return json.dumps(final_result)


# ==============================================================================
# FUNCTION 2: CSV STREAM (Chunked Generator)
# ==============================================================================
async def stream_contracts_csv(
    conn,
    usernames_list,
    c_type=None,
    status=None,
    partner_code=None,
    party=None,
    local_id=None
):
    """
    Generator that fetches ALL matching IDs, then chunks details fetching
    to avoid OOM, yielding CSV lines.
    """

    # 0. Pre-resolve
    user_rows = await conn.fetch(SQL_RESOLVE_USER_IDS, usernames_list)
    if not user_rows: return
    user_ids = [r['userdataid'] for r in user_rows]
    user_map_name = {r['userdataid']: r['username'] for r in user_rows}

    # 1. Fetch ALL IDs (Map Category to DB Types)
    type_filter_array = None
    if c_type in ("TRADE", "BUY", "SELL"):
        type_filter_array = ['COMEX_PURCHASE_PICKUP', 'DELIVERY']
    elif c_type == "SHIPMENT":
        type_filter_array = ['DELIVERY_SHIPMENT']
    elif c_type == "LOAN":
        type_filter_array = ['LOAN_PAYOUT', 'LOAN_INSTALLMENT']
    elif c_type:
        type_filter_array = [c_type] # Fallback for exact matches

    contract_ids_rows = await conn.fetch(
        SQL_GET_CONTRACT_IDS,
        user_ids, type_filter_array, status, partner_code, party, local_id,
        100000, 0
    )

    # These IDs are already sorted by Date DESC
    all_ids = [r['id'] for r in contract_ids_rows]

    # 2. Setup CSV Buffer
    output = io.StringIO()
    writer = csv.writer(output)

    # Write Header
    headers = [
        "ContractId", "LocalId", "Date", "User", "Partner", "Type",
        "Status", "Party", "TotalAmount", "Currency", "Principal", "Interest",
        "Installments", "Progress", "Strategy", "InterestRatePercent"
    ]
    writer.writerow(headers)
    yield output.getvalue()
    output.seek(0)
    output.truncate(0)

    # 3. Chunked Processing
    BATCH_SIZE = 100

    for i in range(0, len(all_ids), BATCH_SIZE):
        chunk_ids = all_ids[i : i + BATCH_SIZE]

        # A. Base Data
        base_rows = await conn.fetch(SQL_GET_BASE_DATA, chunk_ids, user_ids)
        base_map = {r['id']: r for r in base_rows}

        # B. Polymorphic Details (Safe Merge)
        detail_map = {}

        if c_type == 'LOAN' or c_type is None:
            loan_rows = await conn.fetch(SQL_GET_CONDITIONS_LOAN, chunk_ids, user_ids)
            for r in loan_rows:
                conds = json.loads(r['conditions_json'])
                total_amt = sum((c['Amount'] or 0) for c in conds)
                total_principal = sum((c['Principal'] or 0) for c in conds)
                total_interest = sum((c['Interest'] or 0) for c in conds)
                currency = conds[0]['Currency'] if conds else 'ICA'

                detail_map[r['contractid']] = {
                    'TotalAmount': total_amt,
                    'Currency': currency,
                    'Principal': total_principal,
                    'Interest': total_interest,
                    'Installments': r['total_installments'],
                    'Progress': f"{r['fulfilled_installments']}/{r['total_installments']}",
                    'Strategy': r['loan_strategy'],
                    'InterestRate': round(float(r['interest_rate']), 2) if r['interest_rate'] is not None else 0.0,
                    'TotalInterestRate': round(float(r['total_interest_rate']), 2) if r['total_interest_rate'] is not None else 0.0
                }

        if c_type != 'LOAN':
            generic_rows = await conn.fetch(SQL_GET_CONDITIONS_GENERIC, chunk_ids, user_ids)
            for r in generic_rows:
                conds = json.loads(r['conditions_json'])
                total_amt = sum((c['Amount'] or 0) for c in conds)
                currency = conds[0]['Currency'] if conds else 'ICA'

                detail_map[r['contractid']] = {
                    'TotalAmount': total_amt,
                    'Currency': currency,
                    'Principal': 0, 'Interest': 0,
                    'Installments': 0, 'Progress': '-', 'Strategy': '-', 'InterestRate': 0.0
                }

        # C. Write Rows
        for cid in chunk_ids:
            base = base_map.get(cid)
            if not base: continue
            details = detail_map.get(cid, {})

            row = [
                str(base['id']),
                base['localid'],
                base['date'].strftime('%Y-%m-%d %H:%M'),
                user_map_name.get(base['userid'], ''),
                base['partnername'],
                c_type or "GENERIC",
                base['status'],
                base['party'],
                details.get('TotalAmount', 0),
                details.get('Currency', 'ICA'),
                details.get('Principal', 0),
                details.get('Interest', 0),
                details.get('Installments', 0),
                details.get('Progress', '-'),
                details.get('Strategy', '-'),
                details.get('InterestRate', 0.0)
            ]

            writer.writerow(row)
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)
