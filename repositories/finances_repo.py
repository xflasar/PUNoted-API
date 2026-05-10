import logging

logger = logging.getLogger(__name__)

SQL_FINANCIAL_OVERVIEW = """
WITH TargetUser AS (
    SELECT DISTINCT userdataid 
    FROM users 
    WHERE accountid = $1
),
CurrentBalances AS (
    SELECT 
        balancecurrencycode AS currency, 
        SUM(balanceamount) AS liquid_amount
    FROM user_currency_accounts
    WHERE userid IN (SELECT userdataid FROM TargetUser)
    GROUP BY balancecurrencycode
),
LockedCapital AS (
    SELECT 
        limitcurrency AS currency, 
        SUM(CASE WHEN type IN ('BUY', 'BUYING') THEN amount * limitamount ELSE 0 END) AS locked_buy,
        SUM(CASE WHEN type IN ('SELL', 'SELLING') THEN amount * limitamount ELSE 0 END) AS locked_sell
    FROM comex_trade_orders
    WHERE userid IN (SELECT userdataid FROM TargetUser)
      AND type IN ('BUY', 'BUYING', 'SELL', 'SELLING') 
      AND status IN ('PLACED', 'PARTIALLY_FILLED', 'OPEN')
    GROUP BY limitcurrency
),
PendingContracts AS (
    SELECT 
        cc.currencymoney AS currency, 
        SUM(CASE WHEN cc.party != c.party THEN cc.amountmoney ELSE 0 END) AS pending_receivable,
        SUM(CASE WHEN cc.party = c.party THEN cc.amountmoney ELSE 0 END) AS pending_payable
    FROM contract_conditions cc
    JOIN contracts c ON cc.contractid = c.id
    WHERE c.userid IN (SELECT userdataid FROM TargetUser)
      AND cc.type = 'PAYMENT' 
      AND cc.status = 'PENDING'
      AND c.status IN ('PARTIALLY_FULFILLED', 'CLOSED')
      AND cc.amountmoney IS NOT NULL
	  AND c.party = cc.contractparty
    GROUP BY cc.currencymoney
),
RawStorage AS (
        -- 1. Calculate current physical materials in storage
        SELECT 
            si.materialid,
            SUM(si.quantity) AS raw_quantity
        FROM storage_items si
        JOIN storages s ON si.storageid = s.storageid
        WHERE s.userid IN (SELECT userdataid FROM TargetUser)
          AND s.type IN ('STORE', 'SHIP_STORE', 'WAREHOUSE_STORE')
        GROUP BY si.materialid
    ),
    ProductionInputs AS (
        -- 2. Calculate all required inputs for active production orders
        SELECT
            mri.materialid AS materialid,
            SUM(mri.factor) AS required_input_quantity
        FROM sites s
        JOIN site_production_lines spl ON s.siteid = spl.siteid
        JOIN site_production_line_orders splo ON spl.productionlineid = splo.productionlineid
        JOIN production_recipe_input_factors mri ON splo.recipeid = mri.productiontemplateid AND mri.productionlineid = spl.productionlineid
        WHERE s.userid IN (SELECT userdataid FROM TargetUser)
        GROUP BY mri.materialid
    ),
    AvailableAfterProduction AS (
        -- 3. Anti-Join: Keep ONLY items that are NOT used in production
        SELECT 
            rs.materialid,
            rs.raw_quantity
        FROM RawStorage rs
        LEFT JOIN ProductionInputs pi ON rs.materialid = pi.materialid
        WHERE pi.materialid IS NULL
    ),
    WorkforceRequirements AS (
        -- 4. Calculate 7-day workforce consumption buffer
        SELECT 
            wfn.materialid,
            -- 'unitsperinterval' is daily consumption, so multiply by 7
            SUM(wfn.unitsperinterval * 7) AS required_workforce_quantity
        FROM workforces wf
        JOIN workforce_needs wfn ON wf.workforceid = wfn.workforceid
        WHERE wf.userid IN (SELECT userdataid FROM TargetUser)
        GROUP BY wfn.materialid
    ),
    SurplusInventory AS (
        -- 5. Final Calculation: Determine surplus quantity after reserving for production and workforce needs
        SELECT 
            aap.materialid,
            (aap.raw_quantity - COALESCE(wr.required_workforce_quantity, 0)) AS surplus_quantity
        FROM AvailableAfterProduction aap
        LEFT JOIN WorkforceRequirements wr ON aap.materialid = wr.materialid
        -- Excludes items where workforce needs exceed your storage
        WHERE (aap.raw_quantity - COALESCE(wr.required_workforce_quantity, 0)) > 0
    ),
InventoryValuation AS (
    SELECT 
        CASE 
            WHEN cb.ticker LIKE '%.IC1' THEN 'ICA'
            WHEN cb.ticker LIKE '%.CI1' THEN 'CIS'
            WHEN cb.ticker LIKE '%.NC1' THEN 'NCC'
            WHEN cb.ticker LIKE '%.AI1' THEN 'AIC'
            WHEN cb.ticker LIKE '%.EC1' THEN 'ECD'
            ELSE NULL 
        END AS currency,
        SUM(si.surplus_quantity * COALESCE(cb.price, cb.priceaverage, 0)) AS inventory_value
    FROM SurplusInventory si
    JOIN cx_brokers cb ON si.materialid = cb.materialid
    WHERE cb.ticker LIKE '%.IC1' 
       OR cb.ticker LIKE '%.CI1' 
       OR cb.ticker LIKE '%.NC1' 
       OR cb.ticker LIKE '%.AI1' 
       OR cb.ticker LIKE '%.EC1'
    GROUP BY 1
),
VirtualLedger AS (
    -- CX SELLS
    SELECT 
        'CX_' || t.tradeid AS id,  -- <--- ADDED ID
        t.pricecurrency AS currency, 
        CASE 
            WHEN EXISTS (SELECT 1 FROM corporation_shareholders cs WHERE cs.companycode = t.partnercode) THEN 'CORP_CX' 
            ELSE 'CX' 
        END AS category, 
        (t.amount * t.priceamount) AS amount, 
        t.tradetime AS timestamp,
        t.partnername,
        t.partnercode
    FROM comex_trade_orders_trades t
    JOIN comex_trade_orders o ON t.orderid = o.orderid
    WHERE o.userid IN (SELECT userdataid FROM TargetUser)
      AND o.type IN ('SELL', 'SELLING')

    UNION ALL

    -- CX BUYS
    SELECT 
        'CX_' || t.tradeid AS id,  -- <--- ADDED ID
        t.pricecurrency AS currency, 
        CASE 
            WHEN EXISTS (SELECT 1 FROM corporation_shareholders cs WHERE cs.companycode = t.partnercode) THEN 'CORP_CX' 
            ELSE 'CX' 
        END AS category, 
        -(t.amount * t.priceamount) AS amount, 
        t.tradetime AS timestamp,
        t.partnername,
        t.partnercode
    FROM comex_trade_orders_trades t
    JOIN comex_trade_orders o ON t.orderid = o.orderid
    WHERE o.userid IN (SELECT userdataid FROM TargetUser)
      AND o.type IN ('BUY', 'BUYING')

    UNION ALL

    -- CONTRACTS
    SELECT 
        'CTR_' || c.id AS id,  -- <--- ADDED ID
        cc.currencymoney AS currency, 
        CASE 
            WHEN EXISTS (SELECT 1 FROM corporation_shareholders cs WHERE cs.companycode = c.partnercode) THEN 'CORP_CONTRACT' 
            ELSE 'CONTRACT' 
        END AS category, 
        CASE 
            WHEN cc.party = c.party THEN -cc.amountmoney 
            ELSE cc.amountmoney 
        END AS amount,
        c.date AS timestamp,
        c.partnername, 
        c.partnercode  
    FROM contract_conditions cc
    JOIN contracts c ON cc.contractid = c.id
    WHERE c.userid IN (SELECT userdataid FROM TargetUser)
      AND cc.type = 'PAYMENT' 
      AND cc.status IN ('FULFILLED', 'RESOLVED') 
      AND cc.amountmoney IS NOT NULL
	  AND c.party = cc.contractparty
),
RecentTransactions AS (
    SELECT 
        currency,
        jsonb_agg(
            jsonb_build_object(
                'Id', id,  -- <--- INJECTED INTO JSON
                'Type', CASE 
                    WHEN category IN ('CX', 'CORP_CX') AND amount > 0 THEN category || ' SELL'
                    WHEN category IN ('CX', 'CORP_CX') AND amount < 0 THEN category || ' BUY'
                    WHEN category IN ('CONTRACT', 'CORP_CONTRACT') AND amount > 0 THEN category || ' INCOME'
                    WHEN category IN ('CONTRACT', 'CORP_CONTRACT') AND amount < 0 THEN category || ' EXPENSE'
                    ELSE 'UNKNOWN'
                END,
                'PartnerName', COALESCE(partnername, 'Unknown'),
                'PartnerCode', COALESCE(partnercode, '???'),
                'Amount', amount,
                'Timestamp', to_char(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
            ) ORDER BY timestamp DESC
        ) AS transactions_json
    FROM VirtualLedger
    WHERE timestamp >= NOW() - INTERVAL '30 days'
    GROUP BY currency
),
MultiTimeframeCashFlow AS (
    SELECT 
        currency,
        category,
        SUM(CASE WHEN amount > 0 AND timestamp >= NOW() - INTERVAL '7 days' THEN amount ELSE 0 END) AS income_7d,
        SUM(CASE WHEN amount < 0 AND timestamp >= NOW() - INTERVAL '7 days' THEN ABS(amount) ELSE 0 END) AS expense_7d,
        SUM(CASE WHEN amount > 0 AND timestamp >= NOW() - INTERVAL '30 days' THEN amount ELSE 0 END) AS income_30d,
        SUM(CASE WHEN amount < 0 AND timestamp >= NOW() - INTERVAL '30 days' THEN ABS(amount) ELSE 0 END) AS expense_30d,
        SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS income_all,
        SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) AS expense_all
    FROM VirtualLedger
    GROUP BY currency, category
),
AggregatedFlows AS (
    SELECT 
        currency,
        jsonb_agg(
            jsonb_build_object(
                'Category', category,
                '7D', jsonb_build_object('Income', income_7d, 'Expense', expense_7d, 'Net', income_7d - expense_7d),
                '30D', jsonb_build_object('Income', income_30d, 'Expense', expense_30d, 'Net', income_30d - expense_30d),
                'AllTime', jsonb_build_object('Income', income_all, 'Expense', expense_all, 'Net', income_all - expense_all)
            )
        ) AS flows_json
    FROM MultiTimeframeCashFlow
    GROUP BY currency
),
DailyBalances AS (
    SELECT DISTINCT ON (balancecurrencycode, DATE(snapshot_at))
        balancecurrencycode AS currency,
        DATE(snapshot_at) AS date,
        balanceamount AS closing_balance
    FROM user_currency_accounts_history
    WHERE userid IN (SELECT userdataid FROM TargetUser)
      AND snapshot_at >= NOW() - INTERVAL '30 days'
    ORDER BY balancecurrencycode, DATE(snapshot_at), snapshot_at DESC
),
AggregatedHistory AS (
    SELECT
        currency,
        jsonb_agg(
            jsonb_build_object(
                'Date', to_char(date, 'MM/DD'),
                'Balance', closing_balance
            ) ORDER BY date ASC
        ) AS history_json
    FROM DailyBalances
    GROUP BY currency
)
SELECT 
    jsonb_build_object(
        'CompanyId', $1,
        'Timestamp', to_char(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS"Z"'),
        'Currencies', COALESCE(
            (
                SELECT jsonb_agg(
                    jsonb_build_object(
                        'Currency', cb.currency,
                        'Liquid', COALESCE(cb.liquid_amount, 0),
                        'LockedBuy', COALESCE(lc.locked_buy, 0),
                        'LockedSell', COALESCE(lc.locked_sell, 0),
                        'InventoryValue', COALESCE(iv.inventory_value, 0),
                        'PendingReceivable', COALESCE(pc.pending_receivable, 0),
                        'PendingPayable', COALESCE(pc.pending_payable, 0),
                        'TotalAssets', COALESCE(cb.liquid_amount, 0) + 
                                       COALESCE(lc.locked_buy, 0) + 
                                       COALESCE(lc.locked_sell, 0) + 
                                       COALESCE(iv.inventory_value, 0) + 
                                       COALESCE(pc.pending_receivable, 0),
                        'CashFlows', COALESCE(af.flows_json, '[]'::jsonb),
                        'History', COALESCE(ah.history_json, '[]'::jsonb),
                        'Transactions', COALESCE(rt.transactions_json, '[]'::jsonb)
                    )
                )
                FROM CurrentBalances cb
                LEFT JOIN LockedCapital lc ON cb.currency = lc.currency
                LEFT JOIN PendingContracts pc ON cb.currency = pc.currency
                LEFT JOIN InventoryValuation iv ON cb.currency = iv.currency
                LEFT JOIN AggregatedFlows af ON cb.currency = af.currency
                LEFT JOIN AggregatedHistory ah ON cb.currency = ah.currency
                LEFT JOIN RecentTransactions rt ON cb.currency = rt.currency
            ), 
            '[]'::jsonb
        )
    )::text AS financial_json;
"""

async def get_financial_overview_json(db, user_id: str) -> str:
    try:
        async with db.pool.acquire() as conn:
            json_str = await conn.fetchval(SQL_FINANCIAL_OVERVIEW, user_id)
            return json_str or "{}"
    except Exception as e:
        logger.error(f"Error fetching financial overview: {e}", exc_info=True)
        raise e

SQL_TRANSACTION_DETAIL = """
WITH TargetUser AS (
    SELECT DISTINCT userdataid 
    FROM users 
    WHERE accountid = $1
)
SELECT jsonb_build_object(
    'ReferenceId', ref_id,
    'Location', loc,
    'FeeAmount', fee,
    'FeeCurrency', fee_curr,
    'ContextData', ctx
)::text
FROM (
    -- Route 1: It was a CX Trade
    SELECT 
        t.tradeid AS ref_id,
        ce.name AS loc,
        t.priceamount AS fee,
        t.pricecurrency AS fee_curr,
        'CX Trade | Material: ' || m.ticker || ' | Qty: ' || t.amount AS ctx
    FROM comex_trade_orders_trades t
    JOIN comex_trade_orders o ON t.orderid = o.orderid
    JOIN commodity_exchanges ce ON o.exchangeid = ce.id
    INNER JOIN materials m ON o.materialid = m.materialid
    WHERE 'CX_' || t.tradeid = $2 
      AND o.userid IN (SELECT userdataid FROM TargetUser)
    
    UNION ALL
    
    -- Route 2: It was a Contract
    SELECT 
        c.id AS ref_id,
        'Contract' AS loc,
        0 AS fee,
        '' AS fee_curr,
        'Name: ' || COALESCE(c.name, 'Local Market') || ' | Status: ' || c.status AS ctx
    FROM contracts c
    WHERE 'CTR_' || c.id = $2 
      AND c.userid IN (SELECT userdataid FROM TargetUser)
) sub;
"""

async def get_transaction_details_json(db, user_id: str, tx_id: str) -> str:
    try:
        async with db.pool.acquire() as conn:
            json_str = await conn.fetchval(SQL_TRANSACTION_DETAIL, user_id, tx_id)
            return json_str or "{}"
    except Exception as e:
        logger.error(f"Error fetching transaction details: {e}", exc_info=True)
        raise e
