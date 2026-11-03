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
CurrentShareholderCodes AS (
    SELECT DISTINCT companycode
    FROM corporation_shareholders
),
VirtualLedger AS (
    -- CX SELLS
    SELECT 
        'CX_' || t.tradeid AS id,
        t.pricecurrency AS currency, 
        CASE 
            WHEN sc.companycode IS NOT NULL THEN 'CORP_CX' 
            ELSE 'CX' 
        END AS category, 
        (t.amount * t.priceamount) AS amount, 
        t.amount AS item_quantity,
        t.tradetime AS timestamp,
        t.partnername,
        t.partnercode
    FROM comex_trade_orders_trades t
    JOIN comex_trade_orders o ON t.orderid = o.orderid
    LEFT JOIN CurrentShareholderCodes sc ON sc.companycode = t.partnercode
    WHERE o.userid IN (SELECT userdataid FROM TargetUser)
      AND o.type IN ('SELL', 'SELLING')

    UNION ALL

    -- CX BUYS
    SELECT 
        'CX_' || t.tradeid AS id,
        t.pricecurrency AS currency, 
        CASE 
            WHEN sc.companycode IS NOT NULL THEN 'CORP_CX' 
            ELSE 'CX' 
        END AS category, 
        -(t.amount * t.priceamount) AS amount, 
        t.amount AS item_quantity,
        t.tradetime AS timestamp,
        t.partnername,
        t.partnercode
    FROM comex_trade_orders_trades t
    JOIN comex_trade_orders o ON t.orderid = o.orderid
    LEFT JOIN CurrentShareholderCodes sc ON sc.companycode = t.partnercode
    WHERE o.userid IN (SELECT userdataid FROM TargetUser)
      AND o.type IN ('BUY', 'BUYING')

    UNION ALL

    -- CONTRACTS
    SELECT 
        'CTR_' || c.id AS id,
        cc.currencymoney AS currency, 
        CASE 
            WHEN sc.companycode IS NOT NULL THEN 'CORP_CONTRACT' 
            ELSE 'CONTRACT' 
        END AS category, 
        CASE 
            WHEN cc.party = c.party THEN -cc.amountmoney 
            ELSE cc.amountmoney 
        END AS amount,
        0 AS item_quantity,
        c.date AS timestamp,
        c.partnername, 
        c.partnercode  
    FROM contract_conditions cc
    JOIN contracts c ON cc.contractid = c.id
    LEFT JOIN CurrentShareholderCodes sc ON sc.companycode = c.partnercode
    WHERE c.userid IN (SELECT userdataid FROM TargetUser)
      AND cc.type = 'PAYMENT' 
      AND cc.status IN ('FULFILLED', 'RESOLVED') 
      AND cc.amountmoney IS NOT NULL
	  AND c.party = cc.contractparty
),
LimitedVirtualLedger AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER(PARTITION BY currency ORDER BY timestamp DESC) as rn
        FROM VirtualLedger
    ) t
    WHERE rn <= 250
),
RecentTransactions AS (
    SELECT 
        currency,
        jsonb_agg(
            jsonb_build_object(
                'Id', id,
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
                'ItemQuantity', item_quantity,
                'Timestamp', to_char(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
            ) ORDER BY timestamp DESC
        ) AS transactions_json
    FROM LimitedVirtualLedger
    GROUP BY currency
),
MultiTimeframeCashFlow AS (
    SELECT 
        currency,
        category,
        SUM(CASE WHEN amount > 0 AND timestamp >= NOW() - INTERVAL '24 hours' THEN amount ELSE 0 END) AS income_24h,
        SUM(CASE WHEN amount < 0 AND timestamp >= NOW() - INTERVAL '24 hours' THEN ABS(amount) ELSE 0 END) AS expense_24h,
        SUM(CASE WHEN amount > 0 AND timestamp >= NOW() - INTERVAL '7 days' THEN amount ELSE 0 END) AS income_7d,
        SUM(CASE WHEN amount < 0 AND timestamp >= NOW() - INTERVAL '7 days' THEN ABS(amount) ELSE 0 END) AS expense_7d,
        SUM(CASE WHEN amount > 0 AND timestamp >= NOW() - INTERVAL '30 days' THEN amount ELSE 0 END) AS income_30d,
        SUM(CASE WHEN amount < 0 AND timestamp >= NOW() - INTERVAL '30 days' THEN ABS(amount) ELSE 0 END) AS expense_30d,
        SUM(CASE WHEN amount > 0 AND timestamp >= NOW() - INTERVAL '1 year' THEN amount ELSE 0 END) AS income_1y,
        SUM(CASE WHEN amount < 0 AND timestamp >= NOW() - INTERVAL '1 year' THEN ABS(amount) ELSE 0 END) AS expense_1y,
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
                '24H', jsonb_build_object('Income', income_24h, 'Expense', expense_24h, 'Net', income_24h - expense_24h),
                '7D', jsonb_build_object('Income', income_7d, 'Expense', expense_7d, 'Net', income_7d - expense_7d),
                '30D', jsonb_build_object('Income', income_30d, 'Expense', expense_30d, 'Net', income_30d - expense_30d),
                '1Y', jsonb_build_object('Income', income_1y, 'Expense', expense_1y, 'Net', income_1y - expense_1y),
                'AllTime', jsonb_build_object('Income', income_all, 'Expense', expense_all, 'Net', income_all - expense_all)
            )
        ) AS flows_json
    FROM MultiTimeframeCashFlow
    GROUP BY currency
),
TopPartnersAgg AS (
    SELECT 
        currency,
        jsonb_agg(
            jsonb_build_object(
                'code', partnercode,
                'name', partnername,
                'volume', volume,
                'income', income,
                'expense', expense,
                'net', net,
                'lastTimestamp', to_char(last_tx, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
            ) ORDER BY volume DESC
        ) AS top_partners_json
    FROM (
        SELECT 
            currency,
            partnercode,
            COALESCE(MAX(partnername), partnercode) as partnername,
            SUM(ABS(amount)) as volume,
            SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as income,
            SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as expense,
            SUM(amount) as net,
            MAX(timestamp) as last_tx,
            ROW_NUMBER() OVER(PARTITION BY currency ORDER BY SUM(ABS(amount)) DESC) as rn
        FROM VirtualLedger
        WHERE partnercode IS NOT NULL AND partnercode != ''
        GROUP BY currency, partnercode
    ) p
    WHERE rn <= 100
    GROUP BY currency
),
VolumeBreakdownAgg AS (
    SELECT 
        currency,
        jsonb_build_object(
            'cx', COALESCE(SUM(CASE WHEN category IN ('CX', 'CORP_CX') THEN ABS(amount) ELSE 0 END), 0),
            'contract', COALESCE(SUM(CASE WHEN category = 'CONTRACT' THEN ABS(amount) ELSE 0 END), 0),
            'corp', COALESCE(SUM(CASE WHEN category IN ('CORP_CX', 'CORP_CONTRACT') THEN ABS(amount) ELSE 0 END), 0)
        ) AS volume_breakdown_json
    FROM VirtualLedger
    GROUP BY currency
),
DailyBalances AS (
    SELECT DISTINCT ON (balancecurrencycode, DATE(snapshot_at))
        balancecurrencycode AS currency,
        DATE(snapshot_at) AS date,
        balanceamount AS closing_balance
    FROM user_currency_accounts_history
    WHERE userid IN (SELECT userdataid FROM TargetUser)
    
    UNION
    
    SELECT 
        balancecurrencycode AS currency,
        CURRENT_DATE AS date,
        balanceamount AS closing_balance
    FROM user_currency_accounts
    WHERE userid IN (SELECT userdataid FROM TargetUser)
    
    ORDER BY currency, date ASC
),
AggregatedHistory AS (
    SELECT
        currency,
        jsonb_agg(
            jsonb_build_object(
                'Date', to_char(date, 'YYYY-MM-DD'),
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
                        'TopPartners', COALESCE(tp.top_partners_json, '[]'::jsonb),
                        'VolumeBreakdown', COALESCE(vb.volume_breakdown_json, '{"cx":0,"contract":0,"corp":0}'::jsonb),
                        'History', COALESCE(ah.history_json, '[]'::jsonb),
                        'Transactions', COALESCE(rt.transactions_json, '[]'::jsonb)
                    )
                )
                FROM CurrentBalances cb
                LEFT JOIN LockedCapital lc ON cb.currency = lc.currency
                LEFT JOIN PendingContracts pc ON cb.currency = pc.currency
                LEFT JOIN InventoryValuation iv ON cb.currency = iv.currency
                LEFT JOIN AggregatedFlows af ON cb.currency = af.currency
                LEFT JOIN TopPartnersAgg tp ON cb.currency = tp.currency
                LEFT JOIN VolumeBreakdownAgg vb ON cb.currency = vb.currency
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

SQL_PAGINATED_TRANSACTIONS = """
WITH TargetUser AS (
    SELECT DISTINCT userdataid 
    FROM users 
    WHERE accountid = $1::uuid
),
CurrentShareholderCodes AS (
    SELECT DISTINCT companycode
    FROM corporation_shareholders
    WHERE userid IN (SELECT userdataid FROM TargetUser)
),
VirtualLedger AS (
    -- Route 1: Comex Trades
    SELECT 
        'CX_' || t.tradeid AS id,
        t.pricecurrency AS currency,
        CASE WHEN o.type = 'BUY' THEN -t.priceamount ELSE t.priceamount END AS amount,
        t.amount as item_quantity,
        t.timestamp,
        CASE WHEN o.type = 'BUY' THEN 'CX BUY' ELSE 'CX SELL' END AS category,
        ce.name AS partnername,
        ce.naturalid AS partnercode
    FROM comex_trade_orders_trades t
    JOIN comex_trade_orders o ON t.orderid = o.orderid
    JOIN commodity_exchanges ce ON o.exchangeid = ce.id
    WHERE o.userid IN (SELECT userdataid FROM TargetUser)

    UNION ALL

    -- Route 2: Fulfilled & Resolved Contracts
    SELECT 
        'CONTRACT_' || c.id || '_' || cc.id AS id,
        cc.currency AS currency,
        CASE 
            WHEN cc.contractparty = 'PAYER' THEN -cc.amountmoney
            ELSE cc.amountmoney 
        END AS amount,
        NULL::numeric as item_quantity,
        c.date_fulfilled AS timestamp,
        CASE 
            WHEN sc.companycode IS NOT NULL THEN 'CORP_CONTRACT'
            ELSE 'CONTRACT'
        END AS category,
        c.partnername,
        c.partnercode  
    FROM contract_conditions cc
    JOIN contracts c ON cc.contractid = c.id
    LEFT JOIN CurrentShareholderCodes sc ON sc.companycode = c.partnercode
    WHERE c.userid IN (SELECT userdataid FROM TargetUser)
      AND cc.type = 'PAYMENT' 
      AND cc.status IN ('FULFILLED', 'RESOLVED') 
      AND cc.amountmoney IS NOT NULL
      AND c.party = cc.contractparty
),
Filtered AS (
    SELECT *
    FROM VirtualLedger
    WHERE ($2::text IS NULL OR currency = $2::text)
      AND (
          $3::text IS NULL OR $3::text = 'ALL' OR
          ($3::text = '24H' AND timestamp >= NOW() - INTERVAL '24 hours') OR
          ($3::text = '7D' AND timestamp >= NOW() - INTERVAL '7 days') OR
          ($3::text = '30D' AND timestamp >= NOW() - INTERVAL '30 days') OR
          ($3::text = '1Y' AND timestamp >= NOW() - INTERVAL '1 year')
      )
      AND (
          $4::text IS NULL OR $4::text = '' OR
          partnercode ILIKE '%' || $4::text || '%' OR
          partnername ILIKE '%' || $4::text || '%' OR
          category ILIKE '%' || $4::text || '%'
      )
),
TotalCount AS (
    SELECT COUNT(*)::int AS count FROM Filtered
),
PageRows AS (
    SELECT 
        id,
        category AS type,
        partnername,
        partnercode,
        amount,
        item_quantity,
        timestamp
    FROM Filtered
    ORDER BY timestamp DESC
    LIMIT $5::int OFFSET $6::int
)
SELECT jsonb_build_object(
    'total_count', (SELECT count FROM TotalCount),
    'limit', $5::int,
    'offset', $6::int,
    'transactions', COALESCE(
        (
            SELECT jsonb_agg(
                jsonb_build_object(
                    'Id', id,
                    'Type', type,
                    'PartnerName', COALESCE(partnername, 'Unknown'),
                    'PartnerCode', COALESCE(partnercode, '???'),
                    'Amount', amount,
                    'ItemQuantity', item_quantity,
                    'Timestamp', to_char(timestamp, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
                )
            )
            FROM PageRows
        ),
        '[]'::jsonb
    )
)::text;
"""

async def get_paginated_transactions_json(
    db, 
    user_id: str, 
    currency: str | None = None, 
    timeframe: str | None = None, 
    search: str | None = None, 
    limit: int = 50, 
    offset: int = 0
) -> str:
    try:
        async with db.pool.acquire() as conn:
            json_str = await conn.fetchval(
                SQL_PAGINATED_TRANSACTIONS, 
                user_id, 
                currency, 
                timeframe, 
                search, 
                limit, 
                offset
            )
            return json_str or '{"total_count": 0, "transactions": []}'
    except Exception as e:
        logger.error(f"Error fetching paginated transactions: {e}", exc_info=True)
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
