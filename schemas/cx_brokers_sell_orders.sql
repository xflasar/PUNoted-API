-- ============================================================================
-- PostgreSQL Table & Index Schema: cx_brokers_sell_orders
-- ============================================================================

CREATE TABLE IF NOT EXISTS cx_brokers_sell_orders (
    amount INTEGER,
    brokermaterialid TEXT,
    orderid TEXT NOT NULL,
    priceamount NUMERIC,
    pricecurrency TEXT,
    tradercode TEXT,
    traderid TEXT,
    tradername TEXT,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (orderid)
);
