-- ============================================================================
-- PostgreSQL Table & Index Schema: commodity_exchanges
-- ============================================================================

CREATE TABLE IF NOT EXISTS commodity_exchanges (
    currencycode TEXT,
    currencydecimals INTEGER,
    currencyname TEXT,
    currencynumericcode INTEGER,
    id TEXT NOT NULL,
    name TEXT,
    operatorid TEXT,
    stationid TEXT,
    systemid TEXT,
    code TEXT,
    PRIMARY KEY (id)
);
