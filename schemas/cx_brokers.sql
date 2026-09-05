-- ============================================================================
-- PostgreSQL Table & Index Schema: cx_brokers
-- ============================================================================

CREATE TABLE IF NOT EXISTS cx_brokers (
    addressstationid TEXT,
    addresssystemid TEXT,
    alltimehigh NUMERIC,
    alltimelow NUMERIC,
    askamount INTEGER,
    askprice NUMERIC,
    bidamount INTEGER,
    bidprice NUMERIC,
    brokermaterialid TEXT NOT NULL,
    currencyid TEXT,
    demand INTEGER,
    exchangeid TEXT,
    high NUMERIC,
    low NUMERIC,
    materialid TEXT,
    narrowpricebandhigh NUMERIC,
    narrowpricebandlow NUMERIC,
    price NUMERIC,
    priceaverage NUMERIC,
    pricetime TIMESTAMP WITHOUT TIME ZONE,
    supply INTEGER,
    ticker TEXT,
    traded INTEGER,
    volume INTEGER,
    widepricebandhigh NUMERIC,
    widepricebandlow NUMERIC,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (brokermaterialid)
);
