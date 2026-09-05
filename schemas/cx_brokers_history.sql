-- ============================================================================
-- PostgreSQL Table & Index Schema: cx_brokers_history
-- ============================================================================

CREATE TABLE IF NOT EXISTS cx_brokers_history (
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
    xata_createdat TIMESTAMP WITH TIME ZONE NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE NOT NULL,
    xata_version INTEGER NOT NULL,
    snapshot_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_cx_brokers_history_date ON public.cx_brokers_history USING btree (snapshot_at);
