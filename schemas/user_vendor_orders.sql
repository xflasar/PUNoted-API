-- ============================================================================
-- PostgreSQL Table & Index Schema: user_vendor_orders
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_vendor_orders (
    fixedprice DOUBLE PRECISION,
    materialid TEXT,
    materialticker TEXT NOT NULL,
    maxprice DOUBLE PRECISION,
    minprice DOUBLE PRECISION,
    orderid TEXT NOT NULL,
    ordertype TEXT,
    pricetype TEXT,
    quantity INTEGER,
    reserved INTEGER DEFAULT 0,
    vendorid TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    location JSONB,
    PRIMARY KEY (orderid)
);
