-- ============================================================================
-- PostgreSQL Table & Index Schema: material_prices
-- ============================================================================

CREATE TABLE IF NOT EXISTS material_prices (
    price DOUBLE PRECISION,
    ticker TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL,
    PRIMARY KEY (ticker)
);
