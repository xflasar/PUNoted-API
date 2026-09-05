-- ============================================================================
-- PostgreSQL Table & Index Schema: currencies
-- ============================================================================

CREATE TABLE IF NOT EXISTS currencies (
    code TEXT,
    decimals INTEGER,
    id TEXT NOT NULL,
    name TEXT,
    numericcode INTEGER,
    PRIMARY KEY (id)
);
