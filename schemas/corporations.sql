-- ============================================================================
-- PostgreSQL Table & Index Schema: corporations
-- ============================================================================

CREATE TABLE IF NOT EXISTS corporations (
    code TEXT,
    countryid TEXT,
    currencycode TEXT,
    foundedtimestamp TIMESTAMP WITHOUT TIME ZONE,
    id TEXT NOT NULL,
    name TEXT,
    totalshares INTEGER,
    founder TEXT,
    officers TEXT[],
    PRIMARY KEY (id)
);
