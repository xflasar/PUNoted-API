-- ============================================================================
-- PostgreSQL Table & Index Schema: countries
-- ============================================================================

CREATE TABLE IF NOT EXISTS countries (
    code TEXT,
    currencycode TEXT,
    currencydecimals INTEGER,
    currencyname TEXT,
    currencynumericcode INTEGER,
    id TEXT NOT NULL,
    name TEXT,
    PRIMARY KEY (id)
);
