-- ============================================================================
-- PostgreSQL Table & Index Schema: corporation_shareholder_holdings
-- ============================================================================

CREATE TABLE IF NOT EXISTS corporation_shareholder_holdings (
    amount INTEGER,
    code TEXT,
    corporationid TEXT,
    currency TEXT,
    id INTEGER DEFAULT nextval('corporation_shareholder_holdings_id_seq'::regclass) NOT NULL,
    name TEXT,
    userid TEXT,
    PRIMARY KEY (id)
);
