-- ============================================================================
-- PostgreSQL Table & Index Schema: user_currency_accounts
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_currency_accounts (
    balanceamount DOUBLE PRECISION,
    balancecurrencycode TEXT,
    bookbalanceamount DOUBLE PRECISION,
    bookbalancecurrencycode TEXT,
    category TEXT,
    number INTEGER,
    type INTEGER,
    userid TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    xata_version INTEGER DEFAULT 0 NOT NULL
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS user_currency_accounts_unique_key ON public.user_currency_accounts USING btree (userid, category, type, number, balancecurrencycode);
