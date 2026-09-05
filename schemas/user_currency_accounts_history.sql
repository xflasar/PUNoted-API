-- ============================================================================
-- PostgreSQL Table & Index Schema: user_currency_accounts_history
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_currency_accounts_history (
    balanceamount DOUBLE PRECISION,
    balancecurrencycode TEXT,
    bookbalanceamount DOUBLE PRECISION,
    bookbalancecurrencycode TEXT,
    category TEXT,
    number INTEGER,
    type INTEGER,
    userid TEXT NOT NULL,
    xata_createdat TIMESTAMP WITH TIME ZONE NOT NULL,
    xata_id TEXT,
    xata_updatedat TIMESTAMP WITH TIME ZONE NOT NULL,
    xata_version INTEGER NOT NULL,
    snapshot_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_currency_history_date ON public.user_currency_accounts_history USING btree (snapshot_at);
CREATE INDEX IF NOT EXISTS idx_currency_history_user ON public.user_currency_accounts_history USING btree (userid);
