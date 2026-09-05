-- ============================================================================
-- PostgreSQL Table & Index Schema: user_custom_prices
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_custom_prices (
    accountid TEXT NOT NULL,
    ticker VARCHAR(32) NOT NULL,
    price NUMERIC DEFAULT 0.00 NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (accountid, ticker)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_user_custom_prices_acc ON public.user_custom_prices USING btree (accountid);
