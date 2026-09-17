-- PostgreSQL Table & Index Schema: government_currency_accounts
-- Stores cash balances for planetary governments / admin centers

CREATE TABLE IF NOT EXISTS government_currency_accounts (
    admincenterid TEXT NOT NULL,
    category TEXT,
    type TEXT,
    number INTEGER,
    bookbalanceamount NUMERIC(20, 4),
    bookbalancecurrencycode TEXT,
    balanceamount NUMERIC(20, 4) NOT NULL DEFAULT 0.0,
    balancecurrencycode TEXT NOT NULL,
    userid TEXT,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (admincenterid, balancecurrencycode)
);

CREATE INDEX IF NOT EXISTS idx_gov_currency_admincenter ON public.government_currency_accounts USING btree (admincenterid);
