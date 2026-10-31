-- ============================================================================
-- PostgreSQL Table & Index Schema: bank_weekly_snapshots & bank_depositor_weekly_snapshots
-- ============================================================================

CREATE TABLE IF NOT EXISTS bank_depositor_weekly_snapshots (
    id SERIAL PRIMARY KEY,
    bank_id INTEGER NOT NULL REFERENCES player_banks(id) ON DELETE CASCADE,
    username VARCHAR(100) NOT NULL,
    week_date DATE NOT NULL,
    starting_balance NUMERIC(20, 2) NOT NULL DEFAULT 0,
    net_deposits NUMERIC(20, 2) NOT NULL DEFAULT 0,
    effective_capital NUMERIC(20, 2) NOT NULL DEFAULT 0,
    yield_earned NUMERIC(20, 2) NOT NULL DEFAULT 0,
    closing_balance NUMERIC(20, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT uq_bank_depositor_weekly UNIQUE (bank_id, username, week_date)
);

CREATE TABLE IF NOT EXISTS bank_weekly_snapshots (
    id SERIAL PRIMARY KEY,
    bank_id INTEGER NOT NULL REFERENCES player_banks(id) ON DELETE CASCADE,
    week_date DATE NOT NULL,
    total_pool NUMERIC(20, 2) NOT NULL DEFAULT 0,
    owner_equity NUMERIC(20, 2) NOT NULL DEFAULT 0,
    client_deposits NUMERIC(20, 2) NOT NULL DEFAULT 0,
    total_yield_distributed NUMERIC(20, 2) NOT NULL DEFAULT 0,
    depositors_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    CONSTRAINT uq_bank_weekly UNIQUE (bank_id, week_date)
);

CREATE INDEX IF NOT EXISTS idx_bank_dep_snapshots_user ON public.bank_depositor_weekly_snapshots (bank_id, username, week_date);
CREATE INDEX IF NOT EXISTS idx_bank_weekly_snapshots_date ON public.bank_weekly_snapshots (bank_id, week_date);
