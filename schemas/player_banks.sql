-- ============================================================================
-- PostgreSQL Table & Index Schema: player_banks
-- ============================================================================

CREATE TABLE IF NOT EXISTS player_banks (
    id INTEGER DEFAULT nextval('player_banks_id_seq'::regclass) NOT NULL,
    name VARCHAR(100) NOT NULL,
    owner_username VARCHAR(100) NOT NULL,
    liquidity NUMERIC DEFAULT 0,
    default_interest_rate NUMERIC DEFAULT 5.0,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    currency VARCHAR(20) DEFAULT 'ICA',
    default_repay_days INTEGER DEFAULT 14,
    default_interval VARCHAR(50) DEFAULT 'At Maturity',
    min_loan_amount NUMERIC DEFAULT 0,
    max_loan_amount NUMERIC DEFAULT 0,
    headline VARCHAR(255),
    last_yield_distribution TIMESTAMP WITH TIME ZONE DEFAULT now(),
    weekly_yield_rate DOUBLE PRECISION DEFAULT 1.5,
    public_show_loans BOOLEAN DEFAULT TRUE,
    public_show_deposits BOOLEAN DEFAULT TRUE,
    public_show_withdrawals BOOLEAN DEFAULT TRUE,
    public_show_names BOOLEAN DEFAULT FALSE,
    is_public BOOLEAN DEFAULT TRUE,
    max_interest_rate NUMERIC,
    max_weekly_yield_rate NUMERIC,
    deposit_status VARCHAR(50) DEFAULT 'OPEN',
    deposit_strategy VARCHAR(50) DEFAULT 'BALANCED',
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS player_banks_name_key ON public.player_banks USING btree (name);
CREATE INDEX IF NOT EXISTS idx_player_banks_owner ON public.player_banks USING btree (owner_username);
