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
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE UNIQUE INDEX IF NOT EXISTS player_banks_name_key ON public.player_banks USING btree (name);
CREATE INDEX IF NOT EXISTS idx_player_banks_owner ON public.player_banks USING btree (owner_username);
