-- ============================================================================
-- PostgreSQL Table & Index Schema: public_users_data
-- ============================================================================

CREATE TABLE IF NOT EXISTS public_users_data (
    id TEXT NOT NULL,
    username TEXT,
    company_id TEXT,
    company_name TEXT,
    company_code TEXT,
    subscription_level TEXT,
    highest_tier TEXT,
    pioneer BOOLEAN DEFAULT false,
    moderator BOOLEAN DEFAULT false,
    team BOOLEAN DEFAULT false,
    translator BOOLEAN DEFAULT false,
    active_days_per_week INTEGER DEFAULT 0,
    created_timestamp BIGINT DEFAULT 0,
    gifts JSONB DEFAULT '{}'::jsonb,
    PRIMARY KEY (id)
);

-- Performance & Optimization Indexes
CREATE INDEX IF NOT EXISTS idx_public_companycode_trgm ON public.public_users_data USING gin (company_code gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_public_username_trgm ON public.public_users_data USING gin (username gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_public_users_data_company_id ON public.public_users_data USING btree (company_id);
CREATE INDEX IF NOT EXISTS idx_public_users_data_subscription_level ON public.public_users_data USING btree (subscription_level);
CREATE INDEX IF NOT EXISTS idx_public_users_data_username ON public.public_users_data USING btree (username);
